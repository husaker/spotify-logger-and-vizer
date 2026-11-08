import assert from "node:assert/strict";
import { generateKeyPairSync } from "node:crypto";
import test from "node:test";
import { encryptFernet, decryptFernet } from "../lib/server/fernet.ts";
import { encryptToken, decryptToken } from "../lib/server/crypto.ts";
import { checkSyncHealth } from "../lib/server/sync-health.ts";
import { checkSpotifyConnection, runSync } from "../lib/server/sync.ts";

const { privateKey } = generateKeyPairSync("rsa", { modulusLength: 2048 });
const key = Buffer.alloc(32, 7).toString("base64url");
const serviceAccount = JSON.stringify({ client_email: "test@example.test", private_key: privateKey.export({ type: "pkcs8", format: "pem" }) });
const item = second => ({ played_at: `2026-11-07T12:00:${String(second).padStart(2, "0")}.123Z`, track: { id: "one", name: "Same track", artists: [], album: {} } });

async function fixture(t, { loseLogResponse = false, rotate = false } = {}) {
  const state = [["refresh_token_enc_v2", await encryptToken("initial-refresh", key)]];
  const log = [];
  const calls = [];
  const original = globalThis.fetch;
  t.after(() => { globalThis.fetch = original; });
  const response = data => Response.json(data);
  globalThis.fetch = async (input, init = {}) => {
    const url = new URL(input);
    calls.push(url.href);
    if (url.hostname === "oauth2.googleapis.com") return response({ access_token: "google", expires_in: 3600 });
    if (url.hostname === "accounts.spotify.com") return response({ access_token: "spotify", ...(rotate ? { refresh_token: "rotated-refresh" } : {}) });
    if (url.hostname === "api.spotify.com") return response({ items: [item(2), item(1), item(1)], next: null });
    assert.equal(url.hostname, "sheets.googleapis.com", "collector must not call Sites");
    if (url.pathname.endsWith("/values:batchGet")) {
      return response({ valueRanges: url.searchParams.getAll("ranges").map(range => ({
        values: structuredClone(range.startsWith("log!") ? log : range === "__app_state!A1:B" ? [["key", "value"], ...state] : range.startsWith("__app_state!") ? state : []),
      })) });
    }
    if (url.pathname.endsWith("/values:batchUpdate")) {
      const { data } = JSON.parse(init.body);
      for (const { range, values } of data) {
        const rows = range.startsWith("log!") ? log : state;
        const first = Number(range.match(/A(\d+)/)[1]) - 2;
        values.forEach((row, i) => { rows[first + i] = row.map(String); });
      }
      if (loseLogResponse && data.some(({ range }) => range.startsWith("log!"))) {
        loseLogResponse = false;
        throw new Error("Sheet committed, response lost");
      }
      return response({});
    }
    if (url.pathname.endsWith(":append")) {
      if (decodeURIComponent(url.pathname).includes("__app_state!")) state.push(...JSON.parse(init.body).values.map(row => row.map(String)));
      return response({});
    }
    return response({ sheets: [{ properties: { sheetId: 0, title: "log", gridProperties: { rowCount: 1000 } } }] });
  };
  const env = {
    ASSETS: { fetch: async () => new Response(null) }, SYNC_EXECUTION_MODE: "github-actions",
    USER_SHEET_ID: "test-workbook", GOOGLE_SERVICE_ACCOUNT_JSON: serviceAccount,
    TOKEN_ENCRYPTION_KEY: key, SPOTIFY_CLIENT_ID: "client", SPOTIFY_CLIENT_SECRET: "secret",
  };
  return { env, log, state, calls };
}

test("standalone sync uses Spotify and Sheets without Sites/D1 and preserves repeat listens", async t => {
  const f = await fixture(t);
  assert.equal((await runSync(f.env)).added, 2);
  assert.equal((await runSync(f.env)).added, 0);
  assert.equal(f.log.length, 2);
  assert.deepEqual(f.log.map(row => row[0]), [item(1).played_at, item(2).played_at]);
  assert.ok(Object.fromEntries(f.state).last_sync_at);
});

test("full sync recovers a committed write after response loss without duplicate rows", async t => {
  const f = await fixture(t, { loseLogResponse: true });
  await assert.rejects(() => runSync(f.env), /response lost/);
  assert.equal(f.log.length, 2);
  assert.equal((await runSync(f.env)).added, 0);
  assert.equal(f.log.length, 2);
});

test("rotated token is saved before a later collection failure", async t => {
  const f = await fixture(t, { rotate: true, loseLogResponse: true });
  await assert.rejects(() => runSync(f.env));
  assert.equal(await decryptToken(Object.fromEntries(f.state).refresh_token_enc_v2, key), "rotated-refresh");
});

test("hosted/manual callers cannot use the single-writer collector accidentally", async () => {
  await assert.rejects(() => runSync({ ASSETS: { fetch: async () => new Response() } }), /serialized GitHub Actions/);
});

test("connection check uses the existing Fernet secret and does not collect or advance sync state", async t => {
  const f = await fixture(t, { rotate: true });
  delete f.env.TOKEN_ENCRYPTION_KEY;
  f.env.FERNET_KEY = key;
  f.state.splice(0, f.state.length, ["refresh_token_enc", await encryptFernet("legacy-refresh", key)]);
  await checkSpotifyConnection(f.env);
  assert.equal(f.log.length, 0);
  assert.equal(Object.fromEntries(f.state).last_sync_at, undefined);
  assert.equal(Object.fromEntries(f.state).last_synced_after_ts, undefined);
  assert.equal(await decryptFernet(Object.fromEntries(f.state).refresh_token_enc, key), "rotated-refresh");
  assert.equal((await runSync(f.env)).added, 2);
  assert.equal((await runSync(f.env)).added, 0);
});

test("health probe only reads Sheet state and never writes success or calls Spotify", async t => {
  const f = await fixture(t);
  f.state.push(["last_sync_at", new Date().toISOString()]);
  const before = structuredClone(f.state);
  await checkSyncHealth(f.env);
  assert.deepEqual(f.state, before);
  assert.equal(f.log.length, 0);
  assert.equal(f.calls.some(url => url.includes("spotify.com")), false);
  f.state[f.state.length - 1][1] = "2020-01-01T00:00:00Z";
  await assert.rejects(() => checkSyncHealth(f.env), /30 minutes/);
  assert.equal(f.state[f.state.length - 1][1], "2020-01-01T00:00:00Z");
});
