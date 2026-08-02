import assert from "node:assert/strict";
import test from "node:test";
import { createAdminSession, isAdmin, isSameOrigin, passwordMatches, sessionCookie } from "../lib/server/auth.ts";
import { decryptToken, encryptToken, signValue, verifySignedValue } from "../lib/server/crypto.ts";
import { parseLegacyPlayedAt } from "../lib/server/google.ts";
import { createSpotifyOAuthState, verifySpotifyOAuthState } from "../lib/server/oauth-state.ts";

const key = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

test("AES-GCM tokens round-trip and reject an unrelated key", async () => {
  const encrypted = await encryptToken("spotify-refresh-token", key);
  assert.match(encrypted, /^v2\./);
  assert.equal(await decryptToken(encrypted, key), "spotify-refresh-token");
  const otherKey = "AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE";
  await assert.rejects(() => decryptToken(encrypted, otherKey));
});

test("signed values and admin sessions fail closed", async () => {
  const secret = "a-long-preview-session-signing-key";
  const signature = await signValue("payload", secret);
  assert.equal(await verifySignedValue("payload", signature, secret), true);
  assert.equal(await verifySignedValue("changed", signature, secret), false);
  const value = await createAdminSession(secret);
  const cookie = sessionCookie(value).split(";")[0];
  const env = { SESSION_SIGNING_KEY: secret, ADMIN_PASSWORD: "correct", ASSETS: { fetch: async () => new Response() } };
  assert.equal(await isAdmin(new Request("https://example.test/admin", { headers: { cookie } }), env), true);
  assert.equal(await isAdmin(new Request("https://example.test/admin"), env), false);
  assert.equal(passwordMatches("correct", env), true);
  assert.equal(passwordMatches("wrong", env), false);
});

test("legacy Moscow timestamps normalize without relying on Date.parse", () => {
  assert.equal(parseLegacyPlayedAt("November 12, 2025 at 10:42AM"), "2025-11-12T07:42:00.000Z");
  assert.equal(parseLegacyPlayedAt("not a date"), null);
});

test("Spotify OAuth state is signed and expires after ten minutes", async () => {
  const secret = "oauth-state-signing-secret";
  const now = Date.UTC(2026, 7, 2, 12, 0, 0);
  const state = await createSpotifyOAuthState(secret, now);
  assert.equal(await verifySpotifyOAuthState(state, secret, now + 599_000), true);
  assert.equal(await verifySpotifyOAuthState(state, secret, now + 601_000), false);
  assert.equal(await verifySpotifyOAuthState(`${state}x`, secret, now), false);
});

test("origin validation accepts opaque same-origin navigation and rejects cross-site requests", () => {
  assert.equal(isSameOrigin(new Request("https://spotify.example/api", { headers: { origin: "https://spotify.example" } })), true);
  assert.equal(isSameOrigin(new Request("https://spotify.example/api", { headers: { origin: "https://attacker.example" } })), false);
  assert.equal(isSameOrigin(new Request("https://spotify.example/api", { headers: { origin: "null", "sec-fetch-site": "same-origin" } })), true);
  assert.equal(isSameOrigin(new Request("https://spotify.example/api", { headers: { origin: "null", "sec-fetch-site": "cross-site" } })), false);
  assert.equal(isSameOrigin(new Request("https://spotify.example/api")), false);
});
