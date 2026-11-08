import assert from "node:assert/strict";
import test from "node:test";
import { decryptFernet, encryptFernet } from "../lib/server/fernet.ts";
import { storedRefreshToken } from "../lib/server/sync-token.ts";
import { encryptToken, decryptToken } from "../lib/server/crypto.ts";

// Published interoperability vector: https://github.com/fernet/spec/blob/master/verify.json
const secret = "cw_0x689RpI-jtRR7oE8h_eQsKImvJapLeSbXpwF4e4=";
const vector = "gAAAAAAdwJ6wAAECAwQFBgcICQoLDA0ODy021cpGVWKZ_eEwCGM4BLLF_5CV9dOPmrhuVUPgJobwOz7JcbmrR64jVmpU4IwqDA==";

test("decrypts the official Fernet compatibility vector without imposing a refresh-token TTL", async () => {
  assert.equal(await decryptFernet(vector, secret), "hello");
});

test("Fernet authenticates before decryption and rejects corruption or unrelated keys", async () => {
  const token = Buffer.from(vector, "base64url");
  token[30] ^= 1;
  await assert.rejects(() => decryptFernet(token.toString("base64url"), secret), /signature/);
  await assert.rejects(() => decryptFernet(vector, Buffer.alloc(32, 4).toString("base64url")), /signature/);
  await assert.rejects(() => decryptFernet("bad", secret));
});

test("Fernet encryption preserves padding/Unicode/empty input and uses fresh IVs", async () => {
  for (const value of ["", "a".repeat(16), "refresh-🔑"]) assert.equal(await decryptFernet(await encryptFernet(value, secret), secret), value);
  assert.notEqual(await encryptFernet("same", secret), await encryptFernet("same", secret));
});

test("legacy-only configuration decrypts and saves rotated tokens with FERNET_KEY", async () => {
  const stored = await storedRefreshToken({ FERNET_KEY: secret }, { refresh_token_enc: vector, refresh_token_enc_v2: "v2.unavailable" });
  assert.equal(stored.value, "hello");
  const update = await stored.save("rotated");
  assert.deepEqual(Object.keys(update), ["refresh_token_enc"]);
  assert.equal(await decryptFernet(update.refresh_token_enc, secret), "rotated");
});

test("configured modern authorization is preferred and failures do not silently downgrade", async () => {
  const modern = Buffer.alloc(32, 7).toString("base64url");
  const state = { refresh_token_enc: vector, refresh_token_enc_v2: await encryptToken("new", modern) };
  const stored = await storedRefreshToken({ TOKEN_ENCRYPTION_KEY: modern, FERNET_KEY: secret }, state);
  assert.equal(stored.value, "new");
  assert.equal(await decryptToken((await stored.save("rotated")).refresh_token_enc_v2, modern), "rotated");
  await assert.rejects(() => storedRefreshToken({ TOKEN_ENCRYPTION_KEY: Buffer.alloc(32, 8).toString("base64url"), FERNET_KEY: secret }, state));
});
