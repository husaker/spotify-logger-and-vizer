import assert from "node:assert/strict";
import test from "node:test";
import { encryptToken, decryptToken } from "../src/crypto.ts";

test("v2 authorization encryption rejects unrelated keys and corrupted ciphertext", async () => {
  const key = Buffer.alloc(32, 1).toString("base64url");
  const encrypted = await encryptToken("refresh-token", key);
  assert.equal(await decryptToken(encrypted, key), "refresh-token");
  await assert.rejects(() => decryptToken(encrypted, Buffer.alloc(32, 2).toString("base64url")));
  const parts = encrypted.split(".");
  const data = Buffer.from(parts[2], "base64url"); data[0] ^= 1; parts[2] = data.toString("base64url");
  await assert.rejects(() => decryptToken(parts.join("."), key));
});
