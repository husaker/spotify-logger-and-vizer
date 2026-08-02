import { base64Url, bytes, constantTimeEqual, fromBase64Url, text } from "./encoding";

async function importHmac(secret: string): Promise<CryptoKey> {
  return crypto.subtle.importKey("raw", bytes(secret), { name: "HMAC", hash: "SHA-256" }, false, ["sign", "verify"]);
}

export async function signValue(value: string, secret: string): Promise<string> {
  return base64Url(await crypto.subtle.sign("HMAC", await importHmac(secret), bytes(value)));
}

export async function verifySignedValue(value: string, signature: string, secret: string): Promise<boolean> {
  const expected = await signValue(value, secret);
  return constantTimeEqual(expected, signature);
}

async function importAesKey(secret: string): Promise<CryptoKey> {
  const raw = fromBase64Url(secret);
  if (raw.byteLength !== 32) throw new Error("TOKEN_ENCRYPTION_KEY must be a base64url-encoded 32-byte key");
  return crypto.subtle.importKey("raw", raw, "AES-GCM", false, ["encrypt", "decrypt"]);
}

export async function encryptToken(token: string, secret: string): Promise<string> {
  const iv = crypto.getRandomValues(new Uint8Array(12));
  const encrypted = await crypto.subtle.encrypt({ name: "AES-GCM", iv }, await importAesKey(secret), bytes(token));
  return `v2.${base64Url(iv)}.${base64Url(encrypted)}`;
}

export async function decryptToken(payload: string, secret: string): Promise<string> {
  const [version, iv, encrypted] = payload.split(".");
  if (version !== "v2" || !iv || !encrypted) throw new Error("Unsupported token encryption format; reconnect Spotify in /admin");
  const plain = await crypto.subtle.decrypt(
    { name: "AES-GCM", iv: fromBase64Url(iv) },
    await importAesKey(secret),
    fromBase64Url(encrypted),
  );
  return text(plain);
}

