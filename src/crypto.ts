import { base64Url, bytes, fromBase64Url, text } from "./encoding";

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
  if (version !== "v2" || !iv || !encrypted) throw new Error("Unsupported token encryption format; reconnect Spotify using npm run authorize");
  const plain = await crypto.subtle.decrypt(
    { name: "AES-GCM", iv: fromBase64Url(iv) },
    await importAesKey(secret),
    fromBase64Url(encrypted),
  );
  return text(plain);
}

