import { base64Url, bytes, fromBase64Url, text } from "./encoding";

// Compatibility with the original Python logger's cryptography.fernet tokens.
// https://github.com/fernet/spec/blob/master/Spec.md
// Long-lived stored refresh tokens have no Fernet TTL; Spotify controls expiry.
async function keys(secret: string) {
  const raw = fromBase64Url(secret);
  if (raw.length !== 32) throw new Error("FERNET_KEY must encode 32 bytes");
  return {
    signing: await crypto.subtle.importKey("raw", raw.slice(0, 16), { name: "HMAC", hash: "SHA-256" }, false, ["sign", "verify"]),
    encryption: await crypto.subtle.importKey("raw", raw.slice(16), "AES-CBC", false, ["encrypt", "decrypt"]),
  };
}

export async function decryptFernet(payload: string, secret: string): Promise<string> {
  const token = fromBase64Url(payload);
  if (token.length < 73 || token[0] !== 0x80 || (token.length - 57) % 16 !== 0) throw new Error("Invalid Fernet token");
  const key = await keys(secret);
  const signed = token.slice(0, -32);
  if (!await crypto.subtle.verify("HMAC", key.signing, token.slice(-32), signed)) throw new Error("Invalid Fernet signature");
  return text(await crypto.subtle.decrypt({ name: "AES-CBC", iv: token.slice(9, 25) }, key.encryption, token.slice(25, -32)));
}

export async function encryptFernet(value: string, secret: string): Promise<string> {
  const key = await keys(secret);
  const iv = crypto.getRandomValues(new Uint8Array(16));
  const ciphertext = new Uint8Array(await crypto.subtle.encrypt({ name: "AES-CBC", iv }, key.encryption, bytes(value)));
  const signed = new Uint8Array(25 + ciphertext.length);
  signed[0] = 0x80;
  new DataView(signed.buffer).setBigUint64(1, BigInt(Math.floor(Date.now() / 1000)));
  signed.set(iv, 9);
  signed.set(ciphertext, 25);
  const token = new Uint8Array(signed.length + 32);
  token.set(signed);
  token.set(new Uint8Array(await crypto.subtle.sign("HMAC", key.signing, signed)), signed.length);
  const encoded = base64Url(token);
  return encoded.padEnd(Math.ceil(encoded.length / 4) * 4, "=");
}
