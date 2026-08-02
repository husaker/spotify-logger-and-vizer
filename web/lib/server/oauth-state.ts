import { signValue, verifySignedValue } from "./crypto";
import { randomToken } from "./encoding";

const OAUTH_STATE_SECONDS = 10 * 60;

export async function createSpotifyOAuthState(secret: string, nowMs = Date.now()): Promise<string> {
  const expires = Math.floor(nowMs / 1000) + OAUTH_STATE_SECONDS;
  const body = `${expires}.${randomToken(16)}`;
  return `${body}.${await signValue(`spotify:${body}`, secret)}`;
}

export async function verifySpotifyOAuthState(value: string, secret: string, nowMs = Date.now()): Promise<boolean> {
  const [expiresRaw, nonce, signature, ...extra] = value.split(".");
  if (!expiresRaw || !nonce || !signature || extra.length) return false;
  const expires = Number(expiresRaw);
  if (!Number.isFinite(expires) || expires < nowMs / 1000) return false;
  return verifySignedValue(`spotify:${expiresRaw}.${nonce}`, signature, secret);
}
