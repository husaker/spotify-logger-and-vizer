import type { WorkerEnv } from "../types";
import { signValue, verifySignedValue } from "./crypto";
import { constantTimeEqual, randomToken } from "./encoding";

const COOKIE_NAME = "spotify_logger_admin";
const SESSION_SECONDS = 12 * 60 * 60;

function parseCookies(request: Request): Record<string, string> {
  return Object.fromEntries(
    (request.headers.get("cookie") ?? "")
      .split(";")
      .map((part) => part.trim().split("="))
      .filter(([key, value]) => Boolean(key && value))
      .map(([key, value]) => [key, decodeURIComponent(value)]),
  );
}

export async function createAdminSession(secret: string): Promise<string> {
  const expires = Math.floor(Date.now() / 1000) + SESSION_SECONDS;
  const payload = `${expires}.${randomToken(18)}`;
  return `${payload}.${await signValue(payload, secret)}`;
}

export async function isAdmin(request: Request, env: WorkerEnv): Promise<boolean> {
  if (!env.SESSION_SIGNING_KEY) return false;
  const value = parseCookies(request)[COOKIE_NAME];
  if (!value) return false;
  const parts = value.split(".");
  if (parts.length !== 3) return false;
  const payload = `${parts[0]}.${parts[1]}`;
  const expires = Number(parts[0]);
  if (!Number.isFinite(expires) || expires < Date.now() / 1000) return false;
  return verifySignedValue(payload, parts[2], env.SESSION_SIGNING_KEY);
}

export function sessionCookie(value: string): string {
  return `${COOKIE_NAME}=${encodeURIComponent(value)}; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=${SESSION_SECONDS}`;
}

export function clearSessionCookie(): string {
  return `${COOKIE_NAME}=; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=0`;
}

export function passwordMatches(candidate: string, env: WorkerEnv): boolean {
  return Boolean(env.ADMIN_PASSWORD) && constantTimeEqual(candidate, env.ADMIN_PASSWORD ?? "");
}

export function isSameOrigin(request: Request): boolean {
  const origin = request.headers.get("origin");
  const expected = new URL(request.url).origin;
  if (origin && origin !== "null") return origin === expected;
  const referer = request.headers.get("referer");
  if (referer) {
    try { return new URL(referer).origin === expected; }
    catch { return false; }
  }
  return origin === "null" && request.headers.get("sec-fetch-site") === "same-origin";
}
