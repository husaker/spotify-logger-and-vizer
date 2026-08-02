import handler from "vinext/server/app-router-entry";
import type { AdminStatus, WorkerEnv } from "../lib/types";
import { clearSessionCookie, createAdminSession, isAdmin, isSameOrigin, passwordMatches, sessionCookie } from "../lib/server/auth";
import { encryptToken } from "../lib/server/crypto";
import { googleIsConfigured, loadDashboard, readAppState, writeAppState } from "../lib/server/google";
import { notifyReauthorized } from "../lib/server/notifications";
import { exchangeSpotifyCode, spotifyAuthorizationUrl, spotifyUserId } from "../lib/server/spotify";
import { runSync } from "../lib/server/sync";
import { createTelegramPairing, handleTelegramWebhook, sendTelegram } from "../lib/server/telegram";
import { createSpotifyOAuthState, verifySpotifyOAuthState } from "../lib/server/oauth-state";
import { constantTimeEqual } from "../lib/server/encoding";
import { demoDashboard } from "../lib/demo";

interface ExecutionContext {
  waitUntil(promise: Promise<unknown>): void;
  passThroughOnException(): void;
}

function json(data: unknown, status = 200, headers?: HeadersInit): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "content-type": "application/json; charset=utf-8", "cache-control": "no-store", ...(headers ?? {}) },
  });
}

function secureResponse(response: Response, pathname: string): Response {
  const headers = new Headers(response.headers);
  headers.set("x-content-type-options", "nosniff");
  headers.set("x-frame-options", "DENY");
  headers.set("referrer-policy", "strict-origin-when-cross-origin");
  headers.set("permissions-policy", "camera=(), microphone=(), geolocation=(), payment=(), usb=()");
  headers.set("content-security-policy", "frame-ancestors 'none'; object-src 'none'; base-uri 'self'");
  headers.set("strict-transport-security", "max-age=31536000");
  if (pathname === "/admin" || pathname.startsWith("/api/")) {
    headers.set("x-robots-tag", "noindex, nofollow, noarchive");
  }
  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers,
  });
}

function safeError(error: unknown): string {
  if (!(error instanceof Error)) return "Request failed";
  if (/configured|incomplete|required|expired|unavailable|invalid/i.test(error.message)) return error.message;
  return "The operation could not be completed. Check the protected status details and try again.";
}

function defaultCache(): Cache | null {
  try {
    return typeof caches === "undefined" ? null : (caches as CacheStorage & { default?: Cache }).default ?? null;
  } catch {
    return null;
  }
}

function escapeHtml(value: string): string {
  return value.replaceAll("&", "&amp;").replaceAll('"', "&quot;").replaceAll("<", "&lt;").replaceAll(">", "&gt;");
}

function spotifyConfirmation(code: string, state: string): Response {
  const body = `<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Complete Spotify connection</title><style>body{margin:0;background:#090b0a;color:#f4f7f5;font:16px system-ui;display:grid;min-height:100vh;place-items:center}.card{max-width:34rem;margin:1.5rem;padding:2rem;border:1px solid #26302a;border-radius:1.25rem;background:#111512}h1{font-size:2rem;margin:.2rem 0 1rem}p{color:#aeb8b2;line-height:1.6}button{border:0;border-radius:999px;background:#1ed760;color:#071109;font-weight:800;padding:.9rem 1.3rem;cursor:pointer}</style></head><body><main class="card"><small>Protected operation</small><h1>Complete Spotify connection</h1><p>Spotify returned successfully. Confirm once to store the new authorization securely.</p><form method="post" action="/api/admin/spotify/callback"><input type="hidden" name="code" value="${escapeHtml(code)}"><input type="hidden" name="state" value="${escapeHtml(state)}"><button type="submit">Complete connection</button></form></main></body></html>`;
  return new Response(body, {
    headers: {
      "content-type": "text/html; charset=utf-8",
      "cache-control": "no-store",
      "content-security-policy": "default-src 'none'; style-src 'unsafe-inline'; form-action 'self'; base-uri 'none'; frame-ancestors 'none'",
      "referrer-policy": "no-referrer",
    },
  });
}

function coreConfigured(env: WorkerEnv): boolean {
  return googleIsConfigured(env) && Boolean(
    env.SPOTIFY_CLIENT_ID && env.SPOTIFY_CLIENT_SECRET && env.TOKEN_ENCRYPTION_KEY
    && env.ADMIN_PASSWORD && env.SESSION_SIGNING_KEY && env.PUBLIC_APP_URL,
  );
}

async function requireAdmin(request: Request, env: WorkerEnv): Promise<Response | null> {
  return await isAdmin(request, env) ? null : json({ error: "Unauthorized" }, 401);
}

function cronAuthorized(request: Request, env: WorkerEnv): boolean {
  const prefix = "Bearer ";
  const authorization = request.headers.get("authorization") ?? "";
  const candidate = authorization.startsWith(prefix) ? authorization.slice(prefix.length) : "";
  return Boolean(env.CRON_SECRET) && constantTimeEqual(candidate, env.CRON_SECRET ?? "");
}

function invalidateDashboardCache(origin: string, ctx: ExecutionContext): void {
  const cache = defaultCache();
  if (cache) ctx.waitUntil(cache.delete(new Request(`${origin.replace(/\/$/, "")}/__dashboard-cache-v2`)).catch(() => false));
}

async function publicDashboard(request: Request, env: WorkerEnv): Promise<Response> {
  if (env.DEMO_MODE === "true") return json(demoDashboard(), 200, { "cache-control": "no-store" });
  if (!googleIsConfigured(env)) return json({ error: "Google Sheet connection is not configured yet" }, 503);
  const cache = defaultCache();
  const key = new Request(`${new URL(request.url).origin}/__dashboard-cache-v2`);
  try {
    const payload = await loadDashboard(env);
    const response = json(payload, 200, { "cache-control": "public, max-age=60, s-maxage=300, stale-while-revalidate=86400" });
    if (cache) await cache.put(key, response.clone()).catch(() => undefined);
    return response;
  } catch (error) {
    const cached = cache ? await cache.match(key).catch(() => undefined) : undefined;
    if (cached) {
      const payload = await cached.json() as Record<string, unknown>;
      return json({ ...payload, stale: true }, 200, { "cache-control": "public, max-age=30" });
    }
    return json({ error: safeError(error) }, 503);
  }
}

async function api(request: Request, env: WorkerEnv, ctx: ExecutionContext): Promise<Response | null> {
  const url = new URL(request.url);
  const { pathname } = url;
  if (pathname === "/api/dashboard" && request.method === "GET") return publicDashboard(request, env);

  if (pathname === "/api/cron/sync" && request.method === "POST") {
    if (!cronAuthorized(request, env)) return json({ error: "Unauthorized" }, 401);
    try {
      const result = await runSync(env);
      invalidateDashboardCache(env.PUBLIC_APP_URL ?? url.origin, ctx);
      return json({ ok: true, added: result.added, lastSyncAt: result.lastSyncAt });
    } catch {
      return json({ error: "Synchronization failed" }, 503);
    }
  }

  if (pathname === "/api/admin/login" && request.method === "POST") {
    if (!isSameOrigin(request)) return json({ error: "Invalid request origin" }, 403);
    const body = await request.json().catch(() => ({})) as { password?: string };
    if (!passwordMatches(body.password ?? "", env)) return json({ error: "Incorrect password" }, 401);
    const session = await createAdminSession(env.SESSION_SIGNING_KEY!);
    return json({ ok: true }, 200, { "set-cookie": sessionCookie(session) });
  }
  if (pathname === "/api/telegram/webhook" && request.method === "POST") {
    try { return await handleTelegramWebhook(request, env, await readAppState(env)); }
    catch { return new Response("ok"); }
  }

  if (!pathname.startsWith("/api/admin/")) return null;
  const unauthorized = await requireAdmin(request, env);
  if (unauthorized) return unauthorized;
  if (request.method === "POST" && !isSameOrigin(request)) return json({ error: "Invalid request origin" }, 403);

  try {
    if (pathname === "/api/admin/logout" && request.method === "POST") {
      return json({ message: "Logged out" }, 200, { "set-cookie": clearSessionCookie() });
    }
    if (pathname === "/api/admin/status" && request.method === "GET") {
      const state = googleIsConfigured(env) ? await readAppState(env) : {};
      const status: AdminStatus = {
        configured: coreConfigured(env),
        spotifyConnected: Boolean(state.refresh_token_enc_v2),
        reauthorizationRequired: state.reauth_required === "true",
        authorizedAt: state.spotify_authorized_at || null,
        reauthorizationDueAt: state.spotify_reauth_due_at || null,
        lastSyncAt: state.last_sync_at || null,
        lastError: state.last_error || null,
        consecutiveFailures: Number(state.consecutive_failures) || 0,
        telegramConnected: Boolean(state.telegram_chat_id),
        telegramConnectedAt: state.telegram_connected_at || null,
      };
      return json(status);
    }

    if (pathname === "/api/admin/sync" && request.method === "POST") {
      const result = await runSync(env);
      invalidateDashboardCache(url.origin, ctx);
      return json({ message: `Sync complete · ${result.added} new play${result.added === 1 ? "" : "s"}` });
    }

    if (pathname === "/api/admin/spotify/connect" && request.method === "GET") {
      if (!env.SESSION_SIGNING_KEY) throw new Error("SESSION_SIGNING_KEY is not configured");
      const state = await createSpotifyOAuthState(env.SESSION_SIGNING_KEY);
      return Response.redirect(spotifyAuthorizationUrl(env, state), 302);
    }

    if (pathname === "/api/admin/spotify/callback" && (request.method === "GET" || request.method === "POST")) {
      if (!env.SESSION_SIGNING_KEY || !env.TOKEN_ENCRYPTION_KEY) throw new Error("Spotify security secrets are incomplete");
      const form = request.method === "POST" ? await request.formData() : null;
      const code = request.method === "POST" ? String(form?.get("code") ?? "") : url.searchParams.get("code") ?? "";
      const stateValue = request.method === "POST" ? String(form?.get("state") ?? "") : url.searchParams.get("state") ?? "";
      const valid = Boolean(code) && await verifySpotifyOAuthState(stateValue, env.SESSION_SIGNING_KEY);
      if (!valid) return Response.redirect(`${url.origin}/admin?error=spotify-state`, 302);
      if (request.method === "GET") return spotifyConfirmation(code, stateValue);
      const tokens = await exchangeSpotifyCode(env, code);
      const now = new Date();
      const due = new Date(now);
      due.setUTCMonth(due.getUTCMonth() + 6);
      const oldState = await readAppState(env);
      await writeAppState(env, {
        refresh_token_enc_v2: await encryptToken(tokens.refreshToken, env.TOKEN_ENCRYPTION_KEY),
        spotify_user_id: await spotifyUserId(tokens.accessToken),
        spotify_authorized_at: now.toISOString(),
        spotify_reauth_due_at: due.toISOString(),
        reauth_required: "false",
        consecutive_failures: "0",
        last_error: "",
      });
      ctx.waitUntil(notifyReauthorized(env, oldState).catch(() => undefined));
      return Response.redirect(`${url.origin}/admin?spotify=connected`, 302);
    }

    if (pathname === "/api/admin/telegram/pair" && request.method === "POST") {
      const pairing = await createTelegramPairing(env);
      return json({ url: pairing.url, message: `Pairing link opened for @${pairing.username}. Send Start in Telegram.` });
    }
    if (pathname === "/api/admin/telegram/test" && request.method === "POST") {
      const state = await readAppState(env);
      if (!state.telegram_chat_id) throw new Error("Telegram is not connected");
      await sendTelegram(env, state.telegram_chat_id, "Spotify Logger test notification received. Your alerts are configured correctly.");
      return json({ message: "Test notification sent" });
    }
    if (pathname === "/api/admin/telegram/disconnect" && request.method === "POST") {
      await writeAppState(env, { telegram_chat_id: "", telegram_connected_at: "", telegram_pair_used: "" });
      return json({ message: "Telegram disconnected" });
    }
  } catch (error) {
    return json({ error: safeError(error) }, 400);
  }
  return json({ error: "Not found" }, 404);
}

const worker = {
  async fetch(request: Request, env: WorkerEnv, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);
    if (url.pathname.startsWith("/api/")) {
      const response = await api(request, env, ctx);
      if (response) return secureResponse(response, url.pathname);
    }
    return secureResponse(await handler.fetch(request, env, ctx), url.pathname);
  },
  async scheduled(_controller: unknown, env: WorkerEnv, ctx: ExecutionContext): Promise<void> {
    if (env.SCHEDULED_SYNC_ENABLED !== "true") return;
    ctx.waitUntil(runSync(env).then(async () => {
      const cache = defaultCache();
      if (cache && env.PUBLIC_APP_URL) await cache.delete(new Request(`${env.PUBLIC_APP_URL.replace(/\/$/, "")}/__dashboard-cache-v2`));
    }).catch(() => undefined));
  },
};

export default worker;
