import type { AppState, WorkerEnv } from "../types";
import { signValue } from "./crypto";
import { constantTimeEqual, randomToken } from "./encoding";
import { writeAppState } from "./google";

type TelegramResult<T> = { ok: boolean; result?: T; description?: string };

async function telegramCall<T>(env: WorkerEnv, method: string, payload?: Record<string, unknown>): Promise<T> {
  if (!env.TELEGRAM_BOT_TOKEN) throw new Error("Telegram bot token is not configured");
  const response = await fetch(`https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/${method}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload ?? {}),
  });
  const result = await response.json() as TelegramResult<T>;
  if (!response.ok || !result.ok || result.result === undefined) {
    if (response.status === 401 || /unauthorized|token/i.test(result.description ?? "")) {
      throw new Error("Telegram bot token is invalid or revoked");
    }
    if (method === "setWebhook") throw new Error("Telegram webhook setup is temporarily unavailable");
    if (method === "getMe") throw new Error("Telegram bot validation is temporarily unavailable");
    throw new Error("Telegram delivery is temporarily unavailable");
  }
  return result.result;
}

export async function sendTelegram(env: WorkerEnv, chatId: string, message: string): Promise<void> {
  await telegramCall(env, "sendMessage", {
    chat_id: chatId,
    text: message,
    disable_web_page_preview: true,
  });
}

export async function createTelegramPairing(env: WorkerEnv): Promise<{ url: string; username: string }> {
  if (!env.SESSION_SIGNING_KEY || !env.PUBLIC_APP_URL || !env.TELEGRAM_WEBHOOK_SECRET) throw new Error("Telegram pairing secrets are incomplete");
  const bot = await telegramCall<{ username?: string }>(env, "getMe");
  if (!bot.username) throw new Error("Telegram bot username is unavailable");
  await telegramCall(env, "setWebhook", {
    url: `${env.PUBLIC_APP_URL.replace(/\/$/, "")}/api/telegram/webhook`,
    secret_token: env.TELEGRAM_WEBHOOK_SECRET,
    allowed_updates: ["message"],
  });
  const expires = Math.floor(Date.now() / 1000) + 600;
  const nonce = randomToken(8);
  const body = `${expires}_${nonce}`;
  const signature = (await signValue(`telegram:${body}`, env.SESSION_SIGNING_KEY)).slice(0, 22);
  const payload = `${body}_${signature}`;
  return { username: bot.username, url: `https://t.me/${bot.username}?start=${payload}` };
}

export async function handleTelegramWebhook(request: Request, env: WorkerEnv, state: AppState): Promise<Response> {
  if (!env.TELEGRAM_WEBHOOK_SECRET || !env.SESSION_SIGNING_KEY) return new Response("Not configured", { status: 503 });
  const header = request.headers.get("x-telegram-bot-api-secret-token") ?? "";
  if (!constantTimeEqual(header, env.TELEGRAM_WEBHOOK_SECRET)) return new Response("Forbidden", { status: 403 });
  const update = await request.json() as { message?: { text?: string; chat?: { id?: number } } };
  const text = update.message?.text ?? "";
  const chatId = update.message?.chat?.id;
  const payload = text.match(/^\/start(?:@\w+)?\s+([A-Za-z0-9_-]+)$/)?.[1];
  if (!payload || chatId === undefined) return new Response("ok");
  const parts = payload.split("_");
  if (parts.length !== 3) return new Response("ok");
  const [expiresRaw, nonce, signature] = parts;
  const body = `${expiresRaw}_${nonce}`;
  const expected = (await signValue(`telegram:${body}`, env.SESSION_SIGNING_KEY)).slice(0, 22);
  const valid = Number(expiresRaw) >= Date.now() / 1000 && constantTimeEqual(expected, signature);
  if (!valid || state.telegram_pair_used === payload) return new Response("ok");
  await writeAppState(env, {
    telegram_chat_id: String(chatId),
    telegram_connected_at: new Date().toISOString(),
    telegram_pair_used: payload,
  });
  await sendTelegram(env, String(chatId), "Spotify Logger connected. Reauthorization and sync alerts will arrive here.");
  return new Response("ok");
}
