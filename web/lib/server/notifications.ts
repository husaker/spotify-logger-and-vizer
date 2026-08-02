import type { AppState, WorkerEnv } from "../types";
import { sendTelegram } from "./telegram";

export async function notificationChanges(env: WorkerEnv, state: AppState): Promise<AppState> {
  const chatId = state.telegram_chat_id;
  const dueAt = Date.parse(state.spotify_reauth_due_at ?? "");
  const authorizedAt = state.spotify_authorized_at ?? "";
  if (!chatId || !Number.isFinite(dueAt) || !authorizedAt || !env.PUBLIC_APP_URL) return {};
  const days = Math.ceil((dueAt - Date.now()) / 86_400_000);
  const threshold = [1, 7, 14, 30].find((value) => days <= value && state[`notify_reauth_${value}_for`] !== authorizedAt);
  if (!threshold) return {};
  await sendTelegram(
    env,
    chatId,
    `Spotify authorization expires in ${Math.max(0, days)} day${days === 1 ? "" : "s"}. Reconnect securely: ${env.PUBLIC_APP_URL.replace(/\/$/, "")}/admin`,
  );
  return { [`notify_reauth_${threshold}_for`]: authorizedAt };
}

export async function notifyExpired(env: WorkerEnv, state: AppState): Promise<AppState> {
  const cycle = state.spotify_authorized_at ?? "unknown";
  if (!state.telegram_chat_id || state.notify_expired_for === cycle || !env.PUBLIC_APP_URL) return {};
  await sendTelegram(env, state.telegram_chat_id, `Spotify authorization expired. Your history is safe. Reconnect here: ${env.PUBLIC_APP_URL.replace(/\/$/, "")}/admin`);
  return { notify_expired_for: cycle };
}

export async function notifyFailure(env: WorkerEnv, state: AppState, failures: number): Promise<AppState> {
  if (!state.telegram_chat_id || failures !== 3 || state.notify_failure_active === "true" || !env.PUBLIC_APP_URL) return {};
  await sendTelegram(env, state.telegram_chat_id, `Spotify Logger has failed to synchronize three times. Check the protected status page: ${env.PUBLIC_APP_URL.replace(/\/$/, "")}/admin`);
  return { notify_failure_active: "true" };
}

export async function notifyRecovery(env: WorkerEnv, state: AppState): Promise<AppState> {
  if (!state.telegram_chat_id || state.notify_failure_active !== "true") return {};
  await sendTelegram(env, state.telegram_chat_id, "Spotify Logger synchronization recovered and is collecting plays again.");
  return { notify_failure_active: "false" };
}

export async function notifyReauthorized(env: WorkerEnv, state: AppState): Promise<void> {
  if (!state.telegram_chat_id) return;
  await sendTelegram(env, state.telegram_chat_id, "Spotify was reauthorized successfully. Automatic listening-history collection is active.");
}

