import type { AppState, LoggerEnv } from "./types";
import { readAppState } from "./google";

export function syncHealth(state: AppState, now = Date.now()): { healthy: boolean; reason: string } {
  const synced = Date.parse(state.last_sync_at || "");
  if (state.reauth_required === "true") return { healthy: false, reason: "Spotify needs reauthorization" };
  if (!Number.isFinite(synced)) return { healthy: false, reason: "No successful synchronization recorded" };
  if (synced > now + 60_000) return { healthy: false, reason: "Invalid future synchronization timestamp" };
  if (now - synced >= 30 * 60_000) return { healthy: false, reason: "No successful synchronization for at least 30 minutes" };
  return { healthy: true, reason: "Synchronization is fresh" };
}

// This probe never calls Spotify, refreshes credentials, writes app state or advances
// the collector cursor. It must not itself make the last-success timestamp fresh.
export async function checkSyncHealth(env: LoggerEnv): Promise<void> {
  const state = await readAppState({ ...env, SYNC_ABORT_SIGNAL: AbortSignal.timeout(30_000) });
  const result = syncHealth(state);
  if (!result.healthy) throw new Error(result.reason);
}
