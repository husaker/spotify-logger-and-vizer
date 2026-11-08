import { parseLegacyPlayedAt } from "./google";

// The log is authoritative even if the previous run failed before updating __dedupe.
// Legacy rows only stored Moscow minutes; new rows retain the exact Spotify timestamp.
export function createTimestampGuard(logRows: string[][], dedupeRows: string[][]): (playedAt: string) => string | null {
  const exact = new Set<number>();
  const legacyMinutes = new Set<number>();
  for (const [value] of logRows) {
    const parsed = parseLegacyPlayedAt(value ?? "");
    if (!parsed) continue;
    const timestamp = Date.parse(parsed);
    if (/^\d{4}-\d{2}-\d{2}T/.test(value)) exact.add(timestamp);
    else legacyMinutes.add(Math.floor(timestamp / 60_000));
  }
  for (const [key] of dedupeRows) {
    const value = (key ?? "").split("|")[0];
    if (!/^\d{4}-\d{2}-\d{2}T/.test(value)) continue;
    const timestamp = Date.parse(value);
    if (Number.isFinite(timestamp)) exact.add(timestamp);
  }
  return (playedAt) => {
    const timestamp = Date.parse(playedAt);
    if (!Number.isFinite(timestamp) || exact.has(timestamp) || legacyMinutes.has(Math.floor(timestamp / 60_000))) return null;
    exact.add(timestamp);
    return new Date(timestamp).toISOString();
  };
}
