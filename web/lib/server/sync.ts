import type { AppState, WorkerEnv } from "../types";
import { decryptToken, encryptToken } from "./crypto";
import { appendRows, batchWrite, readAppState, readRanges, writeAppState } from "./google";
import { notificationChanges, notifyExpired, notifyFailure, notifyRecovery } from "./notifications";
import { recentlyPlayed, refreshSpotifyToken, spotifyArtist, SpotifyInvalidGrantError, type SpotifyTrack } from "./spotify";

const MONTHS = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"];

function moscowDisplay(iso: string): string {
  const date = new Date(iso);
  const shifted = new Date(date.getTime() + 3 * 60 * 60 * 1000);
  const hour = shifted.getUTCHours();
  const clock = `${hour % 12 || 12}:${String(shifted.getUTCMinutes()).padStart(2, "0")}${hour >= 12 ? "PM" : "AM"}`;
  return `${MONTHS[shifted.getUTCMonth()]} ${shifted.getUTCDate()}, ${shifted.getUTCFullYear()} at ${clock}`;
}

async function upsertRows(env: WorkerEnv, range: string, rows: Array<Array<string | number>>, existingRows: string[][]): Promise<void> {
  const positions = new Map<string, number>();
  existingRows.forEach((row, index) => { if (row[0]) positions.set(row[0], index + 2); });
  const updates: Array<{ range: string; values: Array<Array<string | number>> }> = [];
  const additions: Array<Array<string | number>> = [];
  for (const row of rows) {
    const position = positions.get(String(row[0]));
    if (position) updates.push({ range: `${range}!A${position}:${String.fromCharCode(64 + row.length)}${position}`, values: [row] });
    else additions.push(row);
  }
  await batchWrite(env, updates);
  await appendRows(env, `${range}!A:${String.fromCharCode(64 + (rows[0]?.length ?? 1))}`, additions);
}

function trackCacheRow(track: SpotifyTrack, now: string): Array<string | number> | null {
  if (!track.id) return null;
  const artistIds = (track.artists ?? []).map((artist) => artist.id ?? "").filter(Boolean);
  return [
    track.id, track.name ?? "", track.duration_ms ?? 0, track.album?.id ?? "",
    track.album?.images?.[0]?.url ?? "", artistIds[0] ?? "", artistIds.join(";"),
    track.external_urls?.spotify ?? `https://open.spotify.com/track/${track.id}`, now,
  ];
}

function albumCacheRow(track: SpotifyTrack, now: string): string[] | null {
  if (!track.album?.id) return null;
  return [track.album.id, track.album.name ?? "", track.album.images?.[0]?.url ?? "", track.album.release_date ?? "", now];
}

export async function runSync(env: WorkerEnv): Promise<{ added: number; lastSyncAt: string }> {
  if (!env.TOKEN_ENCRYPTION_KEY) throw new Error("TOKEN_ENCRYPTION_KEY is not configured");
  let state = await readAppState(env);
  const encrypted = state.refresh_token_enc_v2;
  if (!encrypted) {
    await writeAppState(env, { reauth_required: "true", last_error: "Spotify reconnect required" });
    throw new SpotifyInvalidGrantError("Spotify reconnect required");
  }
  try {
    const refreshToken = await decryptToken(encrypted, env.TOKEN_ENCRYPTION_KEY);
    const tokens = await refreshSpotifyToken(env, refreshToken);
    const now = new Date().toISOString();
    const lastCursor = Number(state.last_synced_after_ts) || 0;
    const after = lastCursor ? Math.max(0, lastCursor - 120 * 60 * 1000) : Date.now() - 120 * 60 * 1000;
    const items = await recentlyPlayed(tokens.accessToken, after);
    const ranges = await readRanges(env, ["__dedupe!A2:A", "__cache_tracks!A2:I", "__cache_artists!A2:F", "__cache_albums!A2:E"]);
    const dedupe = new Set(ranges["__dedupe!A2:A"].slice(-5000).map((row) => row[0]).filter(Boolean));
    const logRows: string[][] = [];
    const dedupeRows: string[][] = [];
    const trackRows = new Map<string, Array<string | number>>();
    const albumRows = new Map<string, string[]>();
    const artistIds = new Set<string>();
    let cursor = lastCursor;
    for (const item of items) {
      const track = item.track;
      if (!item.played_at || !track?.id) continue;
      const key = `${item.played_at}|${track.id}`;
      const playedMs = Date.parse(item.played_at);
      if (Number.isFinite(playedMs)) cursor = Math.max(cursor, playedMs);
      const trackRow = trackCacheRow(track, now);
      const albumRow = albumCacheRow(track, now);
      if (trackRow) trackRows.set(track.id, trackRow);
      if (albumRow) albumRows.set(albumRow[0], albumRow);
      for (const artist of track.artists ?? []) if (artist.id) artistIds.add(artist.id);
      if (dedupe.has(key)) continue;
      logRows.push([moscowDisplay(item.played_at), track.name ?? "", track.artists?.[0]?.name ?? "", track.id, track.external_urls?.spotify ?? `https://open.spotify.com/track/${track.id}`]);
      dedupeRows.push([key]);
      dedupe.add(key);
    }
    await appendRows(env, "log!A:E", logRows);
    await appendRows(env, "__dedupe!A:A", dedupeRows);
    await upsertRows(env, "__cache_tracks", [...trackRows.values()], ranges["__cache_tracks!A2:I"]);
    await upsertRows(env, "__cache_albums", [...albumRows.values()], ranges["__cache_albums!A2:E"]);
    const knownArtists = new Set(ranges["__cache_artists!A2:F"].map((row) => row[0]).filter(Boolean));
    const artistRows: string[][] = [];
    for (const artistId of [...artistIds].filter((id) => !knownArtists.has(id)).slice(0, 25)) {
      const artist = await spotifyArtist(tokens.accessToken, artistId);
      const genres = artist.genres ?? [];
      artistRows.push([artistId, artist.name ?? "", artist.images?.[0]?.url ?? "", genres.join("; "), genres[0] ?? "", now]);
    }
    await upsertRows(env, "__cache_artists", artistRows, ranges["__cache_artists!A2:F"]);
    const recovery = await notifyRecovery(env, state).catch(() => ({}));
    const tokenChange: AppState = tokens.refreshToken ? { refresh_token_enc_v2: await encryptToken(tokens.refreshToken, env.TOKEN_ENCRYPTION_KEY) } : {};
    const reminder = await notificationChanges(env, state).catch(() => ({}));
    const changes: AppState = {
      ...tokenChange,
      ...recovery,
      ...reminder,
      last_synced_after_ts: String(cursor),
      last_sync_at: now,
      last_error: "",
      consecutive_failures: "0",
      reauth_required: "false",
    };
    await writeAppState(env, changes);
    return { added: logRows.length, lastSyncAt: now };
  } catch (error) {
    state = await readAppState(env).catch(() => state);
    const failures = (Number(state.consecutive_failures) || 0) + 1;
    const expired = error instanceof SpotifyInvalidGrantError;
    const notification = expired
      ? await notifyExpired(env, state).catch(() => ({}))
      : await notifyFailure(env, state, failures).catch(() => ({}));
    await writeAppState(env, {
      ...notification,
      consecutive_failures: String(failures),
      last_error: expired ? "Spotify reauthorization required" : error instanceof Error ? error.message.slice(0, 240) : "Synchronization failed",
      ...(expired ? { reauth_required: "true" } : {}),
    }).catch(() => undefined);
    throw error;
  }
}
