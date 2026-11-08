import type { AppState, LoggerEnv } from "./types";
import { storedRefreshToken } from "./sync-token";
import { appendRows, batchWrite, ensureLogRows, readAppState, readRanges, writeAppState } from "./google";
import { recentlyPlayed, refreshSpotifyToken, spotifyArtist, SpotifyInvalidGrantError, type SpotifyTrack } from "./spotify";
import { persistUniqueLogRows } from "./sync-write";

async function upsertRows(env: LoggerEnv, range: string, rows: Array<Array<string | number>>, existingRows: string[][]): Promise<void> {
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

export async function runSync(env: LoggerEnv): Promise<{ added: number; lastSyncAt: string }> {
  if (env.SYNC_EXECUTION_MODE !== "github-actions") throw new Error("Synchronization runs only in the serialized GitHub Actions workflow");
  return runSerializedSync({ ...env, SYNC_ABORT_SIGNAL: env.SYNC_ABORT_SIGNAL ?? AbortSignal.timeout(120_000) });
}

async function runSerializedSync(env: LoggerEnv): Promise<{ added: number; lastSyncAt: string }> {
  let state = await readAppState(env);
  try {
    const stored = await storedRefreshToken(env, state);
    const tokens = await refreshSpotifyToken(env, stored.value);
    // Persist rotation immediately, in the same format that was decrypted.
    if (tokens.refreshToken) await writeAppState(env, await stored.save(tokens.refreshToken));
    const now = new Date().toISOString();
    const lastCursor = Number(state.last_synced_after_ts) || 0;
    const after = lastCursor ? Math.max(0, lastCursor - 120 * 60 * 1000) : Date.now() - 120 * 60 * 1000;
    const items = await recentlyPlayed(tokens.accessToken, after, env.SYNC_ABORT_SIGNAL);
    const ranges = await readRanges(env, ["log!A2:E", "__cache_tracks!A2:I", "__cache_artists!A2:F", "__cache_albums!A2:E"]);
    const logRows: string[][] = [];
    const trackRows = new Map<string, Array<string | number>>();
    const albumRows = new Map<string, string[]>();
    const artistIds = new Set<string>();
    let cursor = lastCursor;
    for (const item of items.sort((a, b) => Date.parse(a.played_at ?? "") - Date.parse(b.played_at ?? ""))) {
      const track = item.track;
      if (!item.played_at || !track?.id) continue;
      const playedMs = Date.parse(item.played_at);
      if (!Number.isFinite(playedMs)) continue;
      cursor = Math.max(cursor, playedMs);
      const trackRow = trackCacheRow(track, now);
      const albumRow = albumCacheRow(track, now);
      if (trackRow) trackRows.set(track.id, trackRow);
      if (albumRow) albumRows.set(albumRow[0], albumRow);
      for (const artist of track.artists ?? []) if (artist.id) artistIds.add(artist.id);
      logRows.push([new Date(playedMs).toISOString(), track.name ?? "", track.artists?.[0]?.name ?? "", track.id, track.external_urls?.spotify ?? `https://open.spotify.com/track/${track.id}`]);
    }
    const added = await persistUniqueLogRows(ranges["log!A2:E"], logRows, {
      ensureRows: lastRow => ensureLogRows(env, lastRow),
      write: updates => batchWrite(env, updates),
    });
    await upsertRows(env, "__cache_tracks", [...trackRows.values()], ranges["__cache_tracks!A2:I"]);
    await upsertRows(env, "__cache_albums", [...albumRows.values()], ranges["__cache_albums!A2:E"]);
    const knownArtists = new Set(ranges["__cache_artists!A2:F"].map((row) => row[0]).filter(Boolean));
    const artistRows: string[][] = [];
    for (const artistId of [...artistIds].filter((id) => !knownArtists.has(id)).slice(0, 25)) {
      const artist = await spotifyArtist(tokens.accessToken, artistId, env.SYNC_ABORT_SIGNAL);
      const genres = artist.genres ?? [];
      artistRows.push([artistId, artist.name ?? "", artist.images?.[0]?.url ?? "", genres.join("; "), genres[0] ?? "", now]);
    }
    await upsertRows(env, "__cache_artists", artistRows, ranges["__cache_artists!A2:F"]);
    const changes: AppState = {
      last_synced_after_ts: String(cursor),
      last_sync_at: now,
      last_error: "",
      consecutive_failures: "0",
      reauth_required: "false",
    };
    await writeAppState(env, changes);
    return { added, lastSyncAt: now };
  } catch (error) {
    state = await readAppState(env).catch(() => state);
    const failures = (Number(state.consecutive_failures) || 0) + 1;
    const expired = error instanceof SpotifyInvalidGrantError;
    await writeAppState({ ...env, SYNC_ABORT_SIGNAL: AbortSignal.timeout(1500) }, {
      consecutive_failures: String(failures),
      last_error: expired ? "Spotify reauthorization required" : error instanceof Error ? error.message.slice(0, 240) : "Synchronization failed",
      ...(expired ? { reauth_required: "true" } : {}),
    }).catch(() => undefined);
    throw error;
  }
}

// Can run before cutover: verifies the existing connection without collecting log rows
// or advancing the cursor. Only a rotated credential, if returned, is saved.
export async function checkSpotifyConnection(env: LoggerEnv): Promise<void> {
  const bounded = { ...env, SYNC_ABORT_SIGNAL: AbortSignal.timeout(45_000) };
  const state = await readAppState(bounded);
  const stored = await storedRefreshToken(bounded, state);
  const tokens = await refreshSpotifyToken(bounded, stored.value);
  if (tokens.refreshToken) await writeAppState(bounded, await stored.save(tokens.refreshToken));
  await recentlyPlayed(tokens.accessToken, Date.now() - 120 * 60_000, bounded.SYNC_ABORT_SIGNAL);
}
