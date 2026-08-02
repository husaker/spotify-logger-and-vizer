import type { WorkerEnv } from "../types";

export class SpotifyInvalidGrantError extends Error {}

type SpotifyImage = { url?: string };
export type SpotifyArtist = { id?: string; name?: string; genres?: string[]; images?: SpotifyImage[] };
export type SpotifyAlbum = { id?: string; name?: string; release_date?: string; images?: SpotifyImage[] };
export type SpotifyTrack = {
  id?: string;
  name?: string;
  duration_ms?: number;
  album?: SpotifyAlbum;
  artists?: SpotifyArtist[];
  external_urls?: { spotify?: string };
};
export type RecentlyPlayedItem = { played_at?: string; track?: SpotifyTrack };

function spotifyConfig(env: WorkerEnv): { id: string; secret: string } {
  if (!env.SPOTIFY_CLIENT_ID || !env.SPOTIFY_CLIENT_SECRET) throw new Error("Spotify is not configured");
  return { id: env.SPOTIFY_CLIENT_ID, secret: env.SPOTIFY_CLIENT_SECRET };
}

async function retryingFetch(input: string, init: RequestInit, attempts = 4): Promise<Response> {
  let last: Response | null = null;
  for (let attempt = 0; attempt < attempts; attempt += 1) {
    const response = await fetch(input, init);
    last = response;
    if (response.status !== 429 && response.status < 500) return response;
    const retryAfter = Number(response.headers.get("retry-after"));
    const delay = Number.isFinite(retryAfter) ? retryAfter * 1000 : Math.min(8_000, 500 * (2 ** attempt));
    await new Promise((resolve) => setTimeout(resolve, delay));
  }
  return last!;
}

export function spotifyAuthorizationUrl(env: WorkerEnv, state: string): string {
  const { id } = spotifyConfig(env);
  if (!env.PUBLIC_APP_URL) throw new Error("PUBLIC_APP_URL is not configured");
  const params = new URLSearchParams({
    response_type: "code",
    client_id: id,
    redirect_uri: `${env.PUBLIC_APP_URL.replace(/\/$/, "")}/api/admin/spotify/callback`,
    scope: "user-read-recently-played user-read-private",
    state,
    show_dialog: "true",
  });
  return `https://accounts.spotify.com/authorize?${params}`;
}

async function tokenRequest(env: WorkerEnv, body: URLSearchParams): Promise<Record<string, unknown>> {
  const { id, secret } = spotifyConfig(env);
  const response = await retryingFetch("https://accounts.spotify.com/api/token", {
    method: "POST",
    headers: {
      authorization: `Basic ${btoa(`${id}:${secret}`)}`,
      "content-type": "application/x-www-form-urlencoded",
    },
    body,
  });
  const result = await response.json() as Record<string, unknown>;
  if (!response.ok) {
    if (response.status === 400 && result.error === "invalid_grant") throw new SpotifyInvalidGrantError("Spotify authorization expired");
    throw new Error(`Spotify token request failed (${response.status})`);
  }
  return result;
}

export async function exchangeSpotifyCode(env: WorkerEnv, code: string): Promise<{ accessToken: string; refreshToken: string }> {
  if (!env.PUBLIC_APP_URL) throw new Error("PUBLIC_APP_URL is not configured");
  const result = await tokenRequest(env, new URLSearchParams({
    grant_type: "authorization_code",
    code,
    redirect_uri: `${env.PUBLIC_APP_URL.replace(/\/$/, "")}/api/admin/spotify/callback`,
  }));
  if (!result.refresh_token) throw new Error("Spotify did not return a refresh token");
  return { accessToken: String(result.access_token), refreshToken: String(result.refresh_token) };
}

export async function refreshSpotifyToken(env: WorkerEnv, refreshToken: string): Promise<{ accessToken: string; refreshToken?: string }> {
  const result = await tokenRequest(env, new URLSearchParams({ grant_type: "refresh_token", refresh_token: refreshToken }));
  return { accessToken: String(result.access_token), refreshToken: result.refresh_token ? String(result.refresh_token) : undefined };
}

async function spotifyGet<T>(url: string, accessToken: string): Promise<T> {
  const response = await retryingFetch(url, { headers: { authorization: `Bearer ${accessToken}` } });
  if (!response.ok) throw new Error(`Spotify API request failed (${response.status})`);
  return response.json() as Promise<T>;
}

export async function spotifyUserId(accessToken: string): Promise<string> {
  const result = await spotifyGet<{ id: string }>("https://api.spotify.com/v1/me", accessToken);
  return result.id;
}

export async function recentlyPlayed(accessToken: string, afterMs: number): Promise<RecentlyPlayedItem[]> {
  let url: string | null = `https://api.spotify.com/v1/me/player/recently-played?${new URLSearchParams({ limit: "50", after: String(Math.max(0, afterMs)) })}`;
  const items: RecentlyPlayedItem[] = [];
  for (let page = 0; url && page < 5; page += 1) {
    const result: { items?: RecentlyPlayedItem[]; next?: string | null } = await spotifyGet(url, accessToken);
    items.push(...(result.items ?? []));
    url = result.next ?? null;
  }
  return items;
}

export async function spotifyArtist(accessToken: string, artistId: string): Promise<SpotifyArtist> {
  return spotifyGet<SpotifyArtist>(`https://api.spotify.com/v1/artists/${encodeURIComponent(artistId)}`, accessToken);
}
