import type { AppState, DashboardPayload, WorkerEnv } from "../types";
import { base64Url, bytes } from "./encoding";

type ServiceAccount = { client_email: string; private_key: string; token_uri?: string };
type SheetValues = { range: string; values?: string[][] };

let cachedGoogleToken: { value: string; expiresAt: number; identity: string } | null = null;

function requireGoogleConfig(env: WorkerEnv): { account: ServiceAccount; sheetId: string } {
  if (!env.GOOGLE_SERVICE_ACCOUNT_JSON || !env.USER_SHEET_ID) throw new Error("Google Sheet is not configured");
  return { account: JSON.parse(env.GOOGLE_SERVICE_ACCOUNT_JSON) as ServiceAccount, sheetId: env.USER_SHEET_ID };
}

function pemToDer(pem: string): Uint8Array<ArrayBuffer> {
  const raw = pem.replace(/-----[^-]+-----/g, "").replace(/\s/g, "");
  const binary = atob(raw);
  const output = new Uint8Array(binary.length);
  for (let index = 0; index < binary.length; index += 1) output[index] = binary.charCodeAt(index);
  return output;
}

async function googleAccessToken(env: WorkerEnv): Promise<string> {
  const { account } = requireGoogleConfig(env);
  if (cachedGoogleToken && cachedGoogleToken.expiresAt > Date.now() + 60_000 && cachedGoogleToken.identity === account.client_email) {
    return cachedGoogleToken.value;
  }
  const now = Math.floor(Date.now() / 1000);
  const header = base64Url(bytes(JSON.stringify({ alg: "RS256", typ: "JWT" })));
  const claims = base64Url(bytes(JSON.stringify({
    iss: account.client_email,
    scope: "https://www.googleapis.com/auth/spreadsheets",
    aud: account.token_uri ?? "https://oauth2.googleapis.com/token",
    iat: now,
    exp: now + 3600,
  })));
  const input = `${header}.${claims}`;
  const key = await crypto.subtle.importKey(
    "pkcs8",
    pemToDer(account.private_key),
    { name: "RSASSA-PKCS1-v1_5", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const assertion = `${input}.${base64Url(await crypto.subtle.sign("RSASSA-PKCS1-v1_5", key, bytes(input)))}`;
  const response = await fetch(account.token_uri ?? "https://oauth2.googleapis.com/token", {
    method: "POST",
    headers: { "content-type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({ grant_type: "urn:ietf:params:oauth:grant-type:jwt-bearer", assertion }),
  });
  if (!response.ok) throw new Error(`Google authentication failed (${response.status})`);
  const result = await response.json() as { access_token: string; expires_in: number };
  cachedGoogleToken = { value: result.access_token, expiresAt: Date.now() + result.expires_in * 1000, identity: account.client_email };
  return result.access_token;
}

async function sheetsFetch(env: WorkerEnv, path: string, init?: RequestInit): Promise<Response> {
  const response = await fetch(`https://sheets.googleapis.com/v4/${path}`, {
    ...init,
    headers: {
      authorization: `Bearer ${await googleAccessToken(env)}`,
      "content-type": "application/json",
      ...(init?.headers ?? {}),
    },
  });
  if (!response.ok) {
    const body = await response.text();
    throw new Error(`Google Sheets request failed (${response.status}): ${body.slice(0, 240)}`);
  }
  return response;
}

export async function readRanges(env: WorkerEnv, ranges: string[]): Promise<Record<string, string[][]>> {
  const { sheetId } = requireGoogleConfig(env);
  const params = new URLSearchParams();
  for (const range of ranges) params.append("ranges", range);
  params.set("majorDimension", "ROWS");
  const response = await sheetsFetch(env, `spreadsheets/${sheetId}/values:batchGet?${params}`);
  const result = await response.json() as { valueRanges?: SheetValues[] };
  const output: Record<string, string[][]> = {};
  ranges.forEach((range, index) => { output[range] = result.valueRanges?.[index]?.values ?? []; });
  return output;
}

export async function appendRows(env: WorkerEnv, range: string, values: Array<Array<string | number>>): Promise<void> {
  if (!values.length) return;
  const { sheetId } = requireGoogleConfig(env);
  const params = new URLSearchParams({ valueInputOption: "RAW", insertDataOption: "INSERT_ROWS" });
  await sheetsFetch(env, `spreadsheets/${sheetId}/values/${encodeURIComponent(range)}:append?${params}`, {
    method: "POST",
    body: JSON.stringify({ majorDimension: "ROWS", values }),
  });
}

export async function batchWrite(env: WorkerEnv, data: Array<{ range: string; values: Array<Array<string | number>> }>): Promise<void> {
  if (!data.length) return;
  const { sheetId } = requireGoogleConfig(env);
  await sheetsFetch(env, `spreadsheets/${sheetId}/values:batchUpdate`, {
    method: "POST",
    body: JSON.stringify({ valueInputOption: "RAW", data }),
  });
}

export function appStateFromRows(rows: string[][]): AppState {
  return Object.fromEntries(rows.filter((row) => row[0]).map((row) => [row[0], row[1] ?? ""]));
}

export async function readAppState(env: WorkerEnv): Promise<AppState> {
  const ranges = await readRanges(env, ["__app_state!A2:B"]);
  return appStateFromRows(ranges["__app_state!A2:B"]);
}

export async function writeAppState(env: WorkerEnv, changes: AppState): Promise<void> {
  const rows = (await readRanges(env, ["__app_state!A1:B"]))["__app_state!A1:B"];
  const positions = new Map<string, number>();
  rows.slice(1).forEach((row, index) => { if (row[0]) positions.set(row[0], index + 2); });
  const now = new Date().toISOString();
  const payload = { ...changes, updated_at: now };
  const updates: Array<{ range: string; values: string[][] }> = [];
  const additions: string[][] = [];
  for (const [key, value] of Object.entries(payload)) {
    const row = positions.get(key);
    if (row) updates.push({ range: `__app_state!A${row}:B${row}`, values: [[key, value]] });
    else additions.push([key, value]);
  }
  await batchWrite(env, updates);
  await appendRows(env, "__app_state!A:B", additions);
}

const monthIndexes: Record<string, number> = {
  January: 0, February: 1, March: 2, April: 3, May: 4, June: 5,
  July: 6, August: 7, September: 8, October: 9, November: 10, December: 11,
};

export function parseLegacyPlayedAt(value: string): string | null {
  const iso = Date.parse(value);
  if (Number.isFinite(iso)) return new Date(iso).toISOString();
  const match = value.match(/^(\w+)\s+(\d{1,2}),\s+(\d{4})\s+at\s+(\d{1,2}):(\d{2})(AM|PM)$/i);
  if (!match || monthIndexes[match[1]] === undefined) return null;
  let hour = Number(match[4]) % 12;
  if (match[6].toUpperCase() === "PM") hour += 12;
  const utc = Date.UTC(Number(match[3]), monthIndexes[match[1]], Number(match[2]), hour - 3, Number(match[5]));
  return new Date(utc).toISOString();
}

export async function loadDashboard(env: WorkerEnv): Promise<DashboardPayload> {
  const requested = [
    "log!A2:E", "__cache_tracks!A2:I", "__cache_artists!A2:F",
    "__cache_albums!A2:E", "__app_state!A2:B",
  ];
  const ranges = await readRanges(env, requested);
  const state = appStateFromRows(ranges["__app_state!A2:B"]);
  const tracks: DashboardPayload["tracks"] = {};
  for (const row of ranges["__cache_tracks!A2:I"]) {
    if (!row[0]) continue;
    tracks[row[0]] = { id: row[0], name: row[1] ?? "", durationMs: Number(row[2]) || 0, albumId: row[3] ?? "", coverUrl: row[4] ?? "", primaryArtistId: row[5] ?? "" };
  }
  const artists: DashboardPayload["artists"] = {};
  for (const row of ranges["__cache_artists!A2:F"]) {
    if (!row[0]) continue;
    artists[row[0]] = { id: row[0], name: row[1] ?? "", coverUrl: row[2] ?? "", genres: (row[3] ?? "").split(";").map((value) => value.trim()).filter(Boolean), primaryGenre: row[4] ?? "" };
  }
  const albums: DashboardPayload["albums"] = {};
  for (const row of ranges["__cache_albums!A2:E"]) {
    if (!row[0]) continue;
    albums[row[0]] = { id: row[0], name: row[1] ?? "", coverUrl: row[2] ?? "", releaseDate: row[3] ?? "" };
  }
  const plays = ranges["log!A2:E"].flatMap((row) => {
    const playedAt = parseLegacyPlayedAt(row[0] ?? "");
    if (!playedAt || !row[3]) return [];
    return [{ playedAt, trackName: row[1] ?? "", artistName: row[2] ?? "", trackId: row[3], trackUrl: row[4] ?? `https://open.spotify.com/track/${row[3]}` }];
  });
  return {
    configured: true,
    generatedAt: new Date().toISOString(),
    timezone: "Europe/Moscow",
    stale: false,
    lastSyncAt: state.last_sync_at || null,
    reauthorizationRequired: state.reauth_required === "true",
    plays,
    tracks,
    artists,
    albums,
  };
}

export function googleIsConfigured(env: WorkerEnv): boolean {
  return Boolean(env.GOOGLE_SERVICE_ACCOUNT_JSON && env.USER_SHEET_ID);
}
