import type { AppState, LoggerEnv } from "./types";
import { base64Url, bytes } from "./encoding";

type ServiceAccount = { client_email: string; private_key: string; token_uri?: string };
type SheetValues = { range: string; values?: string[][] };

let cachedGoogleToken: { value: string; expiresAt: number; identity: string } | null = null;

function requireGoogleConfig(env: LoggerEnv): { account: ServiceAccount; sheetId: string } {
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

async function googleAccessToken(env: LoggerEnv): Promise<string> {
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
    signal: env.SYNC_ABORT_SIGNAL,
    body: new URLSearchParams({ grant_type: "urn:ietf:params:oauth:grant-type:jwt-bearer", assertion }),
  });
  if (!response.ok) throw new Error(`Google authentication failed (${response.status})`);
  const result = await response.json() as { access_token: string; expires_in: number };
  cachedGoogleToken = { value: result.access_token, expiresAt: Date.now() + result.expires_in * 1000, identity: account.client_email };
  return result.access_token;
}

async function sheetsFetch(env: LoggerEnv, path: string, init?: RequestInit): Promise<Response> {
  const response = await fetch(`https://sheets.googleapis.com/v4/${path}`, {
    ...init,
    signal: env.SYNC_ABORT_SIGNAL,
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

export async function readRanges(env: LoggerEnv, ranges: string[]): Promise<Record<string, string[][]>> {
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

export async function appendRows(env: LoggerEnv, range: string, values: Array<Array<string | number>>): Promise<void> {
  if (!values.length) return;
  const { sheetId } = requireGoogleConfig(env);
  const params = new URLSearchParams({ valueInputOption: "RAW", insertDataOption: "INSERT_ROWS" });
  await sheetsFetch(env, `spreadsheets/${sheetId}/values/${encodeURIComponent(range)}:append?${params}`, {
    method: "POST",
    body: JSON.stringify({ majorDimension: "ROWS", values }),
  });
}

export async function ensureLogRows(env: LoggerEnv, lastRow: number): Promise<void> {
  const { sheetId } = requireGoogleConfig(env);
  const response = await sheetsFetch(env, `spreadsheets/${sheetId}?fields=sheets.properties`);
  const data = await response.json() as { sheets?: Array<{ properties: { sheetId: number; title: string; gridProperties?: { rowCount?: number } } }> };
  const log = data.sheets?.find(sheet => sheet.properties.title === "log")?.properties;
  const rowCount = log?.gridProperties?.rowCount;
  if (!log || !rowCount) throw new Error("Listening log worksheet is unavailable");
  if (lastRow <= rowCount) return;
  await sheetsFetch(env, `spreadsheets/${sheetId}:batchUpdate`, {
    method: "POST",
    body: JSON.stringify({ requests: [{ appendDimension: { sheetId: log.sheetId, dimension: "ROWS", length: Math.max(1000, lastRow - rowCount) } }] }),
  });
}

export async function batchWrite(env: LoggerEnv, data: Array<{ range: string; values: Array<Array<string | number>> }>): Promise<void> {
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

export async function readAppState(env: LoggerEnv): Promise<AppState> {
  const ranges = await readRanges(env, ["__app_state!A2:B"]);
  return appStateFromRows(ranges["__app_state!A2:B"]);
}

export async function writeAppState(env: LoggerEnv, changes: AppState): Promise<void> {
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
