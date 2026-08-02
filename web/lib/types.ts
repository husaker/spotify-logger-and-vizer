export type Play = {
  playedAt: string;
  trackId: string;
  trackName: string;
  artistName: string;
  trackUrl: string;
};

export type TrackMeta = {
  id: string;
  name: string;
  durationMs: number;
  albumId: string;
  coverUrl: string;
  primaryArtistId: string;
};

export type ArtistMeta = {
  id: string;
  name: string;
  coverUrl: string;
  genres: string[];
  primaryGenre: string;
};

export type AlbumMeta = {
  id: string;
  name: string;
  coverUrl: string;
  releaseDate: string;
};

export type DashboardPayload = {
  configured: boolean;
  generatedAt: string;
  timezone: "Europe/Moscow";
  stale: boolean;
  lastSyncAt: string | null;
  reauthorizationRequired: boolean;
  plays: Play[];
  tracks: Record<string, TrackMeta>;
  artists: Record<string, ArtistMeta>;
  albums: Record<string, AlbumMeta>;
};

export type AdminStatus = {
  configured: boolean;
  spotifyConnected: boolean;
  reauthorizationRequired: boolean;
  authorizedAt: string | null;
  reauthorizationDueAt: string | null;
  lastSyncAt: string | null;
  lastError: string | null;
  consecutiveFailures: number;
  telegramConnected: boolean;
  telegramConnectedAt: string | null;
};

export type WorkerEnv = {
  ASSETS: { fetch(request: Request): Promise<Response> };
  GOOGLE_SERVICE_ACCOUNT_JSON?: string;
  USER_SHEET_ID?: string;
  SPOTIFY_CLIENT_ID?: string;
  SPOTIFY_CLIENT_SECRET?: string;
  TOKEN_ENCRYPTION_KEY?: string;
  ADMIN_PASSWORD?: string;
  SESSION_SIGNING_KEY?: string;
  PUBLIC_APP_URL?: string;
  TELEGRAM_BOT_TOKEN?: string;
  TELEGRAM_WEBHOOK_SECRET?: string;
  DEMO_MODE?: string;
  SCHEDULED_SYNC_ENABLED?: string;
};

export type AppState = Record<string, string>;
