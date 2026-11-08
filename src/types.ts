export type LoggerEnv = {
  SYNC_ABORT_SIGNAL?: AbortSignal;
  SYNC_EXECUTION_MODE?: "github-actions";
  GOOGLE_SERVICE_ACCOUNT_JSON?: string;
  USER_SHEET_ID?: string;
  SPOTIFY_CLIENT_ID?: string;
  SPOTIFY_CLIENT_SECRET?: string;
  TOKEN_ENCRYPTION_KEY?: string;
  FERNET_KEY?: string;
};

export type AppState = Record<string, string>;
