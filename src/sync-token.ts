import type { AppState, LoggerEnv } from "./types";
import { decryptToken, encryptToken } from "./crypto";
import { decryptFernet, encryptFernet } from "./fernet";

export async function storedRefreshToken(env: LoggerEnv, state: AppState): Promise<{ value: string; save(value: string): Promise<AppState> }> {
  // Prefer the configured modern format. Never silently fall back after bad decryption.
  if (env.TOKEN_ENCRYPTION_KEY && state.refresh_token_enc_v2) return {
    value: await decryptToken(state.refresh_token_enc_v2, env.TOKEN_ENCRYPTION_KEY),
    save: async value => ({ refresh_token_enc_v2: await encryptToken(value, env.TOKEN_ENCRYPTION_KEY!) }),
  };
  if (env.FERNET_KEY && state.refresh_token_enc) return {
    value: await decryptFernet(state.refresh_token_enc, env.FERNET_KEY),
    save: async value => ({ refresh_token_enc: await encryptFernet(value, env.FERNET_KEY!) }),
  };
  throw new Error("No stored Spotify authorization matches the configured encryption key");
}
