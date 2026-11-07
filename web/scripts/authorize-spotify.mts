import { createServer } from "node:http";
import { randomBytes } from "node:crypto";
import { encryptToken } from "../lib/server/crypto";
import { encryptFernet } from "../lib/server/fernet";
import { writeAppState } from "../lib/server/google";
import type { WorkerEnv } from "../lib/types";

// Local, explicit reauthorization. No hosted callback or plaintext token output.
const required = ["GOOGLE_SERVICE_ACCOUNT_JSON", "USER_SHEET_ID", "SPOTIFY_CLIENT_ID", "SPOTIFY_CLIENT_SECRET"] as const;
if (process.env.GITHUB_ACTIONS === "true" || required.some(key => !process.env[key]) || (!process.env.TOKEN_ENCRYPTION_KEY && !process.env.FERNET_KEY)) {
  console.error(`Run locally with these environment variables: ${required.join(", ")}, and TOKEN_ENCRYPTION_KEY or FERNET_KEY`);
  process.exitCode = 1;
} else {
  const env = {
    ASSETS: { fetch: async () => new Response(null) },
    ...Object.fromEntries(required.map(key => [key, process.env[key]])),
    TOKEN_ENCRYPTION_KEY: process.env.TOKEN_ENCRYPTION_KEY,
    FERNET_KEY: process.env.FERNET_KEY,
    SYNC_ABORT_SIGNAL: AbortSignal.timeout(5 * 60_000),
  } as WorkerEnv;
  // Validate encryption before asking Spotify for a token.
  const field = env.TOKEN_ENCRYPTION_KEY ? "refresh_token_enc_v2" : "refresh_token_enc";
  const encrypt = (value: string) => env.TOKEN_ENCRYPTION_KEY ? encryptToken(value, env.TOKEN_ENCRYPTION_KEY) : encryptFernet(value, env.FERNET_KEY!);
  await encrypt("validate-key");
  const state = randomBytes(32).toString("hex");
  const redirect = "http://127.0.0.1:8888/callback";
  let handling = false;
  const server = createServer(async (request, response) => {
    const url = new URL(request.url ?? "/", redirect);
    if (url.pathname !== "/callback") { response.writeHead(404).end(); return; }
    if (url.searchParams.get("state") !== state || !url.searchParams.get("code")) {
      response.writeHead(400).end("Authorization was not completed. Retry the local command.");
      return;
    }
    if (handling) { response.writeHead(409).end("Already processing authorization."); return; }
    handling = true;
    try {
      const tokenResponse = await fetch("https://accounts.spotify.com/api/token", {
        method: "POST", signal: env.SYNC_ABORT_SIGNAL,
        headers: { authorization: `Basic ${btoa(`${env.SPOTIFY_CLIENT_ID}:${env.SPOTIFY_CLIENT_SECRET}`)}`, "content-type": "application/x-www-form-urlencoded" },
        body: new URLSearchParams({ grant_type: "authorization_code", code: url.searchParams.get("code")!, redirect_uri: redirect }),
      });
      const tokens = await tokenResponse.json() as { refresh_token?: string };
      if (!tokenResponse.ok || !tokens.refresh_token) throw new Error("Spotify authorization failed");
      const now = new Date();
      const due = new Date(now);
      due.setUTCMonth(due.getUTCMonth() + 6);
      await writeAppState(env, {
        [field]: await encrypt(tokens.refresh_token),
        spotify_authorized_at: now.toISOString(), spotify_reauth_due_at: due.toISOString(),
        reauth_required: "false", last_error: "",
      });
      response.writeHead(200, { "content-type": "text/plain; charset=utf-8", "cache-control": "no-store" }).end("Spotify connected. You can close this tab.");
      console.log("Encrypted authorization saved to Google Sheets. No listening history was changed.");
    } catch {
      response.writeHead(500).end("Could not save authorization. Check local configuration and retry.");
      console.error("Authorization failed. Check local configuration and Sheet access; no tokens were printed.");
      process.exitCode = 1;
    } finally { clearTimeout(timer); server.close(); }
  });
  const timer = setTimeout(() => { console.error("Authorization timed out."); process.exitCode = 1; server.close(); }, 5 * 60_000);
  server.once("error", () => { clearTimeout(timer); console.error("Could not bind 127.0.0.1:8888. Close the process using that port and retry."); process.exitCode = 1; });
  server.listen(8888, "127.0.0.1", () => {
    const params = new URLSearchParams({ response_type: "code", client_id: env.SPOTIFY_CLIENT_ID!, redirect_uri: redirect, scope: "user-read-recently-played user-read-private", state });
    console.log(`Open in your browser:\nhttps://accounts.spotify.com/authorize?${params}`);
  });
}
