import { checkSpotifyConnection, runSync } from "../src/sync";
import { checkSyncHealth } from "../src/sync-health";
import { SpotifyInvalidGrantError } from "../src/spotify";
import type { LoggerEnv } from "../src/types";

// Never source runtime secrets from Sites. All credentials come from Actions secrets.
if (process.env.GITHUB_ACTIONS !== "true" || process.env.GITHUB_WORKFLOW !== "Spotify Logger Sync") {
  console.error("Run this collector through the Spotify Logger Sync workflow, which serializes writes.");
  process.exitCode = 1;
} else {
  const healthOnly = process.argv.includes("--health");
  const keys = healthOnly ? ["GOOGLE_SERVICE_ACCOUNT_JSON", "USER_SHEET_ID"] : ["GOOGLE_SERVICE_ACCOUNT_JSON", "USER_SHEET_ID", "SPOTIFY_CLIENT_ID", "SPOTIFY_CLIENT_SECRET"];
  const missing: string[] = keys.filter(key => !process.env[key]);
  if (!healthOnly && !process.env.TOKEN_ENCRYPTION_KEY && !process.env.FERNET_KEY) missing.push("TOKEN_ENCRYPTION_KEY or FERNET_KEY");
  if (missing.length) {
    console.error(`Missing repository secrets: ${missing.join(", ")}`);
    process.exitCode = 1;
  } else {
    const env: LoggerEnv = {
      ...Object.fromEntries(keys.map(key => [key, process.env[key]])),
      TOKEN_ENCRYPTION_KEY: process.env.TOKEN_ENCRYPTION_KEY,
      FERNET_KEY: process.env.FERNET_KEY,
      SYNC_EXECUTION_MODE: "github-actions",
    };
    try {
      if (healthOnly) {
        await checkSyncHealth(env);
        console.log("Logger health is OK: last successful sync is less than 30 minutes old.");
      } else if (process.argv.includes("--check")) {
        await checkSpotifyConnection(env);
        console.log("Spotify connection verified. No listening log rows or sync cursor were changed.");
      } else {
        const result = await runSync(env);
        console.log(`Sync completed: ${result.added} new events; last success ${result.lastSyncAt}.`);
      }
    } catch (error) {
      // API error bodies can contain private values. Detailed state stays in the Sheet.
      if (error instanceof SpotifyInvalidGrantError) console.error("Stored Spotify authorization has expired or was revoked; reconnect Spotify locally.");
      console.error(healthOnly
        ? "Logger health check failed: synchronization is stale, authorization is required, or the Sheet is unavailable. Check the latest collection run."
        : "Spotify sync failed. Check credentials, Spotify authorization and the Sheet's __app_state status.");
      process.exitCode = 1;
    }
  }
}
