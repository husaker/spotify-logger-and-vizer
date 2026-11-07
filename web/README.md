# Spotify Logger Web

The React/TypeScript web application lives here. Listening history is collected by a standalone Node entrypoint in GitHub Actions; cron-job.org dispatches it every five minutes. Collection uses Spotify and Google Sheets directly, without a hosted HTTP endpoint or D1.

## Architecture

- `scripts/run-sync.mts` is the standalone Actions collector.
- `scripts/authorize-spotify.mts` provides local Spotify reauthorization with a loopback callback.
- `lib/server/sync.ts` contains collection logic; `sync-write.ts` filters timestamps and writes explicit Sheet ranges.
- `/` and `/admin` are the legacy web dashboard and admin surfaces. They are not required for collection.
- `worker/index.ts` retains the legacy web APIs; collection in this source requires the Actions execution mode.
- No D1 or R2 database is used.

## Local configuration

Copy `.dev.vars.example` to `.dev.vars` and fill every value. Never commit `.dev.vars`.

The Actions collector can reuse the existing `FERNET_KEY` and `refresh_token_enc` authorization. The legacy web admin uses `TOKEN_ENCRYPTION_KEY`, a cryptographically secure 32-byte value encoded with base64url. Use independent, long random values for `SESSION_SIGNING_KEY`, `CRON_SECRET`, and `TELEGRAM_WEBHOOK_SECRET`.

Set the Spotify redirect URI to the exact deployed callback URL:

```text
https://YOUR_DEPLOYED_HOST/api/admin/spotify/callback
```

Share the listening-data Google Sheet with the service-account email as Editor. Preserve these worksheets and headers:

- `log`: `Date, Track, Artist, Spotify ID, URL`
- `__app_state`: `key, value`
- `__dedupe`: `dedupe_key`
- `__cache_tracks`, `__cache_artists`, and `__cache_albums`: existing cache schemas

## Operations

Follow [CRON_SETUP.md](./CRON_SETUP.md) for GitHub secrets, external dispatch, local Spotify authorization and safe cutover. Keep the legacy hosted `SCHEDULED_SYNC_ENABLED=false` and disable other writers before switching. A local code update does not change an already deployed Worker.

The workflow concurrency group serializes all runs. Each run deduplicates against the complete log by timestamp, preserves exact new timestamps and valid repeats, and writes an explicit range. A failed/ambiguous write is not blindly retried: the next run rereads the Sheet. Historical rows are not deleted. This requires the workflow to be the only writer; there is no distributed lock across other applications.

Token rotation is saved before later data/cache processing. Refresh credentials remain encrypted in `__app_state`. Reauthorize locally when required; no publicly hosted callback is necessary.

## Telegram pairing

1. Open `@BotFather` in Telegram and send `/newbot`.
2. Choose a display name and a unique username ending in `bot`.
3. Store the supplied token as the protected `TELEGRAM_BOT_TOKEN` runtime secret.
4. Open `/admin` and select **Connect Telegram**.
5. Press **Start** in Telegram using the signed ten-minute pairing link.
6. Return to `/admin` and send a test notification.

The bot sends idempotent reminders 30, 14, 7, and 1 day before Spotify reauthorization, as well as expiration, repeated-sync-failure, recovery, and successful-reauthorization alerts.

## Commands

```bash
npm install
npm run dev
npm run lint
npm test
```

`npm test` creates a production build before running analytics, rendered HTML, and security tests.
