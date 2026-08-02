# Spotify Logger Web

The production application is a React/TypeScript dashboard hosted with OpenAI Sites. Its server entry point uses the Worker runtime for same-origin APIs and a five-minute scheduled collector. One Google Sheet remains authoritative for listening history, metadata caches, deduplication, and application state.

## Architecture

- `/` is the public, read-only listening dashboard.
- `/admin` is protected by the dedicated application password.
- `worker/index.ts` serves the API, Spotify callback, Telegram webhook, and scheduled sync.
- `lib/server/` contains Google, Spotify, encryption, session, synchronization, and notification code.
- No D1 or R2 database is used.

## Local configuration

Copy `.dev.vars.example` to `.dev.vars` and fill every value. Never commit `.dev.vars`.

Generate `TOKEN_ENCRYPTION_KEY` as a cryptographically secure 32-byte value encoded with base64url. Use independent, long random values for `SESSION_SIGNING_KEY` and `TELEGRAM_WEBHOOK_SECRET`.

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

Keep `SCHEDULED_SYNC_ENABLED=false` until Spotify is connected and a manual sync succeeds. Then set it to `true`; the installed `*/5 * * * *` trigger begins collecting recent plays.

Spotify authorization is renewed from `/admin`. The reconnect creates a new six-month authorization cycle without changing listening history.

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
