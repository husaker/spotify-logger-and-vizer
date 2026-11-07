# GitHub Actions + cron-job.org

The collector executes in GitHub Actions. cron-job.org only dispatches the workflow through GitHub's API. Neither a VPS nor OpenAI Sites is needed for collection. The local personal portfolio continues reading Google Sheets independently.

## 1. Prepare GitHub

Merge `.github/workflows/sync.yml` and the collector changes into the repository's default branch (`main`). `workflow_dispatch` must be present on that branch. The workflow intentionally has no GitHub `schedule` trigger. Keep its `spotify-logger-sync` concurrency group and `cancel-in-progress: false`: every manual run, rerun, branch and external dispatch must use that same group.

Under repository **Settings → Secrets and variables → Actions**, set:

| Repository secret | Value |
| --- | --- |
| `GOOGLE_SERVICE_ACCOUNT_JSON` | Complete service-account JSON; its email must have Editor access to this workbook |
| `USER_SHEET_ID` | `1EPpXNIcrQj6xu74GNYjvTBehiQMjsB1mPZfaJZeb5CM` |
| `SPOTIFY_CLIENT_ID` | Existing Spotify application client ID |
| `SPOTIFY_CLIENT_SECRET` | Existing Spotify application secret |
| `FERNET_KEY` | Existing GitHub secret; decrypts the original `__app_state.refresh_token_enc` |
| `TOKEN_ENCRYPTION_KEY` | Optional alternative: decrypts `__app_state.refresh_token_enc_v2` |
| `TELEGRAM_BOT_TOKEN` | Optional; enables existing recovery notifications |

Use the existing `FERNET_KEY` when available; no Sites encryption-key transfer is needed. If a modern `TOKEN_ENCRYPTION_KEY` and v2 token are both configured, that format is preferred. Bad modern decryption never silently falls back. Rotated credentials are saved in the format that was read. If neither key is available, use the local authorization flow below with a new key and set that same key in GitHub. Never place keys in chat, commits or workflow logs. `CRON_SECRET`, admin password and Sites URL are not required.

## 2. Authorize Spotify without hosting

Existing Fernet or v2 authorization in the Sheet remains usable with its corresponding encryption key. For a new connection or later reauthorization:

1. Add `http://127.0.0.1:8888/callback` to the Spotify application's allowed Redirect URIs.
2. Put the four connection values and either `FERNET_KEY` or `TOKEN_ENCRYPTION_KEY` in an ignored local `web/.env.logger.local` file (never commit it). Use the same chosen encryption key as GitHub.
3. Pause dispatches and wait for the active workflow to finish, so token/state edits do not overlap collection.
4. From `web`, run `node --env-file=.env.logger.local --import tsx scripts/authorize-spotify.mts` and open the printed authorization URL.
5. The loopback callback encrypts the refresh token and saves it directly to the existing Sheet. No refresh token is printed and no history is removed. Resume dispatches.

## 3. Prepare cron-job.org (initially disabled)

Create a fine-grained GitHub personal access token, select **Only select repositories → husaker/spotify-logger-and-vizer**, and give **Repository permissions → Actions: Read and write**. Set an expiration and a reminder to replace it before expiry. Store this token only in cron-job.org's authorization header. It is distinct from all Spotify/Google secrets.

| Setting | Value |
| --- | --- |
| Title | Spotify Logger |
| URL | `https://api.github.com/repos/husaker/spotify-logger-and-vizer/actions/workflows/sync.yml/dispatches` |
| Schedule | Every 5 minutes (`*/5 * * * *`) |
| Method | POST |
| Header | `Authorization: Bearer <GITHUB_FINE_GRAINED_TOKEN>` |
| Header | `Accept: application/vnd.github+json` |
| Header | `Content-Type: application/json` |
| Header | `X-GitHub-Api-Version: 2022-11-28` |
| Body | `{"ref":"main"}` |
| Notifications | Failure, recovery, automatic deactivation |

Use the repository's actual default branch if it changes. Do not put credentials in the URL or JSON body. Do not use a broad classic token. GitHub currently scopes this permission to Actions for the selected repository, not one workflow.

## 4. Switch safely

1. Back up the Sheet. Prepare the new cron job disabled and configure GitHub secrets.
2. Stop the old GitHub workflow and wait for any active run to finish before merging/releasing the replacement. Keep the old hosted `SCHEDULED_SYNC_ENABLED=false`; stop any other external triggers and do not use its manual sync button. The GitHub collector must be the only writer to this Sheet.
3. First dispatch the prepared branch with `check_only=true` to verify the existing authorization without collecting log rows or advancing the sync cursor (a rotated credential, if returned, is saved). Then merge the replacement into `main`, enable the workflow if it was disabled, and use **Actions → Spotify Logger Sync → Run workflow** with `check_only=false`. Check for a successful collection step and a fresh `__app_state.last_sync_at`.
4. Run again immediately; overlapping Spotify events must not add duplicate timestamps. Then test cron-job.org manually and verify the corresponding Actions run actually finishes successfully.
5. Enable the external schedule. Leave GitHub's native schedule removed. If setup fails, keep the external job disabled and restore the previous working trigger while resolving the configuration.

The local changes do not unpublish the old Sites app or change its runtime settings. Retire it separately after collection succeeds in Actions. Do not run that old writer alongside the new collector.

## Guarantees and monitoring

Each serialized run reads all `log` timestamps, normalizes ISO offsets, and filters the fetched batch by timestamp. Genuine repeat listens at different timestamps are kept. New rows preserve seconds/milliseconds. Legacy minute-only rows conservatively occupy their minute. Existing historical duplicates are not deleted.

New rows are written to an explicit range after the last existing row. There is no blind append retry. If a write succeeded but its response was lost, the next run rereads the committed history. This is a single-writer design: GitHub concurrency does not coordinate with another repository, an old hosted worker or manual edits. Do not physically sort/delete/insert rows or write to `log` during collection; use filter views.

A successful dispatch response only means GitHub accepted the command, not that collection succeeded. Enable GitHub Actions failure notifications for this repository and inspect `last_sync_at` for actual freshness. cron-job.org failure notifications cover dispatch failures (expired token, disabled workflow, etc.), not a failing runner. A GitHub workflow can still be explicitly disabled, and runners can be delayed/unavailable; external dispatch does not guarantee continuous collection. Queued pending runs may be coalesced by GitHub concurrency.

The API exposes a limited recent listening history; a prolonged outage can leave gaps. Private-repository Actions minute quotas also matter at five-minute frequency.

References: [GitHub workflow dispatch](https://docs.github.com/en/rest/actions/workflows#create-a-workflow-dispatch-event), [workflow concurrency](https://docs.github.com/en/actions/how-tos/write-workflows/choose-when-workflows-run/control-workflow-concurrency), [cron-job.org FAQ](https://cron-job.org/en/faq/).
