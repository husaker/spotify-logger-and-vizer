# Spotify Logger

A Spotify listening-history collector that runs in GitHub Actions and writes to Google Sheets. cron-job.org triggers the workflow through GitHub's API every five minutes. Collection requires neither a VPS nor OpenAI Sites.

See [setup and migration instructions](web/CRON_SETUP.md) for repository secrets, the external trigger, local Spotify authorization and safe cutover from the previous hosted collector.

## Repository layout

- `.github/workflows/sync.yml` — serialized, externally dispatched collection job
- `web/scripts/run-sync.mts` — standalone Node collector entrypoint
- `web/scripts/authorize-spotify.mts` — local OAuth connection and reauthorization
- `web/lib/server/` — Spotify, Google Sheets, timestamp deduplication and notifications
- `web/tests/` — collector, analytics and web tests
- `web/app/`, `web/worker/` — legacy web dashboard/admin, not required by the collector

The personal portfolio is maintained in its separate repository. This repository retains the legacy web source; no hosting is required to execute the collector.

## Validate locally

```sh
cd web
npm ci
npm run test:logger
```

`npm test` also builds and checks the legacy web app. The collector itself runs from the Actions workflow to preserve its single-writer concurrency guarantee. The new workflow and secrets must be released/configured before cron-job.org is enabled; local changes alone do not alter the running logger.
