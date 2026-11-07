import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import test from "node:test";

function cli(extra = {}) {
  return spawnSync(process.execPath, ["--import", "tsx", "scripts/run-sync.mts"], {
    cwd: new URL("../", import.meta.url), encoding: "utf8",
    env: { PATH: process.env.PATH, ...extra }, timeout: 10_000,
  });
}

test("CLI rejects collection outside the serialized workflow before reading any credentials", () => {
  const result = cli();
  assert.equal(result.status, 1);
  assert.match(result.stderr, /serializes writes/);
});

test("CLI reports missing secret names without exposing provided secret values", () => {
  const result = cli({ GITHUB_ACTIONS: "true", GITHUB_WORKFLOW: "Spotify Logger Sync", SPOTIFY_CLIENT_SECRET: "secret-value-must-not-be-printed" });
  assert.equal(result.status, 1);
  assert.match(result.stderr, /Missing repository secrets:.*GOOGLE_SERVICE_ACCOUNT_JSON/);
  assert.doesNotMatch(result.stderr + result.stdout, /secret-value-must-not-be-printed/);
});
