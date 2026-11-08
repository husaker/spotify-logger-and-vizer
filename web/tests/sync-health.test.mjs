import assert from "node:assert/strict";
import test from "node:test";
import { syncHealth } from "../lib/server/sync-health.ts";

const now = Date.parse("2026-11-08T06:00:00Z");
test("fresh successful sync is healthy even when no music was played", () => {
  assert.equal(syncHealth({ last_sync_at: new Date(now - 5 * 60_000).toISOString() }, now).healthy, true);
});
test("missing, invalid, future and stale success timestamps fail closed", () => {
  for (const timestamp of ["", "invalid", new Date(now + 120_000).toISOString(), new Date(now - 30 * 60_000).toISOString()]) {
    assert.equal(syncHealth({ last_sync_at: timestamp }, now).healthy, false);
  }
  assert.equal(syncHealth({ last_sync_at: new Date(now - 30 * 60_000 + 1).toISOString() }, now).healthy, true);
});
test("reauthorization alerts even with a recent success", () => {
  assert.equal(syncHealth({ last_sync_at: new Date(now).toISOString(), reauth_required: "true" }, now).healthy, false);
});
