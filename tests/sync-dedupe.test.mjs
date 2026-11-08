import assert from "node:assert/strict";
import test from "node:test";
import { createTimestampGuard } from "../src/sync-dedupe.ts";

test("a retry after log append succeeds but dedupe append fails cannot append that event again", () => {
  const claimed = createTimestampGuard([["2026-07-03T15:29:12.345Z"]], []);
  assert.equal(claimed("2026-07-03T15:29:12.345Z"), null);
  assert.equal(claimed("2026-07-03T18:29:12.345+03:00"), null);
});

test("legacy log minutes prevent reimporting events from that stored minute", () => {
  const claimed = createTimestampGuard([["July 3, 2026 at 6:29PM"]], []);
  assert.equal(claimed("2026-07-03T15:29:12.345Z"), null);
  assert.equal(claimed("2026-07-03T15:30:12.345Z"), "2026-07-03T15:30:12.345Z");
});

test("new events preserve seconds and milliseconds while equal timestamps in one batch count once", () => {
  const claimed = createTimestampGuard([], []);
  assert.equal(claimed("2026-07-03T15:29:12.345Z"), "2026-07-03T15:29:12.345Z");
  assert.equal(claimed("2026-07-03T15:29:12.345Z"), null);
  assert.equal(claimed("2026-07-03T15:29:12.346Z"), "2026-07-03T15:29:12.346Z");
  assert.equal(claimed("2026-07-03T15:29:42.345Z"), "2026-07-03T15:29:42.345Z");
});

test("existing exact log timestamps do not discard other events within the same minute", () => {
  const claimed = createTimestampGuard([["2026-07-03T15:29:12.345Z"]], []);
  assert.equal(claimed("2026-07-03T15:29:42.345Z"), "2026-07-03T15:29:42.345Z");
});

test("the legacy dedupe ledger is accepted even when timestamp ISO formatting differs", () => {
  const claimed = createTimestampGuard([], [["2026-07-03T15:29:00Z|track-one"]]);
  assert.equal(claimed("2026-07-03T15:29:00.000Z"), null);
});

test("malformed log rows and events do not introduce timestamps", () => {
  const claimed = createTimestampGuard([["invalid"], []], [["invalid|track"]]);
  assert.equal(claimed("invalid"), null);
  assert.equal(claimed("2026-07-03T15:29:00Z"), "2026-07-03T15:29:00.000Z");
});
