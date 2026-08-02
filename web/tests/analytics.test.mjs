import assert from "node:assert/strict";
import test from "node:test";
import { activity, activityStats, delta, discoverySeries, enrich, fingerprint, inRange, metrics, previousRange, rangeForPreset, weeklySeries } from "../lib/analytics.ts";

const payload = {
  configured: true, generatedAt: "2026-08-02T00:00:00Z", timezone: "Europe/Moscow", stale: false,
  lastSyncAt: null, reauthorizationRequired: false,
  plays: [
    { playedAt: "2026-08-01T21:30:00Z", trackId: "a", trackName: "A", artistName: "One", trackUrl: "" },
    { playedAt: "2026-08-02T20:00:00Z", trackId: "a", trackName: "A", artistName: "One", trackUrl: "" },
    { playedAt: "2026-08-02T21:00:00Z", trackId: "b", trackName: "B", artistName: "Two", trackUrl: "" },
  ],
  tracks: {
    a: { id: "a", name: "A", durationMs: 180000, albumId: "x", coverUrl: "", primaryArtistId: "one" },
    b: { id: "b", name: "B", durationMs: 240000, albumId: "y", coverUrl: "", primaryArtistId: "two" },
  },
  artists: { one: { id:"one",name:"One",coverUrl:"",genres:["rock"],primaryGenre:"rock" }, two: { id:"two",name:"Two",coverUrl:"",genres:["pop"],primaryGenre:"pop" } },
  albums: { x:{id:"x",name:"X",coverUrl:"",releaseDate:""}, y:{id:"y",name:"Y",coverUrl:"",releaseDate:""} },
};

test("Moscow day boundaries drive filtering and metrics", () => {
  const plays = enrich(payload);
  const selected = inRange(plays, { from: "2026-08-02", to: "2026-08-02" });
  assert.equal(selected.length, 2);
  assert.deepEqual(metrics(selected), { plays: 2, tracks: 1, artists: 1, minutes: 6, activeDays: 1 });
});

test("rolling year and previous period are inclusive", () => {
  assert.deepEqual(rangeForPreset("365", [], "2026-08-02"), { from: "2025-08-03", to: "2026-08-02" });
  assert.deepEqual(previousRange({ from: "2026-08-01", to: "2026-08-02" }), { from: "2026-07-30", to: "2026-07-31" });
  assert.equal(delta(120, 100), 20);
  assert.equal(delta(1, 0), null);
});

test("activity and fingerprint always produce stable grids", () => {
  const plays = enrich(payload);
  const days = activity(plays, "2026-08-03");
  assert.equal(days.length, 365);
  assert.deepEqual(activityStats(days), { plays: 3, activeDays: 2, longestStreak: 2, currentStreak: 2, busiestDay: "2026-08-02", busiestPlays: 2 });
  const grid = fingerprint(plays, "plays");
  assert.equal(grid.length, 7);
  assert.equal(grid.flat().length, 168);
  assert.equal(grid.flat().reduce((sum, value) => sum + value, 0), 3);
});

test("time series covers the complete selected range including empty buckets", () => {
  const plays = enrich(payload);
  const range = { from: "2026-08-01", to: "2026-08-31" };
  const weekly = weeklySeries(plays, range);
  assert.deepEqual(weekly.map((item) => item.label), ["2026-07-27", "2026-08-03", "2026-08-10", "2026-08-17", "2026-08-24", "2026-08-31"]);
  assert.deepEqual(weekly.map((item) => item.value), [2, 1, 0, 0, 0, 0]);

  const monthly = weeklySeries(plays, { from: "2026-07-01", to: "2026-09-30" }, true);
  assert.deepEqual(monthly.map((item) => [item.label, item.value]), [["2026-07", 0], ["2026-08", 3], ["2026-09", 0]]);

  const discovery = discoverySeries(plays, plays, range);
  assert.equal(discovery.length, weekly.length);
  assert.equal(discovery.at(-1)?.fresh, 0);
  assert.equal(discovery.at(-1)?.replay, 0);
});
