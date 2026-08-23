import assert from "node:assert/strict";
import test from "node:test";
import { clusterUniverse, layoutUniverse } from "../lib/universe-layout.ts";

function graph() {
  const ids = ["a", "b", "c", "x", "y", "z"];
  return {
    nodes: ids.map((id, index) => ({ id, name: id.toUpperCase(), coverUrl: "", plays: 20 - index, minutes: 60 - index * 3, connections: 0 })),
    edges: [
      { source: "a", target: "b", weight: 12 },
      { source: "b", target: "c", weight: 11 },
      { source: "a", target: "c", weight: 10 },
      { source: "x", target: "y", weight: 13 },
      { source: "y", target: "z", weight: 12 },
      { source: "x", target: "z", weight: 11 },
      { source: "c", target: "x", weight: 2 },
    ],
  };
}

test("galaxy communities keep strongly connected artists together", () => {
  const communities = clusterUniverse(graph());
  assert.equal(communities.get("a"), communities.get("b"));
  assert.equal(communities.get("b"), communities.get("c"));
  assert.equal(communities.get("x"), communities.get("y"));
  assert.equal(communities.get("y"), communities.get("z"));
  assert.notEqual(communities.get("a"), communities.get("x"));
});

test("galaxy layout is deterministic and separates communities", () => {
  const first = layoutUniverse(graph());
  const second = layoutUniverse(graph());
  assert.deepEqual(first.map(({ id, x, y, community }) => [id, x, y, community]), second.map(({ id, x, y, community }) => [id, x, y, community]));
  const byId = new Map(first.map((point) => [point.id, point]));
  const distance = (a, b) => Math.hypot(byId.get(a).x - byId.get(b).x, byId.get(a).y - byId.get(b).y);
  const within = (distance("a", "b") + distance("b", "c") + distance("x", "y") + distance("y", "z")) / 4;
  assert.ok(distance("c", "x") > within, `expected the weak cross-community link to be longer than ${within}`);
});
