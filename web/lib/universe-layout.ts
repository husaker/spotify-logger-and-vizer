import type { UniverseGraph, UniverseNode } from "./analytics";

export type UniversePoint = UniverseNode & { x: number; y: number; radius: number; rank: number; community: number };

export const UNIVERSE_WIDTH = 2800;
export const UNIVERSE_HEIGHT = 1400;
export const UNIVERSE_VIEW = { x: 0, y: 0, width: UNIVERSE_WIDTH, height: UNIVERSE_HEIGHT };

type WeightedEdge = { source: string; target: string; raw: number; affinity: number };

function hashNumber(value: string): number {
  let hash = 0;
  for (let index = 0; index < value.length; index++) hash = ((hash << 5) - hash + value.charCodeAt(index)) | 0;
  return Math.abs(hash);
}

function positionEdges(graph: UniverseGraph): WeightedEdge[] {
  const totals = new Map<string, number>();
  for (const edge of graph.edges) {
    totals.set(edge.source, (totals.get(edge.source) ?? 0) + edge.weight);
    totals.set(edge.target, (totals.get(edge.target) ?? 0) + edge.weight);
  }
  return graph.edges.map((edge) => {
    const normalized = edge.weight / Math.sqrt((totals.get(edge.source) ?? 1) * (totals.get(edge.target) ?? 1));
    return { ...edge, raw: edge.weight, affinity: normalized * edge.weight / (edge.weight + 2) };
  });
}

export function clusterUniverse(graph: UniverseGraph): Map<string, number> {
  const edges = positionEdges(graph);
  const indexById = new Map(graph.nodes.map((node, index) => [node.id, index]));
  const adjacency = graph.nodes.map(() => [] as Array<{ neighbour: number; weight: number }>);
  for (const edge of edges) {
    const source = indexById.get(edge.source);
    const target = indexById.get(edge.target);
    if (source === undefined || target === undefined) continue;
    adjacency[source].push({ neighbour: target, weight: edge.affinity });
    adjacency[target].push({ neighbour: source, weight: edge.affinity });
  }
  const strengths = adjacency.map((items) => items.reduce((sum, item) => sum + item.weight, 0));
  const totalStrength = Math.max(Number.EPSILON, strengths.reduce((sum, value) => sum + value, 0));
  const communities = graph.nodes.map((_, index) => index);
  const communityTotals = new Map(communities.map((community, index) => [community, strengths[index]]));
  const order = graph.nodes.map((_, index) => index).sort((a, b) => strengths[b] - strengths[a] || graph.nodes[a].id.localeCompare(graph.nodes[b].id));
  const resolution = 1.08;

  for (let pass = 0; pass < 24; pass++) {
    let moved = false;
    for (const index of order) {
      const current = communities[index];
      const links = new Map<number, number>();
      for (const edge of adjacency[index]) {
        const community = communities[edge.neighbour];
        links.set(community, (links.get(community) ?? 0) + edge.weight);
      }
      communityTotals.set(current, (communityTotals.get(current) ?? 0) - strengths[index]);
      let best = current;
      let bestScore = (links.get(current) ?? 0) - resolution * strengths[index] * (communityTotals.get(current) ?? 0) / totalStrength;
      for (const [community, weight] of [...links.entries()].sort((a, b) => a[0] - b[0])) {
        const score = weight - resolution * strengths[index] * (communityTotals.get(community) ?? 0) / totalStrength;
        if (score > bestScore + 1e-10 || (Math.abs(score - bestScore) <= 1e-10 && community < best)) {
          best = community;
          bestScore = score;
        }
      }
      communities[index] = best;
      communityTotals.set(best, (communityTotals.get(best) ?? 0) + strengths[index]);
      if (best !== current) moved = true;
    }
    if (!moved) break;
  }

  const members = new Map<number, number[]>();
  communities.forEach((community, index) => members.set(community, [...(members.get(community) ?? []), index]));
  const ordered = [...members.entries()].sort(([, a], [, b]) => {
    const minutesA = a.reduce((sum, index) => sum + graph.nodes[index].minutes, 0);
    const minutesB = b.reduce((sum, index) => sum + graph.nodes[index].minutes, 0);
    return minutesB - minutesA || graph.nodes[a[0]].id.localeCompare(graph.nodes[b[0]].id);
  });
  const remap = new Map(ordered.map(([community], index) => [community, index]));
  return new Map(graph.nodes.map((node, index) => [node.id, remap.get(communities[index]) ?? index]));
}

export function layoutUniverse(graph: UniverseGraph): UniversePoint[] {
  const maxMinutes = Math.max(1, ...graph.nodes.map((node) => node.minutes));
  const communities = clusterUniverse(graph);
  const points: UniversePoint[] = graph.nodes.map((node, rank) => ({
    ...node,
    rank,
    community: communities.get(node.id) ?? rank,
    x: UNIVERSE_WIDTH / 2,
    y: UNIVERSE_HEIGHT / 2,
    radius: 16 + Math.sqrt(node.minutes / maxMinutes) * 48,
  }));
  const pointIndex = new Map(points.map((point, index) => [point.id, index]));
  const edges = positionEdges(graph);
  const maxAffinity = Math.max(Number.EPSILON, ...edges.map((edge) => edge.affinity));

  const grouped = new Map<number, UniversePoint[]>();
  for (const point of points) grouped.set(point.community, [...(grouped.get(point.community) ?? []), point]);
  const clusters = [...grouped.entries()].map(([id, members]) => ({
    id,
    members,
    minutes: members.reduce((sum, point) => sum + point.minutes, 0),
    radius: Math.min(400, 110 + Math.sqrt(members.length) * 43),
    x: UNIVERSE_WIDTH / 2,
    y: UNIVERSE_HEIGHT / 2,
  })).sort((a, b) => b.minutes - a.minutes || a.id - b.id);
  const clusterIndex = new Map(clusters.map((cluster, index) => [cluster.id, index]));
  const goldenAngle = Math.PI * (3 - Math.sqrt(5));
  clusters.forEach((cluster, index) => {
    const angle = index * goldenAngle + (hashNumber(`cluster:${cluster.id}`) % 360) * Math.PI / 180;
    const distance = index === 0 ? 0 : 220 + Math.sqrt(index) * 230;
    cluster.x = UNIVERSE_WIDTH / 2 + Math.cos(angle) * distance;
    cluster.y = UNIVERSE_HEIGHT / 2 + Math.sin(angle) * distance * .52;
  });
  const clusterLinks = new Map<string, number>();
  for (const edge of edges) {
    const source = points[pointIndex.get(edge.source) ?? -1];
    const target = points[pointIndex.get(edge.target) ?? -1];
    if (!source || !target || source.community === target.community) continue;
    const [a, b] = source.community < target.community ? [source.community, target.community] : [target.community, source.community];
    const key = `${a}:${b}`;
    clusterLinks.set(key, (clusterLinks.get(key) ?? 0) + edge.affinity);
  }
  const maxClusterLink = Math.max(Number.EPSILON, ...clusterLinks.values());

  for (let iteration = 0; iteration < 140; iteration++) {
    const forces = clusters.map(() => ({ x: 0, y: 0 }));
    for (let a = 0; a < clusters.length; a++) {
      for (let b = a + 1; b < clusters.length; b++) {
        const dx = clusters[b].x - clusters[a].x || .01;
        const dy = clusters[b].y - clusters[a].y || .01;
        const distance = Math.max(1, Math.hypot(dx, dy));
        const minimum = clusters[a].radius + clusters[b].radius + 130;
        const strength = distance < minimum ? (minimum - distance) * .035 : 34_000 / (distance * distance);
        const fx = dx / distance * strength;
        const fy = dy / distance * strength;
        forces[a].x -= fx; forces[a].y -= fy; forces[b].x += fx; forces[b].y += fy;
      }
    }
    for (const [key, weight] of clusterLinks) {
      const [sourceId, targetId] = key.split(":").map(Number);
      const sourceIndex = clusterIndex.get(sourceId);
      const targetIndex = clusterIndex.get(targetId);
      if (sourceIndex === undefined || targetIndex === undefined) continue;
      const source = clusters[sourceIndex];
      const target = clusters[targetIndex];
      const dx = target.x - source.x || .01;
      const dy = target.y - source.y || .01;
      const distance = Math.max(1, Math.hypot(dx, dy));
      const normalized = weight / maxClusterLink;
      const desired = source.radius + target.radius + 160 + (1 - normalized) * 220;
      const strength = (distance - desired) * .006 * (.65 + normalized);
      const fx = dx / distance * strength;
      const fy = dy / distance * strength;
      forces[sourceIndex].x += fx; forces[sourceIndex].y += fy; forces[targetIndex].x -= fx; forces[targetIndex].y -= fy;
    }
    const cooling = .8 - iteration / 140 * .55;
    clusters.forEach((cluster, index) => {
      forces[index].x += (UNIVERSE_WIDTH / 2 - cluster.x) * .0007;
      forces[index].y += (UNIVERSE_HEIGHT / 2 - cluster.y) * .001;
      cluster.x = Math.max(cluster.radius + 70, Math.min(UNIVERSE_WIDTH - cluster.radius - 70, cluster.x + forces[index].x * cooling));
      cluster.y = Math.max(cluster.radius + 60, Math.min(UNIVERSE_HEIGHT - cluster.radius - 60, cluster.y + forces[index].y * cooling));
    });
  }

  for (const cluster of clusters) {
    cluster.members.sort((a, b) => a.rank - b.rank).forEach((point, index) => {
      const angle = index * goldenAngle + (hashNumber(`${point.id}:angle`) % 180) * Math.PI / 180;
      const distance = index === 0 ? 0 : Math.min(cluster.radius * .72, 52 + Math.sqrt(index) * 61);
      point.x = cluster.x + Math.cos(angle) * distance;
      point.y = cluster.y + Math.sin(angle) * distance;
    });
  }

  const iterations = Math.max(90, Math.min(190, Math.round(5200 / Math.max(1, points.length))));
  for (let iteration = 0; iteration < iterations; iteration++) {
    const forces = points.map(() => ({ x: 0, y: 0 }));
    for (let a = 0; a < points.length; a++) {
      for (let b = a + 1; b < points.length; b++) {
        const dx = points[b].x - points[a].x || .01;
        const dy = points[b].y - points[a].y || .01;
        const distance = Math.max(1, Math.hypot(dx, dy));
        const sameCommunity = points[a].community === points[b].community;
        const minimum = points[a].radius + points[b].radius + (sameCommunity ? 34 : 100);
        const strength = distance < minimum ? (minimum - distance) * .11 : (sameCommunity ? 7_500 : 18_000) / (distance * distance);
        const fx = dx / distance * strength;
        const fy = dy / distance * strength;
        forces[a].x -= fx; forces[a].y -= fy; forces[b].x += fx; forces[b].y += fy;
      }
    }
    for (const edge of edges) {
      const sourceIndex = pointIndex.get(edge.source);
      const targetIndex = pointIndex.get(edge.target);
      if (sourceIndex === undefined || targetIndex === undefined) continue;
      const source = points[sourceIndex];
      const target = points[targetIndex];
      const dx = target.x - source.x || .01;
      const dy = target.y - source.y || .01;
      const distance = Math.max(1, Math.hypot(dx, dy));
      const normalized = edge.affinity / maxAffinity;
      const sameCommunity = source.community === target.community;
      const desired = source.radius + target.radius + (sameCommunity ? 65 + (1 - normalized) * 125 : 230 + (1 - normalized) * 170);
      const strength = (distance - desired) * (sameCommunity ? .011 : .0035) * (.75 + normalized * 1.5);
      const fx = dx / distance * strength;
      const fy = dy / distance * strength;
      forces[sourceIndex].x += fx; forces[sourceIndex].y += fy; forces[targetIndex].x -= fx; forces[targetIndex].y -= fy;
    }
    const cooling = .9 - iteration / iterations * .64;
    points.forEach((point, index) => {
      const cluster = clusters[clusterIndex.get(point.community) ?? 0];
      forces[index].x += (cluster.x - point.x) * .007;
      forces[index].y += (cluster.y - point.y) * .007;
      point.x = Math.max(point.radius + 70, Math.min(UNIVERSE_WIDTH - point.radius - 70, point.x + forces[index].x * cooling));
      point.y = Math.max(point.radius + 64, Math.min(UNIVERSE_HEIGHT - point.radius - 64, point.y + forces[index].y * cooling));
    });
  }
  return points;
}
