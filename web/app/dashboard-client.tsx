"use client";

/* Dynamic Spotify CDN artwork is intentionally rendered directly. */
/* eslint-disable @next/next/no-img-element */

import { useEffect, useMemo, useRef, useState } from "react";
import { createPortal } from "react-dom";
import {
  activity, activityStats, albumMosaic, delta, discoverySeries, enrich, fingerprint, inRange, listeningUniverse,
  metrics, mosaicPeriodOptions, previousRange, rangeForPreset, rankings, todayMoscow, weeklySeries,
  type DateRange, type MosaicAlbum, type MosaicPeriod, type MosaicPeriodOption, type RangePreset,
  type RankedItem, type UniverseGraph, type UniverseNode,
} from "../lib/analytics";
import type { DashboardPayload } from "../lib/types";

const presets: Array<{ id: RangePreset; label: string }> = [
  { id: "365", label: "Last 365 days" }, { id: "year", label: "This year" },
  { id: "90", label: "90 days" }, { id: "30", label: "30 days" },
  { id: "7", label: "7 days" }, { id: "all", label: "All time" }, { id: "custom", label: "Custom" },
];

type TooltipState = { text: string; x: number; y: number } | null;

function useInstantTooltip() {
  const [tooltip, setTooltip] = useState<TooltipState>(null);
  function show(event: React.PointerEvent<Element>, text: string) {
    if (event.pointerType === "touch") return;
    const rect = event.currentTarget.getBoundingClientRect();
    const x = Math.max(110, Math.min(window.innerWidth - 110, rect.left + rect.width / 2));
    setTooltip({ text, x, y: Math.max(42, rect.top - 8) });
  }
  function hide() { setTooltip(null); }
  return { tooltip, show, hide };
}

function InstantTooltip({ tooltip }: { tooltip: TooltipState }) {
  if (!tooltip || typeof document === "undefined") return null;
  return createPortal(<div className="instant-tooltip" role="tooltip" style={{ left: tooltip.x, top: tooltip.y }}>{tooltip.text}</div>, document.body);
}

function timeAgo(value: string | null): string {
  if (!value) return "Waiting for first sync";
  const minutes = Math.max(0, Math.round((Date.now() - Date.parse(value)) / 60_000));
  if (minutes < 2) return "Updated just now";
  if (minutes < 60) return `Updated ${minutes} minutes ago`;
  const hours = Math.round(minutes / 60);
  return `Updated ${hours} hour${hours === 1 ? "" : "s"} ago`;
}

function Delta({ current, previous }: { current: number; previous: number }) {
  const value = delta(current, previous);
  if (value === null) return <span className="metric-delta neutral">New</span>;
  const direction = value > 0 ? "up" : value < 0 ? "down" : "neutral";
  return <span className={`metric-delta ${direction}`}>{value > 0 ? "+" : ""}{Math.round(value)}%</span>;
}

function MetricCard({ label, value, previous, suffix }: { label: string; value: number; previous: number; suffix?: string }) {
  return <article className="metric-card">
    <div className="metric-top"><span>{label}</span><Delta current={value} previous={previous} /></div>
    <strong>{value.toLocaleString()}{suffix ?? ""}</strong>
    <small>vs previous period</small>
  </article>;
}

function ActivityGrid({ data }: { data: ReturnType<typeof activity> }) {
  const { tooltip, show, hide } = useInstantTooltip();
  const first = new Date(`${data[0]?.day ?? todayMoscow()}T00:00:00Z`);
  const padding = ((first.getUTCDay() + 6) % 7);
  const weeks = Math.ceil((padding + data.length) / 7);
  const stats = activityStats(data);
  const monthLabels = data.reduce<Array<{ label: string; column: number }>>((labels, item, index) => {
    if (index && item.day.slice(0, 7) === data[index - 1].day.slice(0, 7)) return labels;
    const date = new Date(`${item.day}T00:00:00Z`);
    const label = new Intl.DateTimeFormat("en", { month: "short", timeZone: "UTC" }).format(date);
    const column = Math.floor((padding + index) / 7) + 1;
    if (labels.at(-1)?.column === column) labels[labels.length - 1] = { label, column };
    else labels.push({ label, column });
    return labels;
  }, []);
  const busiestDate = new Intl.DateTimeFormat("en", { month: "short", day: "numeric", timeZone: "UTC" }).format(new Date(`${stats.busiestDay}T00:00:00Z`));
  return <section className="panel activity-panel">
    <div className="section-heading activity-heading"><div><span className="eyebrow">Consistency</span><h2>{stats.plays.toLocaleString()} plays in the last year</h2></div><div className="activity-insights"><span><b>{stats.activeDays}</b> active days</span><span><b>{stats.longestStreak}</b> day longest streak</span><span><b>{stats.currentStreak}</b> day current streak</span></div></div>
    <div className="activity-scroll"><div className="activity-chart" style={{ "--weeks": weeks } as React.CSSProperties}>
      <div className="activity-months">{monthLabels.map((month) => <span key={`${month.label}-${month.column}`} style={{ gridColumn: month.column }}>{month.label}</span>)}</div>
      <div className="activity-weekdays"><span style={{ gridRow: 1 }}>Mon</span><span style={{ gridRow: 3 }}>Wed</span><span style={{ gridRow: 5 }}>Fri</span></div>
      <div className="activity-grid">
        {Array.from({ length: padding }, (_, index) => <i key={`pad-${index}`} className="activity-empty" />)}
        {data.map((item) => { const text = `${item.day}: ${item.plays} plays`; return <span key={item.day} className={`activity-cell level-${item.level}`} aria-label={text} onPointerEnter={(event) => show(event, text)} onPointerLeave={hide} onPointerCancel={hide} />; })}
      </div>
    </div></div>
    <div className="activity-footer"><span>Busiest day: <b>{busiestDate}</b> · {stats.busiestPlays.toLocaleString()} plays</span><div className="activity-legend"><span>Less</span>{[0,1,2,3,4].map((level) => <i key={level} className={`level-${level}`} />)}<span>More</span></div></div>
    <InstantTooltip tooltip={tooltip} />
  </section>;
}

function Rankings({ items, title }: { items: RankedItem[]; title: string }) {
  return <div className="ranking-grid">
    {items.map((item, index) => <article className="ranking-card" key={item.id}>
      <div className="rank-number">{String(index + 1).padStart(2, "0")}</div>
      {item.coverUrl ? <img src={item.coverUrl} alt="" /> : <div className="cover-fallback">♪</div>}
      <h3>{item.name || "Unknown"}</h3><p>{item.subtitle}</p>
      <div className="rank-stats"><span><b>{item.plays.toLocaleString()}</b> plays</span><span><b>{Math.round(item.minutes).toLocaleString()}</b> min</span></div>
    </article>)}
    {!items.length && <div className="empty-state">No {title.toLowerCase()} in this period.</div>}
  </div>;
}

type UniversePoint = UniverseNode & { x: number; y: number; radius: number; rank: number };

const UNIVERSE_WIDTH = 2800;
const UNIVERSE_HEIGHT = 1400;
const UNIVERSE_VIEW = { x: 0, y: 0, width: UNIVERSE_WIDTH, height: UNIVERSE_HEIGHT };

function hashNumber(value: string): number {
  let hash = 0;
  for (let index = 0; index < value.length; index++) hash = ((hash << 5) - hash + value.charCodeAt(index)) | 0;
  return Math.abs(hash);
}

function layoutUniverse(graph: UniverseGraph): UniversePoint[] {
  const maxMinutes = Math.max(1, ...graph.nodes.map((node) => node.minutes));
  const points: UniversePoint[] = graph.nodes.map((node, index) => {
    const xRatio = (hashNumber(`${node.id}:x`) % 10_000) / 10_000;
    const yRatio = (hashNumber(`${node.id}:y`) % 10_000) / 10_000;
    return {
      ...node,
      rank: index,
      x: 150 + xRatio * (UNIVERSE_WIDTH - 300),
      y: 130 + yRatio * (UNIVERSE_HEIGHT - 260),
      radius: 16 + Math.sqrt(node.minutes / maxMinutes) * 48,
    };
  });
  const pointIndex = new Map(points.map((point, index) => [point.id, index]));
  const maxEdge = Math.max(1, ...graph.edges.map((edge) => edge.weight));

  const iterations = Math.max(60, Math.min(180, Math.round(3600 / Math.max(1, points.length))));
  for (let iteration = 0; iteration < iterations; iteration++) {
    const forces = points.map(() => ({ x: 0, y: 0 }));
    for (let a = 0; a < points.length; a++) {
      for (let b = a + 1; b < points.length; b++) {
        const dx = points[b].x - points[a].x || .01;
        const dy = points[b].y - points[a].y || .01;
        const distance = Math.max(1, Math.hypot(dx, dy));
        const minimum = points[a].radius + points[b].radius + 44;
        const strength = distance < minimum ? (minimum - distance) * .08 : 9000 / (distance * distance);
        const fx = dx / distance * strength;
        const fy = dy / distance * strength;
        forces[a].x -= fx; forces[a].y -= fy; forces[b].x += fx; forces[b].y += fy;
      }
    }
    for (const edge of graph.edges) {
      const sourceIndex = pointIndex.get(edge.source);
      const targetIndex = pointIndex.get(edge.target);
      if (sourceIndex === undefined || targetIndex === undefined) continue;
      const source = points[sourceIndex];
      const target = points[targetIndex];
      const dx = target.x - source.x || .01;
      const dy = target.y - source.y || .01;
      const distance = Math.max(1, Math.hypot(dx, dy));
      const desired = source.radius + target.radius + 150 + (1 - edge.weight / maxEdge) * 90;
      const strength = (distance - desired) * .0045 * (.7 + edge.weight / maxEdge);
      const fx = dx / distance * strength;
      const fy = dy / distance * strength;
      forces[sourceIndex].x += fx; forces[sourceIndex].y += fy; forces[targetIndex].x -= fx; forces[targetIndex].y -= fy;
    }
    const cooling = .8 - iteration / iterations * .62;
    points.forEach((point, index) => {
      forces[index].x += (UNIVERSE_WIDTH / 2 - point.x) * .0009;
      forces[index].y += (UNIVERSE_HEIGHT / 2 - point.y) * .0012;
      point.x = Math.max(point.radius + 70, Math.min(UNIVERSE_WIDTH - point.radius - 70, point.x + forces[index].x * cooling));
      point.y = Math.max(point.radius + 64, Math.min(UNIVERSE_HEIGHT - point.radius - 64, point.y + forces[index].y * cooling));
    });
  }
  return points;
}

function ListeningUniverse({ graph }: { graph: UniverseGraph }) {
  const { tooltip, show, hide } = useInstantTooltip();
  const [selectedId, setSelectedId] = useState(graph.nodes[0]?.id ?? "");
  const [view, setView] = useState(UNIVERSE_VIEW);
  const [isPanning, setIsPanning] = useState(false);
  const panStart = useRef<{ pointerId: number; clientX: number; clientY: number; view: typeof view } | null>(null);
  const points = useMemo(() => layoutUniverse(graph), [graph]);
  const pointMap = new Map(points.map((point) => [point.id, point]));
  const selected = pointMap.get(selectedId) ?? points[0];
  const maxEdge = Math.max(1, ...graph.edges.map((edge) => edge.weight));
  const selectedEdges = graph.edges.filter((edge) => edge.source === selected?.id || edge.target === selected?.id);
  const neighbourIds = new Set(selectedEdges.flatMap((edge) => [edge.source, edge.target]));
  const strongestNeighbours = selectedEdges.map((edge) => ({ id: edge.source === selected?.id ? edge.target : edge.source, weight: edge.weight })).sort((a, b) => b.weight - a.weight).slice(0, 3);
  const zoom = UNIVERSE_WIDTH / view.width;
  const visibleLimit = zoom < 1.35 ? 12 : zoom < 2 ? 32 : zoom < 3 ? 80 : zoom < 4.5 ? 150 : points.length;
  const labelLimit = zoom < 1.35 ? 8 : zoom < 2 ? 18 : zoom < 3 ? 45 : zoom < 4.5 ? 90 : points.length;
  const visibleIds = new Set(points.slice(0, visibleLimit).map((point) => point.id));
  if (selected?.id) visibleIds.add(selected.id);
  const visiblePoints = points.filter((point) => visibleIds.has(point.id));
  function updateZoom(factor: number, anchorX = .5, anchorY = .5) {
    setView((current) => {
      const nextZoom = Math.max(1, Math.min(8, UNIVERSE_WIDTH / current.width * factor));
      const width = UNIVERSE_WIDTH / nextZoom;
      const height = UNIVERSE_HEIGHT / nextZoom;
      const pointX = current.x + current.width * anchorX;
      const pointY = current.y + current.height * anchorY;
      return {
        x: Math.max(0, Math.min(UNIVERSE_WIDTH - width, pointX - width * anchorX)),
        y: Math.max(0, Math.min(UNIVERSE_HEIGHT - height, pointY - height * anchorY)),
        width,
        height,
      };
    });
  }
  function startPan(event: React.PointerEvent<SVGSVGElement>) {
    if (event.button !== 0 || (event.target as Element).closest(".universe-node")) return;
    panStart.current = { pointerId: event.pointerId, clientX: event.clientX, clientY: event.clientY, view };
    event.currentTarget.setPointerCapture(event.pointerId);
    setIsPanning(true);
    hide();
  }
  function movePan(event: React.PointerEvent<SVGSVGElement>) {
    const start = panStart.current;
    if (!start || start.pointerId !== event.pointerId) return;
    const rect = event.currentTarget.getBoundingClientRect();
    const x = start.view.x - (event.clientX - start.clientX) / rect.width * start.view.width;
    const y = start.view.y - (event.clientY - start.clientY) / rect.height * start.view.height;
    setView({ ...start.view, x: Math.max(0, Math.min(UNIVERSE_WIDTH - start.view.width, x)), y: Math.max(0, Math.min(UNIVERSE_HEIGHT - start.view.height, y)) });
  }
  function stopPan(event: React.PointerEvent<SVGSVGElement>) {
    if (panStart.current?.pointerId !== event.pointerId) return;
    panStart.current = null;
    setIsPanning(false);
    if (event.currentTarget.hasPointerCapture(event.pointerId)) event.currentTarget.releasePointerCapture(event.pointerId);
  }
  function focusArtist(id: string) {
    const point = pointMap.get(id);
    setSelectedId(id);
    if (!point) return;
    const width = UNIVERSE_WIDTH / 5;
    const height = UNIVERSE_HEIGHT / 5;
    setView({ x: Math.max(0, Math.min(UNIVERSE_WIDTH - width, point.x - width / 2)), y: Math.max(0, Math.min(UNIVERSE_HEIGHT - height, point.y - height / 2)), width, height });
  }
  if (!points.length) return <section className="panel universe-panel"><div className="section-heading"><div><span className="eyebrow">Listening universe</span><h2>Artists as musical galaxies</h2><p className="universe-rules">No fixed artist cap. Connections appear when the same artist pair occurs at least twice within 30-minute listening sessions.</p></div></div><div className="empty-state">No repeated artist connections in this period.</div></section>;
  return <section className="panel universe-panel">
    <div className="section-heading universe-heading"><div><span className="eyebrow">Listening universe</span><h2>Artists as musical galaxies</h2><p className="universe-rules">No fixed artist cap. Recurring connections come from transitions within 30-minute listening sessions. The most-listened artists appear first; zoom in to reveal the complete universe.</p></div><div className="universe-tools"><label className="universe-picker">Find artist<select value={selected?.id ?? ""} onChange={(event) => focusArtist(event.target.value)}>{points.map((point) => <option value={point.id} key={point.id}>{point.name}</option>)}</select></label><div className="universe-key"><span>{visiblePoints.length} of {points.length} artists visible</span><span><i className="universe-key-portrait" />More listening</span><span><i className="universe-key-line" />More transitions</span></div></div></div>
    <div className={`universe-canvas ${isPanning ? "is-panning" : ""}`}>
      <div className="universe-zoom-controls" aria-label="Galaxy zoom controls"><button type="button" onClick={() => updateZoom(1 / 1.35)} aria-label="Zoom out" disabled={zoom <= 1.001}>−</button><output aria-live="polite">{Math.round(zoom * 100)}%</output><button type="button" onClick={() => updateZoom(1.35)} aria-label="Zoom in" disabled={zoom >= 7.999}>+</button><button className="universe-reset" type="button" onClick={() => setView(UNIVERSE_VIEW)} disabled={zoom <= 1.001}>Reset</button></div>
      <div className="universe-zoom-hint">Use + / − or Ctrl/⌘ + scroll to zoom · drag to explore</div>
      <svg viewBox={`${view.x} ${view.y} ${view.width} ${view.height}`} role="img" aria-label="Artist portrait network. Portrait size represents listening time and lines connect artists played consecutively." onPointerDown={startPan} onPointerMove={movePan} onPointerUp={stopPan} onPointerCancel={stopPan} onWheel={(event) => { if (!event.ctrlKey && !event.metaKey) return; event.preventDefault(); const rect = event.currentTarget.getBoundingClientRect(); updateZoom(Math.exp(-event.deltaY * .0015), (event.clientX - rect.left) / rect.width, (event.clientY - rect.top) / rect.height); }}>
        <title>Listening universe for the selected date range</title>
        <desc>Every artist with a recurring listening connection is included. Larger portraits represent more minutes listened, and stronger lines represent repeated consecutive transitions no more than 30 minutes apart. Selecting a portrait focuses its closest listening relationships.</desc>
        <defs>{visiblePoints.map((point) => <clipPath id={`universe-portrait-${point.rank}`} key={point.id}><circle r={point.radius} /></clipPath>)}</defs>
        <g className="universe-edges">{graph.edges.map((edge) => { const source = pointMap.get(edge.source); const target = pointMap.get(edge.target); if (!source || !target || !visibleIds.has(source.id) || !visibleIds.has(target.id)) return null; const active = edge.source === selected?.id || edge.target === selected?.id; return <line className={active ? "active" : "muted"} key={`${edge.source}-${edge.target}`} x1={source.x} y1={source.y} x2={target.x} y2={target.y} style={{ "--edge-strength": edge.weight / maxEdge } as React.CSSProperties} />; })}</g>
        <g className="universe-nodes">{visiblePoints.map((point) => { const label = `${point.name}: ${Math.round(point.minutes)} minutes, ${point.plays} plays, ${point.connections} consecutive transitions`; const isSelected = selected?.id === point.id; const related = neighbourIds.has(point.id); const initials = point.name.split(/\s+/).slice(0, 2).map((part) => part[0]).join(""); return <g key={point.id} role="button" tabIndex={0} aria-label={label} className={`universe-node ${isSelected ? "selected" : related ? "related" : "muted"}`} transform={`translate(${point.x} ${point.y})`} onClick={() => setSelectedId(point.id)} onFocus={() => setSelectedId(point.id)} onPointerEnter={(event) => { setSelectedId(point.id); show(event, label); }} onPointerLeave={hide} onPointerCancel={hide} onKeyDown={(event) => { if (event.key === "Enter" || event.key === " ") { event.preventDefault(); setSelectedId(point.id); } }}>
          <circle className="universe-halo" r={point.radius + 9} />
          {point.coverUrl ? <image className="universe-photo" href={point.coverUrl} x={-point.radius} y={-point.radius} width={point.radius * 2} height={point.radius * 2} preserveAspectRatio="xMidYMid slice" clipPath={`url(#universe-portrait-${point.rank})`} /> : <><circle className="universe-fallback" r={point.radius} /><text className="universe-initials" y="4">{initials}</text></>}
          <circle className="universe-portrait-ring" r={point.radius} />
          {(point.rank < labelLimit || isSelected) && <text className="universe-label" y={point.radius + 22}>{point.name.length > 19 ? `${point.name.slice(0, 18)}…` : point.name}</text>}
        </g>; })}</g>
      </svg>
    </div>
    <div className="universe-detail"><span><b>{selected?.name}</b>Selected artist</span><span><b>{Math.round(selected?.minutes ?? 0).toLocaleString()} min</b>{selected?.plays.toLocaleString()} plays</span><span><b>{selected?.connections.toLocaleString()}</b>consecutive transitions</span><small>{strongestNeighbours.length ? `Strongest links: ${strongestNeighbours.map((item) => `${pointMap.get(item.id)?.name ?? "Unknown"} (${item.weight})`).join(" · ")}` : "No consecutive artist links in this period"}</small></div>
    <InstantTooltip tooltip={tooltip} />
  </section>;
}

function AlbumMosaic({ items, options, period, onPeriod }: { items: MosaicAlbum[]; options: MosaicPeriodOption[]; period: MosaicPeriod; onPeriod: (period: MosaicPeriod) => void }) {
  const { tooltip, show, hide } = useInstantTooltip();
  const seasons = options.filter((option) => option.kind === "season");
  const maxMinutes = Math.max(1, ...items.map((item) => item.minutes));
  return <section className="panel mosaic-panel">
    <div className="section-heading mosaic-heading"><div><span className="eyebrow">Album-cover mosaic</span><h2>A listening period in artwork</h2><p className="mosaic-rules">The 48 most-listened albums form a square-packed treemap. Every cover stays square, while its area follows listening time.</p></div><label className="mosaic-filter">Artwork period<select value={period} onChange={(event) => onPeriod(event.target.value as MosaicPeriod)}><option value="all">All selected dates</option>{seasons.length > 0 && <optgroup label="Seasons">{seasons.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}</optgroup>}</select></label></div>
    {items.length ? <div className="album-mosaic" aria-label="Square album-cover treemap sized by listening time.">{items.map((item, index) => { const label = `Number ${index + 1}. ${item.name} by ${item.artistName}: ${Math.round(item.minutes)} minutes, ${item.plays} plays`; const span = Math.max(1, Math.min(4, Math.round(Math.sqrt(item.minutes / maxMinutes) * 4))); return <article className="mosaic-tile" style={{ "--tile-span": span } as React.CSSProperties} key={item.id} tabIndex={0} aria-label={label} onPointerEnter={(event) => show(event, label)} onPointerLeave={hide} onPointerCancel={hide}>
      {item.coverUrl ? <img src={item.coverUrl} alt={`${item.name} by ${item.artistName}`} /> : <div className="mosaic-fallback">{item.name.slice(0, 1)}</div>}
    </article>; })}</div> : <div className="empty-state">No album data in this period.</div>}
    <div className="mosaic-note">Larger squares represent more listening time. Hover over a cover for album details.</div>
    <InstantTooltip tooltip={tooltip} />
  </section>;
}

function axisLabel(label: string, monthly: boolean): string {
  const [year, month, day] = (monthly ? `${label}-01` : label).split("-");
  return `${day}-${month}-${year.slice(-2)}`;
}

function BarSeries({ data, monthly }: { data: ReturnType<typeof weeklySeries>; monthly: boolean }) {
  const { tooltip, show, hide } = useInstantTooltip();
  const max = Math.max(1, ...data.map((item) => item.value));
  return <div className="bar-chart" role="img" aria-label="Listening plays over time">
    {data.map((item) => { const text = `${item.label}: ${item.value} plays, ${Math.round(item.minutes)} minutes`; return <div className="bar-column" key={item.label} aria-label={text} onPointerEnter={(event) => show(event, text)} onPointerLeave={hide} onPointerCancel={hide}>
      <i style={{ height: item.value ? `${Math.max(4, (item.value / max) * 100)}%` : 0 }} /><small>{axisLabel(item.label, monthly)}</small>
    </div>; })}
    <InstantTooltip tooltip={tooltip} />
  </div>;
}

function DiscoveryChart({ data, monthly }: { data: ReturnType<typeof discoverySeries>; monthly: boolean }) {
  const { tooltip, show, hide } = useInstantTooltip();
  const max = Math.max(1, ...data.map((item) => item.fresh + item.replay));
  return <div className="bar-chart discovery-chart" role="img" aria-label="New versus replayed tracks over time">
    {data.map((item) => { const text = `${item.label}: ${item.fresh} new, ${item.replay} replayed, ${Math.round(item.score * 100)}% exploration`; return <div className="bar-column" key={item.label} aria-label={text} onPointerEnter={(event) => show(event, text)} onPointerLeave={hide} onPointerCancel={hide}>
      <div className="stack" style={{ height: item.fresh + item.replay ? `${Math.max(6, ((item.fresh + item.replay) / max) * 100)}%` : 0, minHeight: item.fresh + item.replay ? 6 : 0 }}><i className="fresh" style={{ flex: item.fresh }} /><i className="replay" style={{ flex: item.replay }} /></div>
      <small>{axisLabel(item.label, monthly)}</small>
    </div>; })}
    <InstantTooltip tooltip={tooltip} />
  </div>;
}

function Fingerprint({ grid, metric }: { grid: number[][]; metric: string }) {
  const { tooltip, show, hide } = useInstantTooltip();
  const max = Math.max(1, ...grid.flat());
  const days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
  return <div className="fingerprint-wrap"><div className="fingerprint">
    {grid.map((row, day) => <div className="fingerprint-row" key={days[day]}><b>{days[day]}</b>{row.map((value, hour) => { const text = `${days[day]} ${String(hour).padStart(2,"0")}:00 — ${metric === "plays" ? Math.round(value) : value.toFixed(1)} ${metric}`; return <span key={hour} aria-label={text} onPointerEnter={(event) => show(event, text)} onPointerLeave={hide} onPointerCancel={hide} style={{ "--heat": value / max } as React.CSSProperties} />; })}</div>)}
    <div className="fingerprint-hours"><b />{Array.from({length:24}, (_, hour) => <small key={hour}>{hour % 3 === 0 ? hour : ""}</small>)}</div>
  </div><InstantTooltip tooltip={tooltip} /></div>;
}

function Loading() { return <main className="dashboard-shell"><div className="loading-head shimmer" /><div className="loading-grid">{Array.from({length:10},(_,i)=><div className="loading-card shimmer" key={i}/>)}</div></main>; }

export default function DashboardClient() {
  const [payload, setPayload] = useState<DashboardPayload | null>(null);
  const [error, setError] = useState("");
  const [preset, setPreset] = useState<RangePreset>("year");
  const [custom, setCustom] = useState<DateRange>(() => rangeForPreset("year", []));
  const [tab, setTab] = useState<"artists"|"tracks"|"albums"|"genres">("artists");
  const [grain, setGrain] = useState<"week"|"month">("week");
  const [fingerprintMetric, setFingerprintMetric] = useState<"plays"|"minutes">("plays");
  const [mosaicPeriod, setMosaicPeriod] = useState<MosaicPeriod>("all");

  useEffect(() => {
    fetch("/api/dashboard").then(async (response) => {
      if (!response.ok) throw new Error((await response.json().catch(() => null))?.error ?? "Dashboard data is unavailable");
      return response.json();
    }).then(setPayload).catch((reason) => setError(reason instanceof Error ? reason.message : "Dashboard data is unavailable"));
  }, []);

  const all = useMemo(() => payload ? enrich(payload) : [], [payload]);
  const range = useMemo(() => preset === "custom" ? custom : rangeForPreset(preset, payload?.plays ?? []), [preset, custom, payload]);
  const selected = useMemo(() => inRange(all, range), [all, range]);
  const previous = useMemo(() => inRange(all, previousRange(range)), [all, range]);
  const currentMetrics = useMemo(() => metrics(selected), [selected]);
  const previousMetrics = useMemo(() => metrics(previous), [previous]);
  const ranked = useMemo(() => rankings(selected), [selected]);
  const universe = useMemo(() => listeningUniverse(selected), [selected]);
  const mosaicOptions = useMemo(() => mosaicPeriodOptions(selected), [selected]);
  const effectiveMosaicPeriod = mosaicPeriod === "all" || mosaicOptions.some((option) => option.value === mosaicPeriod) ? mosaicPeriod : "all";
  const mosaic = useMemo(() => albumMosaic(selected, effectiveMosaicPeriod), [selected, effectiveMosaicPeriod]);
  const activityData = useMemo(() => activity(all), [all]);
  const series = useMemo(() => weeklySeries(selected, range, grain === "month"), [selected, range, grain]);
  const discovery = useMemo(() => discoverySeries(selected, all, range, grain === "month"), [selected, all, range, grain]);
  const heatmap = useMemo(() => fingerprint(selected, fingerprintMetric), [selected, fingerprintMetric]);

  if (!payload && !error) return <Loading />;
  if (error) return <main className="error-page"><span className="eyebrow">Setup required</span><h1>Dashboard data is unavailable.</h1><p>{error}</p><a href="/admin">Open protected setup</a></main>;

  return <main className="dashboard-shell">
    <header className="hero">
      <div className="brand"><div><span className="eyebrow">Listening data</span><h1>Listening overview.</h1><p className="overview-note">Recent plays are collected from Spotify every five minutes. Duplicates are removed, track and artist details are enriched, and all dates use UTC+3.</p></div></div>
      <div className={`freshness ${payload!.stale ? "stale" : ""}`}><i />{timeAgo(payload!.lastSyncAt)}</div>
    </header>

    {payload!.reauthorizationRequired && <a className="reauth-banner" href="/admin"><span>Spotify needs to be reconnected. Your history is safe.</span><b>Open admin →</b></a>}

    <section className="range-bar" aria-label="Dashboard date range">
      <div className="preset-list">{presets.map((item) => <button className={preset === item.id ? "active" : ""} key={item.id} onClick={() => { setPreset(item.id); if (item.id === "custom") setCustom(range); }}>{item.label}</button>)}</div>
      <div className="range-dates"><label>From<input type="date" value={range.from} max={range.to} onChange={(event) => { setPreset("custom"); setCustom({...range, from:event.target.value}); }} /></label><span>—</span><label>To<input type="date" value={range.to} min={range.from} max={todayMoscow()} onChange={(event) => { setPreset("custom"); setCustom({...range, to:event.target.value}); }} /></label></div>
    </section>

    <section className="metrics-grid">
      <MetricCard label="Total plays" value={currentMetrics.plays} previous={previousMetrics.plays} />
      <MetricCard label="Unique tracks" value={currentMetrics.tracks} previous={previousMetrics.tracks} />
      <MetricCard label="Unique artists" value={currentMetrics.artists} previous={previousMetrics.artists} />
      <MetricCard label="Minutes listened" value={currentMetrics.minutes} previous={previousMetrics.minutes} />
      <MetricCard label="Active days" value={currentMetrics.activeDays} previous={previousMetrics.activeDays} />
    </section>

    <ActivityGrid data={activityData} />

    <section className="panel rankings-panel"><div className="section-heading"><div><span className="eyebrow">Personal favourites</span><h2>Top listening</h2></div><div className="segmented">{(["artists","tracks","albums","genres"] as const).map((item)=><button key={item} className={tab===item?"active":""} onClick={()=>setTab(item)}>{item}</button>)}</div></div><Rankings items={ranked[tab]} title={tab} /></section>

    <ListeningUniverse graph={universe} />

    <AlbumMosaic items={mosaic} options={mosaicOptions} period={effectiveMosaicPeriod} onPeriod={setMosaicPeriod} />

    <section className="panel"><div className="section-heading"><div><span className="eyebrow">Listening pulse</span><h2>Plays over time</h2></div><div className="segmented"><button className={grain==="week"?"active":""} onClick={()=>setGrain("week")}>Week</button><button className={grain==="month"?"active":""} onClick={()=>setGrain("month")}>Month</button></div></div><BarSeries data={series} monthly={grain === "month"} /></section>

    <div className="analysis-grid"><section className="panel"><div className="section-heading"><div><span className="eyebrow">Exploration</span><h2>Discovery vs replay</h2></div><div className="chart-key"><span><i className="fresh"/>New</span><span><i className="replay"/>Replay</span></div></div><DiscoveryChart data={discovery} monthly={grain === "month"} /></section>
    <section className="panel"><div className="section-heading"><div><span className="eyebrow">Time distribution</span><h2>Listening by day and hour</h2></div><div className="segmented"><button className={fingerprintMetric==="plays"?"active":""} onClick={()=>setFingerprintMetric("plays")}>Plays</button><button className={fingerprintMetric==="minutes"?"active":""} onClick={()=>setFingerprintMetric("minutes")}>Minutes</button></div></div><Fingerprint grid={heatmap} metric={fingerprintMetric} /></section></div>

    <section className="panel recent-panel"><div className="section-heading"><div><span className="eyebrow">Latest</span><h2>Recently played</h2></div><span className="timezone-label">UTC+3</span></div><div className="recent-list">{all.slice(0,8).map((play)=><a href={play.trackUrl} target="_blank" rel="noreferrer" key={`${play.playedAt}-${play.trackId}`}><div>{play.coverUrl?<img src={play.coverUrl} alt=""/>:<span className="mini-cover">♪</span>}<span><b>{play.trackName}</b><small>{play.artistName}</small></span></div><time>{new Intl.DateTimeFormat("en",{timeZone:"Europe/Moscow",hour:"2-digit",minute:"2-digit",month:"short",day:"numeric"}).format(new Date(play.playedAt))}</time></a>)}</div></section>

    <footer><span>Spotify Logger</span><p>Collected automatically · Shown in UTC+3</p><a href="/admin">Admin</a></footer>
  </main>;
}
