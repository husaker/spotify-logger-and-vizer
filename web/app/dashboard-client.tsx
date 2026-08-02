"use client";

/* Dynamic Spotify CDN artwork is intentionally rendered directly. */
/* eslint-disable @next/next/no-img-element */

import { useEffect, useMemo, useState } from "react";
import { createPortal } from "react-dom";
import {
  activity, activityStats, delta, discoverySeries, enrich, fingerprint, inRange, metrics, previousRange,
  rangeForPreset, rankings, todayMoscow, weeklySeries, type DateRange, type RangePreset,
  type RankedItem,
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
  function show(event: React.PointerEvent<HTMLElement>, text: string) {
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

function axisLabel(label: string, monthly: boolean): string {
  const [year, month, day] = (monthly ? `${label}-01` : label).split("-");
  return `${day}-${month}-${year.slice(-2)}`;
}

function BarSeries({ data, monthly }: { data: ReturnType<typeof weeklySeries>; monthly: boolean }) {
  const { tooltip, show, hide } = useInstantTooltip();
  const max = Math.max(1, ...data.map((item) => item.value));
  return <div className="bar-chart" role="img" aria-label="Listening plays over time">
    {data.map((item) => { const text = `${item.label}: ${item.value} plays, ${Math.round(item.minutes)} minutes`; return <div className="bar-column" key={item.label} aria-label={text} onPointerEnter={(event) => show(event, text)} onPointerLeave={hide} onPointerCancel={hide}>
      <span className="bar-value">{item.value}</span><i style={{ height: item.value ? `${Math.max(4, (item.value / max) * 100)}%` : 0 }} /><small>{axisLabel(item.label, monthly)}</small>
    </div>; })}
    <InstantTooltip tooltip={tooltip} />
  </div>;
}

function DiscoveryChart({ data, monthly }: { data: ReturnType<typeof discoverySeries>; monthly: boolean }) {
  const { tooltip, show, hide } = useInstantTooltip();
  const max = Math.max(1, ...data.map((item) => item.fresh + item.replay));
  return <div className="bar-chart discovery-chart" role="img" aria-label="New versus replayed tracks over time">
    {data.map((item) => { const text = `${item.label}: ${item.fresh} new, ${item.replay} replayed, ${Math.round(item.score * 100)}% exploration`; return <div className="bar-column" key={item.label} aria-label={text} onPointerEnter={(event) => show(event, text)} onPointerLeave={hide} onPointerCancel={hide}>
      <span className="bar-value accent">{Math.round(item.score * 100)}%</span>
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
  const activityData = useMemo(() => activity(all), [all]);
  const series = useMemo(() => weeklySeries(selected, range, grain === "month"), [selected, range, grain]);
  const discovery = useMemo(() => discoverySeries(selected, all, range, grain === "month"), [selected, all, range, grain]);
  const heatmap = useMemo(() => fingerprint(selected, fingerprintMetric), [selected, fingerprintMetric]);

  if (!payload && !error) return <Loading />;
  if (error) return <main className="error-page"><span className="eyebrow">Setup required</span><h1>Dashboard data is unavailable.</h1><p>{error}</p><a href="/admin">Open protected setup</a></main>;

  return <main className="dashboard-shell">
    <header className="hero">
      <div className="brand"><div><span className="eyebrow">Listening data</span><h1>Listening overview.</h1></div></div>
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

    <section className="panel"><div className="section-heading"><div><span className="eyebrow">Listening pulse</span><h2>Plays over time</h2></div><div className="segmented"><button className={grain==="week"?"active":""} onClick={()=>setGrain("week")}>Week</button><button className={grain==="month"?"active":""} onClick={()=>setGrain("month")}>Month</button></div></div><BarSeries data={series} monthly={grain === "month"} /></section>

    <div className="analysis-grid"><section className="panel"><div className="section-heading"><div><span className="eyebrow">Exploration</span><h2>Discovery vs replay</h2></div><div className="chart-key"><span><i className="fresh"/>New</span><span><i className="replay"/>Replay</span></div></div><DiscoveryChart data={discovery} monthly={grain === "month"} /></section>
    <section className="panel"><div className="section-heading"><div><span className="eyebrow">Time distribution</span><h2>Listening by day and hour</h2></div><div className="segmented"><button className={fingerprintMetric==="plays"?"active":""} onClick={()=>setFingerprintMetric("plays")}>Plays</button><button className={fingerprintMetric==="minutes"?"active":""} onClick={()=>setFingerprintMetric("minutes")}>Minutes</button></div></div><Fingerprint grid={heatmap} metric={fingerprintMetric} /></section></div>

    <section className="panel recent-panel"><div className="section-heading"><div><span className="eyebrow">Latest</span><h2>Recently played</h2></div><span className="timezone-label">Europe/Moscow</span></div><div className="recent-list">{all.slice(0,8).map((play)=><a href={play.trackUrl} target="_blank" rel="noreferrer" key={`${play.playedAt}-${play.trackId}`}><div>{play.coverUrl?<img src={play.coverUrl} alt=""/>:<span className="mini-cover">♪</span>}<span><b>{play.trackName}</b><small>{play.artistName}</small></span></div><time>{new Intl.DateTimeFormat("en",{timeZone:"Europe/Moscow",hour:"2-digit",minute:"2-digit",month:"short",day:"numeric"}).format(new Date(play.playedAt))}</time></a>)}</div></section>

    <footer><span>Spotify Logger</span><p>Collected automatically · Shown in Europe/Moscow time</p><a href="/admin">Admin</a></footer>
  </main>;
}
