import { useCallback, useEffect, useMemo, useState } from "react";
import { fetchIncidents, fetchLogs, fetchServices, fetchStats, fetchTopAnomalies } from "./api";
import { useLiveFeed } from "./useLiveFeed";
import type { ConnectionStatus, FeedItem, Incident, LogLine, ServiceInfo, Stats, TopAnomaly } from "./types";

/** Re-runs `load` every `ms`; keeps the last good value and the last error. */
function usePolled<T>(load: () => Promise<T>, ms: number) {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<string | null>(null);
  useEffect(() => {
    let alive = true;
    const tick = () =>
      load().then(
        (v) => alive && (setData(v), setError(null)),
        (e: Error) => alive && setError(e.message),
      );
    tick();
    const id = window.setInterval(tick, ms);
    return () => {
      alive = false;
      window.clearInterval(id);
    };
  }, [load, ms]);
  return { data, error };
}

const timeOf = (ms: number) => new Date(ms).toLocaleTimeString();

function Severity({ value }: { value: number }) {
  return <span className={`sev sev-${Math.min(5, Math.max(1, value || 1))}`}>S{value ?? "?"}</span>;
}

function ConnectionBadge({ status }: { status: ConnectionStatus }) {
  const label = { connecting: "connecting…", live: "live", reconnecting: "reconnecting…" }[status];
  return <span className={`conn conn-${status}`}>● {label}</span>;
}

function StatCard({ label, value, hint }: { label: string; value: string | number | null; hint?: string }) {
  return (
    <div className="card stat">
      <div className="stat-value">{value ?? "—"}</div>
      <div className="stat-label">{label}</div>
      {hint && <div className="stat-hint">{hint}</div>}
    </div>
  );
}

function Panel({ title, error, children }: { title: string; error?: string | null; children: React.ReactNode }) {
  return (
    <section className="card panel">
      <h2>{title}</h2>
      {error && <div className="error">{error}</div>}
      {children}
    </section>
  );
}

function LiveFeed({ items, name }: { items: FeedItem[]; name: (id: string) => string }) {
  if (items.length === 0) return <div className="empty">Waiting for events… (start the pipeline)</div>;
  return (
    <ul className="feed">
      {items.map((it) =>
        it.kind === "anomaly" ? (
          <li key={it.key} className="feed-row">
            <span className="time">{timeOf(it.at)}</span>
            <Severity value={it.data.severity} />
            <span className="tag">{it.data.anomaly_type}</span>
            <span className="grow">{name(it.data.service_id)}</span>
            <span className="muted">{it.data.model_used}</span>
          </li>
        ) : (
          <li key={it.key} className="feed-row incident-row">
            <span className="time">{timeOf(it.at)}</span>
            <Severity value={it.data.severity} />
            <span className="tag tag-incident">INCIDENT</span>
            <span className="grow">
              root cause {name(it.data.root_cause_service)} · {it.data.member_service_ids.length} services
            </span>
            <span className="muted">{it.data.causality_method}</span>
          </li>
        ),
      )}
    </ul>
  );
}

function IncidentsTable({ rows, name }: { rows: Incident[]; name: (id: string) => string }) {
  if (rows.length === 0) return <div className="empty">No incidents yet.</div>;
  return (
    <table>
      <thead>
        <tr><th>Opened</th><th>Sev</th><th>Root cause</th><th>Services</th><th>Method</th></tr>
      </thead>
      <tbody>
        {rows.map((r) => (
          <tr key={r.incident_id}>
            <td>{timeOf(r.opened_at * 1000)}</td>
            <td><Severity value={r.severity} /></td>
            <td>{name(r.root_cause_service)}</td>
            <td>{r.member_service_ids.length} <span className="muted">({r.anomaly_count} anomalies)</span></td>
            <td className="muted">{r.causality_method}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function TopAnomalies({ rows, name }: { rows: TopAnomaly[]; name: (id: string) => string }) {
  if (rows.length === 0) return <div className="empty">Nothing in the Redis top-k yet.</div>;
  return (
    <table>
      <thead><tr><th>Score</th><th>Service</th><th>Type</th><th>When</th></tr></thead>
      <tbody>
        {rows.map((r) => (
          <tr key={`${r.service_id}-${r.detected_at}`}>
            <td><strong>{r.anomaly_score.toFixed(2)}</strong></td>
            <td>{name(r.service_id)}</td>
            <td><span className="tag">{r.anomaly_type}</span></td>
            <td className="muted">{timeOf(r.detected_at)}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function LogSearch({ services }: { services: ServiceInfo[] }) {
  const [serviceId, setServiceId] = useState("");
  const [level, setLevel] = useState("");
  const [q, setQ] = useState("");
  const load = useCallback(() => fetchLogs({ service_id: serviceId, level, q }), [serviceId, level, q]);
  const { data, error } = usePolled<LogLine[]>(load, 4000);
  return (
    <>
      <div className="filters">
        <select value={serviceId} onChange={(e) => setServiceId(e.target.value)}>
          <option value="">all services</option>
          {services.map((s) => <option key={s.id} value={s.id}>{s.name}</option>)}
        </select>
        <select value={level} onChange={(e) => setLevel(e.target.value)}>
          <option value="">any level</option>
          <option>INFO</option><option>WARN</option><option>ERROR</option>
        </select>
        <input placeholder="search message…" value={q} onChange={(e) => setQ(e.target.value)} />
      </div>
      {error && <div className="error">{error}</div>}
      <div className="logs">
        {(data ?? []).map((l) => (
          <div key={`${l.span_id}`} className={`log log-${l.level}`}>
            <span className="time">{timeOf(l.timestamp)}</span>
            <span className="lvl">{l.level}</span>
            <span className="muted">{l.service_id}</span>
            <span className="grow">{l.message}</span>
          </div>
        ))}
        {data && data.length === 0 && <div className="empty">No matching log lines.</div>}
      </div>
    </>
  );
}

export default function App() {
  const { items, status } = useLiveFeed();
  const stats = usePolled<Stats>(fetchStats, 5000);
  const incidents = usePolled<Incident[]>(fetchIncidents, 5000);
  const top = usePolled<TopAnomaly[]>(fetchTopAnomalies, 5000);
  const services = usePolled<ServiceInfo[]>(fetchServices, 300_000);

  const names = useMemo(() => new Map((services.data ?? []).map((s) => [s.id, s.name])), [services.data]);
  const name = useCallback((id: string) => names.get(id) ?? id, [names]);
  const byType = Object.entries(stats.data?.anomalies_by_type ?? {}).map(([k, v]) => `${k} ${v}`).join(" · ");

  return (
    <div className="app">
      <header>
        <h1>DistroSense</h1>
        <span className="subtitle">real-time anomaly detection &amp; incident response</span>
        <span className="grow" />
        <ConnectionBadge status={status} />
      </header>

      {stats.error && <div className="error banner">Stats unavailable: {stats.error}</div>}
      <div className="stats">
        <StatCard label="anomalies (15 min)" value={stats.data?.anomalies_15m ?? null} hint={byType || undefined} />
        <StatCard label="incidents (1 h)" value={stats.data?.incidents_1h ?? null} />
        <StatCard label="noisiest service" value={stats.data?.noisiest_service ? name(stats.data.noisiest_service) : null} />
        <StatCard label="top-k in Redis" value={stats.data?.topk_size ?? null} />
        <StatCard label="log lines indexed" value={stats.data?.logs_indexed?.toLocaleString() ?? null} />
      </div>

      <div className="grid">
        <Panel title="Live feed (Kafka → WebSocket)"><LiveFeed items={items} name={name} /></Panel>
        <div className="col">
          <Panel title="Recent incidents" error={incidents.error}><IncidentsTable rows={incidents.data ?? []} name={name} /></Panel>
          <Panel title="Worst anomalies (Redis top-k)" error={top.error}><TopAnomalies rows={top.data ?? []} name={name} /></Panel>
        </div>
      </div>

      <Panel title="Logs (Elasticsearch)"><LogSearch services={services.data ?? []} /></Panel>
    </div>
  );
}
