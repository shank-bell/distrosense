import type { Incident, LogLine, ServiceInfo, Stats, TopAnomaly } from "./types";

async function getJson<T>(url: string): Promise<T> {
  const res = await fetch(url);
  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    throw new Error(body.detail ?? `${res.status} ${res.statusText}`);
  }
  return res.json() as Promise<T>;
}

interface IncidentRow {
  id: string;
  opened_at: number;
  closed_at: number | null;
  root_cause_service_id: string;
  member_service_ids: string[];
  anomaly_count: number;
  severity: number;
  causality_method: string;
}

export const fetchStats = () => getJson<Stats>("/api/stats");
export const fetchTopAnomalies = () => getJson<TopAnomaly[]>("/api/top-anomalies?limit=10");
export const fetchServices = () => getJson<ServiceInfo[]>("/api/services");

export async function fetchIncidents(): Promise<Incident[]> {
  const rows = await getJson<IncidentRow[]>("/api/incidents?limit=15");
  return rows.map((r) => ({
    incident_id: r.id,
    root_cause_service: r.root_cause_service_id,
    causality_method: r.causality_method,
    member_service_ids: r.member_service_ids,
    anomaly_count: r.anomaly_count,
    severity: r.severity,
    opened_at: Number(r.opened_at),
    closed_at: r.closed_at === null ? null : Number(r.closed_at),
  }));
}

export function fetchLogs(params: { service_id?: string; level?: string; q?: string }): Promise<LogLine[]> {
  const qs = new URLSearchParams();
  Object.entries(params).forEach(([k, v]) => v && qs.set(k, v));
  qs.set("limit", "40");
  return getJson<LogLine[]>(`/api/logs?${qs.toString()}`);
}
