export interface Anomaly {
  id: string;
  service_id: string;
  anomaly_type: string;
  severity: number;
  anomaly_score?: number;
  model_used: string;
  detected_at: number; // epoch ms
  z_score?: number | null;
}

/** One shape for incidents, whether they arrive over the WebSocket or from /api/incidents. */
export interface Incident {
  incident_id: string;
  root_cause_service: string;
  causality_method: string;
  member_service_ids: string[];
  anomaly_count: number;
  severity: number;
  anomaly_types?: string[];
  opened_at: number; // epoch seconds
  closed_at: number | null;
}

export interface TopAnomaly {
  service_id: string;
  anomaly_type: string;
  detected_at: number;
  anomaly_score: number;
}

export interface Stats {
  anomalies_15m: number;
  incidents_1h: number;
  anomalies_by_type: Record<string, number>;
  noisiest_service: string | null;
  topk_size: number | null;
  logs_indexed: number | null;
}

export interface LogLine {
  timestamp: number;
  service_id: string;
  level: string;
  message: string;
  trace_id: string;
  span_id: string;
}

export interface ServiceInfo {
  id: string;
  name: string;
  tier: string;
  team: string;
}

export type FeedItem =
  | { key: string; kind: "anomaly"; at: number; data: Anomaly }
  | { key: string; kind: "incident"; at: number; data: Incident };

export type ConnectionStatus = "connecting" | "live" | "reconnecting";
