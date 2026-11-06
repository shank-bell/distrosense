import { useEffect, useRef, useState } from "react";
import type { Anomaly, ConnectionStatus, FeedItem, Incident } from "./types";

const MAX_ITEMS = 100;

/**
 * Subscribes to the backend's /ws stream (anomaly-events + incident-alerts from
 * Kafka) and keeps the newest MAX_ITEMS. Reconnects with exponential backoff.
 */
export function useLiveFeed() {
  const [items, setItems] = useState<FeedItem[]>([]);
  const [status, setStatus] = useState<ConnectionStatus>("connecting");
  const counter = useRef(0);

  useEffect(() => {
    let ws: WebSocket | null = null;
    let retry = 0;
    let timer: number | undefined;
    let closed = false;

    const connect = () => {
      const scheme = location.protocol === "https:" ? "wss" : "ws";
      ws = new WebSocket(`${scheme}://${location.host}/ws`);

      ws.onopen = () => {
        retry = 0;
        setStatus("live");
      };
      ws.onmessage = (ev) => {
        let frame: { type: string; data: unknown };
        try {
          frame = JSON.parse(ev.data);
        } catch {
          return;
        }
        const key = `${Date.now()}-${counter.current++}`;
        let item: FeedItem | null = null;
        if (frame.type === "anomaly") {
          const a = frame.data as Anomaly;
          item = { key, kind: "anomaly", at: a.detected_at, data: a };
        } else if (frame.type === "incident") {
          const i = frame.data as Incident;
          item = { key, kind: "incident", at: i.closed_at ? i.closed_at * 1000 : Date.now(), data: i };
        }
        if (item) setItems((prev) => [item as FeedItem, ...prev].slice(0, MAX_ITEMS));
      };
      ws.onclose = () => {
        if (closed) return;
        setStatus("reconnecting");
        retry += 1;
        timer = window.setTimeout(connect, Math.min(10_000, 500 * 2 ** retry));
      };
      ws.onerror = () => ws?.close();
    };

    connect();
    return () => {
      closed = true;
      window.clearTimeout(timer);
      ws?.close();
    };
  }, []);

  return { items, status };
}
