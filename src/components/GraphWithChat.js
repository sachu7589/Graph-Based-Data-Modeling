"use client";

import React, { useMemo, useState } from "react";
import Graph from "./Graph";
import ChatBox from "./ChatBox";

function isNodeLike(value) {
  if (!value || typeof value !== "object") return false;
  return Array.isArray(value.labels) || value.identity !== undefined || value.id !== undefined;
}

function normalizeNodeLike(value) {
  if (!value || typeof value !== "object") return null;

  // Normalized node object from API route: { id, labels, properties }
  if (Array.isArray(value.labels) && value.properties) {
    return {
      id: value.id != null ? String(value.id) : "",
      label: value.labels[0] ? String(value.labels[0]) : "Node",
      properties: value.properties ?? {},
    };
  }

  // Raw neo4j-like object shape in arrays.
  if (Array.isArray(value.labels) && value.identity !== undefined && value.properties) {
    return {
      id: String(value.identity),
      label: value.labels[0] ? String(value.labels[0]) : "Node",
      properties: value.properties ?? {},
    };
  }

  return null;
}

function toDisplayRows(value) {
  if (!value || typeof value !== "object") return [];
  return Object.entries(value)
    .filter(([, v]) => v !== null && v !== undefined && v !== "")
    .map(([k, v]) => [k, typeof v === "string" ? v : JSON.stringify(v)])
    .slice(0, 18);
}

function buildCardData(selectedNodeData, queryPreviewData) {
  if (selectedNodeData && typeof selectedNodeData === "object") {
    const label = selectedNodeData.label ? String(selectedNodeData.label) : "Node";
    const rows = toDisplayRows(selectedNodeData);
    return { title: label, rows };
  }

  const row = queryPreviewData;
  if (!row || typeof row !== "object") return null;

  const entries = Object.entries(row);
  for (const [, value] of entries) {
    if (isNodeLike(value)) {
      const n = normalizeNodeLike(value);
      if (n) {
        const related = entries
          .filter(([, v]) => Array.isArray(v))
          .map(([k, v]) => [k, Array.isArray(v) ? String(v.length) : "0"]);
        const rows = [
          ["id", n.id],
          ...toDisplayRows(n.properties),
          ...related,
        ].slice(0, 18);
        return { title: n.label, rows };
      }
    }
    if (Array.isArray(value) && value.length > 0 && isNodeLike(value[0])) {
      const n = normalizeNodeLike(value[0]);
      if (n) {
        const rows = [["id", n.id], ...toDisplayRows(n.properties)].slice(0, 18);
        return { title: n.label, rows };
      }
    }
  }

  // Final fallback: show scalar row keys cleanly, not huge nested dumps.
  const rows = entries
    .map(([k, v]) => {
      if (Array.isArray(v)) return [k, `${v.length} item(s)`];
      if (typeof v === "object" && v) return [k, "object"];
      return [k, v == null ? "" : String(v)];
    })
    .slice(0, 18);
  return { title: "Query Match", rows };
}

export default function GraphWithChat() {
  const [granularOverlayVisible, setGranularOverlayVisible] = useState(true);
  const [chatMinimized, setChatMinimized] = useState(false);
  const [highlightNodeIds, setHighlightNodeIds] = useState([]);
  const [selectedNodeData, setSelectedNodeData] = useState(null);
  const [queryPreviewData, setQueryPreviewData] = useState(null);

  const quickReplyHint = useMemo(() => {
    return granularOverlayVisible ? "Hide Granular Overlay" : "Show Granular Overlay";
  }, [granularOverlayVisible]);

  const minimizeHint = useMemo(() => {
    return chatMinimized ? "Restore Chat" : "Minimize";
  }, [chatMinimized]);

  const cardData = useMemo(
    () => buildCardData(selectedNodeData, queryPreviewData),
    [selectedNodeData, queryPreviewData]
  );

  return (
    <div className="h-screen w-screen flex bg-white overflow-hidden">
      <section className="relative flex-[7] bg-white">
        <div className="absolute left-4 top-3 z-10 flex items-center gap-3 text-zinc-700 text-sm">
          <div className="font-semibold">
            Mapping
            <span className="px-2 text-zinc-300">/</span>
            Order To Cash
          </div>
        </div>

        <div className="absolute left-4 top-10 z-10 flex items-center gap-3">
          <button
            type="button"
            className="rounded-lg border border-zinc-200 bg-white/80 px-3 py-1.5 text-xs text-zinc-700 hover:bg-white"
            onClick={() => setChatMinimized((v) => !v)}
          >
            {minimizeHint}
          </button>

          <button
            type="button"
            className="rounded-lg border border-zinc-200 bg-white/80 px-3 py-1.5 text-xs text-zinc-700 hover:bg-white"
            onClick={() => setGranularOverlayVisible((v) => !v)}
          >
            {quickReplyHint}
          </button>
        </div>

        {granularOverlayVisible ? (
          <div
            className="absolute inset-0 pointer-events-none"
            style={{
              backgroundImage:
                "radial-gradient(rgba(0,0,0,0.10) 1px, transparent 1px)",
              backgroundSize: "6px 6px",
              opacity: 0.12,
              mixBlendMode: "multiply",
            }}
          />
        ) : null}

        <div className="absolute inset-0 p-0">
          <Graph
            highlightNodeIds={highlightNodeIds}
            onNodeInspect={(data) => setSelectedNodeData(data)}
          />
        </div>

        {cardData ? (
          <div className="absolute left-3 top-24 z-20 max-w-[320px] rounded-xl border border-zinc-200 bg-white/95 p-3 shadow-lg">
            <div className="text-sm font-semibold text-zinc-900 mb-2">{cardData.title}</div>
            <div className="text-xs text-zinc-700 space-y-1 max-h-[360px] overflow-auto">
              {cardData.rows.map(([k, v], idx) => (
                <div key={`${k}-${idx}`} className="break-words">
                  <span className="font-semibold">{k}: </span>
                  <span>{String(v)}</span>
                </div>
              ))}
            </div>
          </div>
        ) : null}
      </section>

      {chatMinimized ? null : (
        <aside className="flex-[3] border-l border-zinc-200 bg-white flex flex-col">
          <ChatBox
            onQueryResult={(result) => {
              const ids = Array.isArray(result?.highlightNodeIds)
                ? result.highlightNodeIds
                : [];
              setHighlightNodeIds(ids);
              const firstRow = Array.isArray(result?.data) && result.data.length > 0 ? result.data[0] : null;
              setQueryPreviewData(firstRow);
              setSelectedNodeData(null);
            }}
          />
        </aside>
      )}
    </div>
  );
}

