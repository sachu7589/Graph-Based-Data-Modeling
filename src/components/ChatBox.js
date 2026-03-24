"use client";

import React, { useEffect, useMemo, useRef, useState } from "react";
import axios from "axios";

function MessageBubble({ from, children }) {
  return (
    <div className="flex flex-col gap-1">
      <div className="text-xs font-semibold text-zinc-700">{from}</div>
      <div className="rounded-xl bg-zinc-100 px-3 py-2 text-sm text-zinc-900 whitespace-pre-wrap">
        {children}
      </div>
    </div>
  );
}

function formatAsJson(value) {
  try {
    return JSON.stringify(value, null, 2);
  } catch {
    return String(value);
  }
}

export default function ChatBox({ onQueryResult } = {}) {
  const [messages, setMessages] = useState([]);
  const [prompt, setPrompt] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const endRef = useRef(null);

  const canSend = useMemo(() => prompt.trim().length > 0 && !loading, [prompt, loading]);

  useEffect(() => {
    endRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages, loading]);

  async function onSend() {
    const question = prompt.trim();
    if (!question) return;

    setError(null);
    setLoading(true);

    setMessages((prev) => [
      ...prev,
      { from: "You", text: question },
    ]);

    setPrompt("");

    try {
      const res = await axios.post("/api/query", { question });
      const payload = res.data;

      if (payload?.answer && !payload.cypher) {
        if (typeof onQueryResult === "function") {
          onQueryResult({
            highlightNodeIds: Array.isArray(payload?.highlightNodeIds)
              ? payload.highlightNodeIds
              : [],
            cypher: payload?.cypher ?? null,
            data: Array.isArray(payload?.data) ? payload.data : [],
            answer: payload?.answer ?? "",
          });
        }
        setMessages((prev) => [
          ...prev,
          { from: "Assistant", text: payload.answer },
        ]);
        return;
      }

      setMessages((prev) => [
        ...prev,
        {
          from: "Assistant",
          answer: payload.answer,
          cypher: payload.cypher,
          data: payload.data,
        },
      ]);

      if (typeof onQueryResult === "function") {
        onQueryResult({
          highlightNodeIds: Array.isArray(payload?.highlightNodeIds)
            ? payload.highlightNodeIds
            : [],
          cypher: payload?.cypher ?? null,
          data: Array.isArray(payload?.data) ? payload.data : [],
          answer: payload?.answer ?? "",
        });
      }
    } catch (err) {
      const message =
        err instanceof Error ? err.message : typeof err === "string" ? err : "Query failed";
      setError(message);
      setMessages((prev) => [
        ...prev,
        { from: "Assistant", text: `Query failed: ${message}` },
      ]);
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="h-full flex flex-col">
      <div className="p-4 border-b border-zinc-200">
        <div className="text-sm font-semibold text-zinc-900">Query Graph</div>
        <div className="text-xs text-zinc-500 mt-1">Natural language to Cypher to Neo4j</div>
      </div>

      <div className="flex-1 overflow-auto p-4 flex flex-col gap-4">
        {messages.map((m, idx) => (
          <div key={idx}>
            <MessageBubble from={m.from}>
              {m.cypher != null ? (
                <div className="flex flex-col gap-3">
                  {m.answer ? (
                    <>
                      <div className="text-[11px] text-zinc-500">Answer</div>
                      <div className="text-[12px] text-zinc-900">{m.answer}</div>
                    </>
                  ) : null}
                  <div className="text-[11px] text-zinc-500">Generated Cypher</div>
                  <pre className="text-[11px] text-zinc-800 whitespace-pre-wrap">
                    {m.cypher}
                  </pre>
                  <div className="text-[11px] text-zinc-500">Response Data</div>
                  <pre className="text-[11px] text-zinc-800 whitespace-pre-wrap max-h-60 overflow-auto">
                    {formatAsJson(m.data ?? [])}
                  </pre>
                </div>
              ) : (
                m.text
              )}
            </MessageBubble>
          </div>
        ))}

        {loading ? (
          <div className="text-xs text-zinc-500">Executing query...</div>
        ) : null}
        {error ? <div className="text-xs text-red-600">{error}</div> : null}
        <div ref={endRef} />
      </div>

      <div className="p-4 border-t border-zinc-200">
        <div className="flex gap-2">
          <input
            className="flex-1 rounded-xl border border-zinc-200 px-3 py-2 text-sm text-black placeholder:text-black/80 outline-none focus:ring-2 focus:ring-blue-300"
            value={prompt}
            onChange={(e) => setPrompt(e.target.value)}
            placeholder="Ask your query..."
            onKeyDown={(e) => {
              if (e.key === "Enter" && canSend) onSend();
            }}
          />
          <button
            type="button"
            className="rounded-xl bg-zinc-900 text-white px-3 py-2 text-sm hover:bg-zinc-800 disabled:opacity-60 disabled:cursor-not-allowed"
            onClick={onSend}
            disabled={!canSend}
          >
            Send
          </button>
        </div>
      </div>
    </div>
  );
}

