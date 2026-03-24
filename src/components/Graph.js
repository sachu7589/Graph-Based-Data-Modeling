"use client";

import axios from "axios";
import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import CytoscapeComponent from "react-cytoscapejs";

export default function Graph({ onNodeInspect, highlightNodeIds = [] } = {}) {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [elements, setElements] = useState([]);

  const cyRef = useRef(null);
  const boundRef = useRef(false);
  const expandedNodeIdsRef = useRef(new Set());
  const selectedNodeIdRef = useRef(null);
  const handlersRef = useRef({
    tapNode: null,
    mouseOverNode: null,
    mouseOutNode: null,
  });
  const highlightKey = Array.isArray(highlightNodeIds)
    ? highlightNodeIds.map(String).join(",")
    : "";

  useEffect(() => {
    let cancelled = false;

    async function fetchGraph() {
      try {
        setLoading(true);
        setError(null);
        const res = await axios.get("/api/graph");
        const payload = res.data;

        const nodes = Array.isArray(payload?.nodes) ? payload.nodes : [];
        const edges = Array.isArray(payload?.edges) ? payload.edges : [];
        const nextElements = [...nodes, ...edges];

        if (!cancelled) setElements(nextElements);
      } catch (err) {
        if (cancelled) return;
        const message =
          err instanceof Error
            ? err.message
            : typeof err === "object" && err
              ? JSON.stringify(err)
              : String(err);
        setError(message);
      } finally {
        if (!cancelled) setLoading(false);
      }
    }

    fetchGraph();
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    return () => {
      const cy = cyRef.current;
      if (!cy) return;

      // Prevent Cytoscape from trying to refresh after unmount in dev.
      try {
        cy.destroy();
      } catch {
        // ignore
      }
    };
  }, []);

  const stylesheet = useMemo(
    () => [
      {
        selector: "node",
        style: {
          "background-color": "#2563eb",
          // Screenshot-like: render nodes as tiny dots (no label clutter).
          label: "",
          "font-size": 0,
          shape: "ellipse",
          width: 4,
          height: 4,
          "border-width": 0,
          "border-color": "#111827",
        },
      },
      {
        selector: "edge",
        style: {
          // Screenshot-like: edges are light strokes with arrows; no relationship text.
          label: "",
          "curve-style": "bezier",
          "target-arrow-shape": "triangle",
          "target-arrow-color": "#93c5fd",
          "line-color": "#93c5fd",
          width: 1,
          "font-size": 0,
          "arrow-scale": 0.8,
        },
      },
    ],
    []
  );

  function mergeElements(prevElements, nextPayload) {
    const nodeMap = new Map();
    const edgeMap = new Map();

    for (const el of prevElements) {
      const d = el?.data;
      if (!d) continue;
      if (typeof d.source === "string" && typeof d.target === "string") {
        const id =
          typeof d.id === "string" ? d.id : `${d.source}->${d.target}:${d.label ?? ""}`;
        edgeMap.set(String(id), el);
      } else if (typeof d.id === "string") {
        nodeMap.set(d.id, el);
      }
    }

    for (const n of nextPayload.nodes ?? []) {
      const d = n?.data;
      if (!d?.id) continue;
      nodeMap.set(String(d.id), n);
    }
    for (const e of nextPayload.edges ?? []) {
      const d = e?.data;
      if (!d?.id || !d?.source || !d?.target) continue;
      edgeMap.set(String(d.id), e);
    }

    return [...nodeMap.values(), ...edgeMap.values()];
  }

  async function expandNode(nodeId) {
    if (!nodeId) return;
    if (expandedNodeIdsRef.current.has(String(nodeId))) return;
    expandedNodeIdsRef.current.add(String(nodeId));

    try {
      const res = await axios.get("/api/graph/expand", {
        params: { nodeId: String(nodeId), limit: 200 },
      });
      const payload = res.data;
      setElements((prev) => mergeElements(prev, payload));
    } catch (err) {
      const message =
        err instanceof Error ? err.message : typeof err === "string" ? err : "Expand failed";
      console.error(message, err);
    }
  }

  const applyNodeStyles = useCallback((cy) => {
    const highlightSet = new Set(
      highlightKey
        ? highlightKey.split(",").filter(Boolean)
        : []
    );
    const selectedId = selectedNodeIdRef.current ? String(selectedNodeIdRef.current) : null;

    cy.nodes().forEach((n) => {
      const nodeId = String(n.id());
      const isSelected = selectedId === nodeId;
      const isHighlighted = highlightSet.has(nodeId);

      if (isSelected) {
        n.style("background-color", "#ef4444");
        n.style("border-width", 2);
        n.style("border-color", "#111827");
        n.style("width", 8);
        n.style("height", 8);
        return;
      }

      if (isHighlighted) {
        n.style("background-color", "#06b6d4");
        n.style("border-width", 2);
        n.style("border-color", "#0e7490");
        n.style("width", 10);
        n.style("height", 10);
        return;
      }

      n.style("background-color", "#2563eb");
      n.style("border-width", 0);
      n.style("width", 4);
      n.style("height", 4);
    });
  }, [highlightKey]);

  useEffect(() => {
    const cy = cyRef.current;
    if (!cy) return;
    applyNodeStyles(cy);
  }, [applyNodeStyles, elements.length, highlightKey]);

  return (
    <div className="w-full h-full">
      {loading ? (
        <div className="text-zinc-600">Loading graph...</div>
      ) : error ? (
        <div className="text-red-600">
          Failed to load graph: {error}
        </div>
      ) : (
        <CytoscapeComponent
          key="neo4j-graph"
          elements={elements}
          style={{ width: "100%", height: "100%" }}
          layout={{ name: "cose", animate: false }}
          stylesheet={stylesheet}
          cy={(cy) => {
            cyRef.current = cy;
            if (boundRef.current) return;
            boundRef.current = true;

            handlersRef.current.tapNode = (evt) => {
              const clicked = evt.target;
              const data = clicked.data();
              console.log("Node clicked:", data);
              selectedNodeIdRef.current = String(data?.id ?? "");
              applyNodeStyles(cy);

              if (typeof onNodeInspect === "function") onNodeInspect(data);
              // Expand graph around this node.
              expandNode(data?.id);
            };

            handlersRef.current.mouseOverNode = (evt) => {
              evt.target.style("border-width", 1);
              evt.target.style("border-color", "#111827");
            };

            handlersRef.current.mouseOutNode = (evt) => {
              const n = evt.target;
              const bg = n.style("background-color");
              // If it's selected (we set red), keep border.
              if (bg === "#ef4444") return;
              n.style("border-width", 0);
            };

            // Click-to-highlight + hover effects.
            cy.on("tap", "node", handlersRef.current.tapNode);
            cy.on("mouseover", "node", handlersRef.current.mouseOverNode);
            cy.on("mouseout", "node", handlersRef.current.mouseOutNode);
          }}
        />
      )}
    </div>
  );
}

