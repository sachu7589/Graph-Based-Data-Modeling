import { NextResponse } from "next/server";
import { getNeo4jSession } from "@/lib/neo4j";

type CytoscapeNode = { data: Record<string, unknown> };
type CytoscapeEdge = {
  data: { id: string; source: string; target: string; label: string };
};

type Neo4jElementLike = {
  identity?: unknown;
  labels?: unknown;
  properties?: Record<string, unknown>;
};

type Neo4jRelationshipLike = {
  type?: unknown;
};

function toId(value: unknown): string {
  if (value === null || value === undefined) return "";
  if (typeof value === "string" || typeof value === "number") return String(value);
  if (typeof value === "bigint") return value.toString();
  const maybeToString = (value as { toString?: unknown }).toString;
  if (typeof maybeToString === "function") return (maybeToString as () => string)();
  return "";
}

function normalizeNeo4jPropertyValue(value: unknown): unknown {
  if (value === null || value === undefined) return undefined;
  const t = typeof value;
  if (t === "string" || t === "number" || t === "boolean") return value;
  if (t === "bigint") return value.toString();
  // neo4j-driver can return complex objects (e.g., arrays) - Cytoscape can't store those directly as properties.
  return JSON.stringify(value);
}

function normalizeProperties(properties: Record<string, unknown> | undefined): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  if (!properties) return out;

  for (const [k, v] of Object.entries(properties)) {
    const normalized = normalizeNeo4jPropertyValue(v);
    if (normalized !== undefined) out[k] = normalized;
  }

  return out;
}

export const runtime = "nodejs";

export async function GET() {
  const session = getNeo4jSession();

  try {
    const result = await session.run("MATCH (n)-[r]->(m) RETURN n, r, m LIMIT 200");

    const nodesById = new Map<string, CytoscapeNode>();
      const edges: CytoscapeEdge[] = [];
    const edgeKeys = new Set<string>();

    for (const record of result.records) {
      const n = record.get("n") as Neo4jElementLike | null | undefined;
      const r = record.get("r") as Neo4jRelationshipLike | null | undefined;
      const m = record.get("m") as Neo4jElementLike | null | undefined;

      const nId = toId(n?.identity);
      const mId = toId(m?.identity);

      if (!nId || !mId) continue;

      const nLabels: unknown[] = Array.isArray(n?.labels) ? (n.labels as unknown[]) : [];
      const mLabels: unknown[] = Array.isArray(m?.labels) ? (m.labels as unknown[]) : [];

      const nLabel = nLabels.length ? String(nLabels[0]) : "Node";
      const mLabel = mLabels.length ? String(mLabels[0]) : "Node";

      if (!nodesById.has(nId)) {
        const data = {
          ...normalizeProperties(n?.properties ?? {}),
          id: nId,
          label: nLabel,
        };
        nodesById.set(nId, { data });
      }

      if (!nodesById.has(mId)) {
        const data = {
          ...normalizeProperties(m?.properties ?? {}),
          id: mId,
          label: mLabel,
        };
        nodesById.set(mId, { data });
      }

      const relType =
        r?.type !== undefined && r?.type !== null ? String(r.type) : "RELATED_TO";
      const edgeKey = `${nId}->${mId}:${relType}`;
      if (!edgeKeys.has(edgeKey)) {
        edgeKeys.add(edgeKey);
        edges.push({
          data: {
            id: edgeKey,
            source: nId,
            target: mId,
            label: relType,
          },
        });
      }
    }

    return NextResponse.json({
      nodes: Array.from(nodesById.values()),
      edges,
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  } finally {
    await session.close();
  }
}

