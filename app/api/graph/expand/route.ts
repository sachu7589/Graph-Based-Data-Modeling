import { NextResponse } from "next/server";
import { getNeo4jSession } from "@/lib/neo4j";

type CytoscapeNode = { data: Record<string, unknown> };
type CytoscapeEdge = { data: { id: string; source: string; target: string; label: string } };

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

export async function GET(req: Request) {
  const session = getNeo4jSession();

  try {
    const url = new URL(req.url);
    const nodeId = url.searchParams.get("nodeId") ?? "";
    const limit = Math.max(1, Math.min(500, Number(url.searchParams.get("limit") ?? "100")));

    if (!nodeId) {
      return NextResponse.json({ error: "Missing nodeId" }, { status: 400 });
    }

    const nodesById = new Map<string, CytoscapeNode>();
    const edges: CytoscapeEdge[] = [];
    const edgeKeys = new Set<string>();

    const outgoing = await session.run(
      "MATCH (n)-[r]->(m) WHERE id(n) = toInteger($nodeId) RETURN n, r, m LIMIT $limit",
      { nodeId, limit }
    );

    for (const record of outgoing.records) {
      const n = record.get("n") as Neo4jElementLike | null | undefined;
      const r = record.get("r") as Neo4jRelationshipLike | null | undefined;
      const m = record.get("m") as Neo4jElementLike | null | undefined;

      const nId = toId(n?.identity);
      const mId = toId(m?.identity);
      if (!nId || !mId) continue;

      const nLabels: unknown[] = Array.isArray(n?.labels) ? (n!.labels as unknown[]) : [];
      const mLabels: unknown[] = Array.isArray(m?.labels) ? (m!.labels as unknown[]) : [];

      const nLabel = nLabels.length ? String(nLabels[0]) : "Node";
      const mLabel = mLabels.length ? String(mLabels[0]) : "Node";

      if (!nodesById.has(nId)) {
        nodesById.set(nId, {
          data: { ...normalizeProperties(n?.properties ?? {}), id: nId, label: nLabel },
        });
      }
      if (!nodesById.has(mId)) {
        nodesById.set(mId, {
          data: { ...normalizeProperties(m?.properties ?? {}), id: mId, label: mLabel },
        });
      }

      const relType =
        r?.type !== undefined && r?.type !== null ? String(r.type) : "RELATED_TO";
      const edgeKey = `${nId}->${mId}:${relType}`;
      if (!edgeKeys.has(edgeKey)) {
        edgeKeys.add(edgeKey);
        edges.push({
          data: { id: edgeKey, source: nId, target: mId, label: relType },
        });
      }
    }

    const incoming = await session.run(
      "MATCH (n)<-[r]-(m) WHERE id(n) = toInteger($nodeId) RETURN m, r, n LIMIT $limit",
      { nodeId, limit }
    );

    for (const record of incoming.records) {
      // Here record is: m -[r]-> n
      const m = record.get("m") as Neo4jElementLike | null | undefined;
      const r = record.get("r") as Neo4jRelationshipLike | null | undefined;
      const n = record.get("n") as Neo4jElementLike | null | undefined;

      const mId = toId(m?.identity);
      const nId = toId(n?.identity);
      if (!mId || !nId) continue;

      const mLabels: unknown[] = Array.isArray(m?.labels) ? (m!.labels as unknown[]) : [];
      const nLabels: unknown[] = Array.isArray(n?.labels) ? (n!.labels as unknown[]) : [];

      const mLabel = mLabels.length ? String(mLabels[0]) : "Node";
      const nLabel = nLabels.length ? String(nLabels[0]) : "Node";

      if (!nodesById.has(mId)) {
        nodesById.set(mId, {
          data: { ...normalizeProperties(m?.properties ?? {}), id: mId, label: mLabel },
        });
      }
      if (!nodesById.has(nId)) {
        nodesById.set(nId, {
          data: { ...normalizeProperties(n?.properties ?? {}), id: nId, label: nLabel },
        });
      }

      const relType =
        r?.type !== undefined && r?.type !== null ? String(r.type) : "RELATED_TO";
      const edgeKey = `${mId}->${nId}:${relType}`;
      if (!edgeKeys.has(edgeKey)) {
        edgeKeys.add(edgeKey);
        edges.push({
          data: { id: edgeKey, source: mId, target: nId, label: relType },
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

