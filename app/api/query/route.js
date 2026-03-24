import { NextResponse } from "next/server";
import { getNeo4jSession } from "@/lib/neo4j";

export const runtime = "nodejs";

const ALLOWED_KEYWORDS = [
  "order",
  "delivery",
  "invoice",
  "billing",
  "payment",
  "journal",
  "customer",
  "product",
  "flow",
  "sales",
];
const ALLOWED_NODE_LABELS = [
  "Order",
  "OrderItem",
  "Customer",
  "Delivery",
  "Invoice",
  "Payment",
  "Product",
];
const ALLOWED_RELATIONSHIP_TYPES = [
  "PLACED_BY",
  "HAS_DELIVERY",
  "HAS_ITEM",
  "FOR_PRODUCT",
  "BILLED_IN",
  "PAID_BY",
];
const GEMINI_DEFAULT_MODEL = "gemini-2.0-flash";
const GEMINI_FALLBACK_MODELS = ["gemini-2.0-flash"];

function normalizeNeo4jPropertyValue(value) {
  if (value === null || value === undefined) return undefined;
  const t = typeof value;
  if (t === "string" || t === "number" || t === "boolean") return value;
  if (t === "bigint") return value.toString();
  // Avoid returning arbitrary objects (Cytoscape/JSON can't store those directly).
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

function normalizeProperties(properties) {
  const out = {};
  if (!properties) return out;
  for (const [k, v] of Object.entries(properties)) {
    const normalized = normalizeNeo4jPropertyValue(v);
    if (normalized !== undefined) out[k] = normalized;
  }
  return out;
}

function toId(value) {
  if (value === null || value === undefined) return "";
  if (typeof value === "string" || typeof value === "number") return String(value);
  if (typeof value === "bigint") return value.toString();
  const maybeToString = value?.toString;
  if (typeof maybeToString === "function") return maybeToString.call(value);
  return "";
}

function normalizeValue(value) {
  if (value === null || value === undefined) return null;
  const t = typeof value;
  if (t === "string" || t === "number" || t === "boolean") return value;
  if (t === "bigint") return value.toString();

  // Neo4j Node / Relationship-like objects.
  if (typeof value === "object") {
    if (value.labels && value.identity !== undefined) {
      const labels = Array.isArray(value.labels) ? value.labels.map(String) : [];
      return {
        id: toId(value.identity),
        labels,
        properties: normalizeProperties(value.properties ?? {}),
      };
    }
    if (value.type && value.identity !== undefined) {
      return {
        id: toId(value.identity),
        type: String(value.type),
        properties: normalizeProperties(value.properties ?? {}),
      };
    }
  }

  try {
    return JSON.parse(JSON.stringify(value));
  } catch {
    return String(value);
  }
}

function buildGroundedAnswer(data) {
  const rows = Array.isArray(data) ? data : [];
  if (rows.length === 0) {
    return "No matching records were found in the dataset.";
  }

  // Keep this deterministic and grounded to result rows only.
  if (rows.length === 1) {
    const row = rows[0];
    const preview = JSON.stringify(row);
    if (preview.length <= 220) {
      return `Found 1 matching record: ${preview}`;
    }
    return "Found 1 matching record in the dataset.";
  }

  return `Found ${rows.length} matching records in the dataset.`;
}

function collectNodeIdsFromDataRows(rows) {
  const out = new Set();
  for (const row of rows) {
    if (!row || typeof row !== "object") continue;
    for (const value of Object.values(row)) {
      // If query returns raw node objects (normalized in normalizeValue)
      if (value && typeof value === "object" && typeof value.id === "string" && value.labels) {
        out.add(String(value.id));
      }
      if (Array.isArray(value)) {
        for (const v of value) {
          if (v && typeof v === "object" && typeof v.id === "string" && v.labels) {
            out.add(String(v.id));
          }
        }
      }
    }
  }
  return out;
}

async function resolveHighlightNodeIds(session, dataRows, question) {
  const ids = collectNodeIdsFromDataRows(dataRows);
  if (ids.size > 0) return Array.from(ids);

  // Fallback: if result is scalar-only, try to map id-like token in question to graph nodes.
  const token = extractLikelyId(question);
  if (!token) return [];

  const fallback = await session.run(
    `
      MATCH (n)
      WHERE
        (n:Order AND toString(n.orderId) = $token) OR
        (n:Customer AND toString(n.customerId) = $token) OR
        (n:Delivery AND toString(n.deliveryId) = $token) OR
        (n:Invoice AND toString(n.invoiceId) = $token) OR
        (n:Payment AND toString(n.paymentId) = $token) OR
        (n:Product AND toString(n.productId) = $token)
      OPTIONAL MATCH (n)-[]-(m)
      RETURN collect(DISTINCT id(n)) + collect(DISTINCT id(m)) AS ids
      LIMIT 1
    `,
    { token }
  );

  const list = fallback.records[0]?.get("ids");
  if (!Array.isArray(list)) return [];
  return list.map((v) => toId(v)).filter(Boolean);
}

function extractCypher(text) {
  const t = String(text ?? "").trim();
  if (!t) return "";

  // Prefer fenced blocks.
  const fenced = t.match(/```(?:cypher)?\s*([\s\S]*?)```/i);
  if (fenced?.[1]) return fenced[1].trim();

  // Fallback: return whole text.
  return t;
}

function extractLikelyId(question) {
  // Tries to capture ids like 90504204 / 80738040 / 9400000220 / S8907367001003
  const matches = String(question ?? "").match(/[A-Za-z]?[A-Za-z0-9_-]{6,}/g) ?? [];
  if (matches.length === 0) return null;
  return matches[matches.length - 1];
}

function buildFallbackCypherFromQuestion(question) {
  const q = String(question ?? "").toLowerCase();
  const id = extractLikelyId(question);

  if (id && (q.includes("billing") || q.includes("invoice"))) {
    return {
      cypher: `
        MATCH (i:Invoice { invoiceId: $id })
        OPTIONAL MATCH (d:Delivery)-[:BILLED_IN]->(i)
        OPTIONAL MATCH (o:Order)-[:HAS_DELIVERY]->(d)
        OPTIONAL MATCH (o)-[:PLACED_BY]->(c:Customer)
        OPTIONAL MATCH (i)-[:PAID_BY]->(p:Payment)
        RETURN i AS invoice, d AS delivery, o AS order, c AS customer, p AS payment
        LIMIT 50
      `,
      params: { id },
    };
  }

  if (id && (q.includes("salesorder") || q.includes("sales order") || q.includes("order"))) {
    return {
      cypher: `
        MATCH (o:Order { orderId: $id })
        OPTIONAL MATCH (o)-[:PLACED_BY]->(c:Customer)
        OPTIONAL MATCH (o)-[:HAS_DELIVERY]->(d:Delivery)
        OPTIONAL MATCH (d)-[:BILLED_IN]->(i:Invoice)
        OPTIONAL MATCH (i)-[:PAID_BY]->(p:Payment)
        RETURN o AS order, c AS customer, collect(DISTINCT d) AS deliveries, collect(DISTINCT i) AS invoices, collect(DISTINCT p) AS payments
        LIMIT 1
      `,
      params: { id },
    };
  }

  if (id && q.includes("customer")) {
    return {
      cypher: `
        MATCH (c:Customer { customerId: $id })
        OPTIONAL MATCH (o:Order)-[:PLACED_BY]->(c)
        OPTIONAL MATCH (o)-[:HAS_DELIVERY]->(d:Delivery)
        OPTIONAL MATCH (d)-[:BILLED_IN]->(i:Invoice)
        RETURN c AS customer, collect(DISTINCT o) AS orders, collect(DISTINCT d) AS deliveries, collect(DISTINCT i) AS invoices
        LIMIT 1
      `,
      params: { id },
    };
  }

  if (q.includes("highest") && q.includes("billing") && q.includes("product")) {
    return {
      cypher: `
        MATCH (p:Product)<-[:FOR_PRODUCT]-(:OrderItem)<-[:HAS_ITEM]-(:Order)-[:HAS_DELIVERY]->(:Delivery)-[:BILLED_IN]->(i:Invoice)
        RETURN p, count(DISTINCT i.invoiceId) AS billingDocumentCount
        ORDER BY billingDocumentCount DESC
        LIMIT 20
      `,
      params: {},
    };
  }

  return null;
}

function isQueryAllowed(cypher) {
  const c = String(cypher ?? "");
  if (!c.trim()) return false;

  // Read-only guardrails.
  const forbidden = [
    "CREATE",
    "MERGE",
    "DELETE",
    "SET ",
    "REMOVE",
    "DROP",
    "CALL",
    "LOAD",
    "TRUNCATE",
    "ALTER",
    "INSERT",
    "UPDATE",
    "GRANT",
    "REVOKE",
    "CREATE INDEX",
  ];
  const upper = c.toUpperCase();
  if (forbidden.some((k) => upper.includes(k))) return false;

  // Extract node labels only from node patterns like `(n:Order)` or `(:Order)`.
  // This avoids accidentally matching relationship types like `[:PLACED_BY]`.
  const labelMatches = Array.from(
    c.matchAll(/\(\s*(?:[A-Za-z_][A-Za-z0-9_]*\s*)?:\s*([A-Za-z_][A-Za-z0-9_]*)/g)
  ).map((m) => m[1]);
  const usedLabels = new Set(labelMatches);
  for (const l of usedLabels) {
    if (!ALLOWED_NODE_LABELS.includes(l)) return false;
  }

  const relMatches = Array.from(c.matchAll(/\[:\s*([A-Za-z0-9_]+)/g)).map((m) => m[1]);
  const allRelTypes = new Set(relMatches.map(String));
  for (const rt of allRelTypes) {
    if (!ALLOWED_RELATIONSHIP_TYPES.includes(rt)) return false;
  }

  // Basic sanity: must be a MATCH/RETURN query.
  if (!upper.includes("MATCH")) return false;
  if (!upper.includes("RETURN")) return false;

  return usedLabels.size > 0 || allRelTypes.size > 0;
}

async function callGeminiGenerate(model, apiKey, prompt) {
  const url = `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${apiKey}`;
  const body = {
    contents: [{ parts: [{ text: prompt }] }],
    generationConfig: {
      temperature: 0,
      maxOutputTokens: 256,
    },
  };

  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });

  const textBody = await res.text();
  let payload = null;
  try {
    payload = JSON.parse(textBody);
  } catch {
    // keep raw text in error path
  }

  if (!res.ok) {
    const message = payload ? JSON.stringify(payload) : textBody;
    const error = new Error(`Gemini request failed (${model}): ${res.status} ${message}`);
    error.status = res.status;
    throw error;
  }

  const text =
    payload?.candidates?.[0]?.content?.parts?.[0]?.text ??
    payload?.candidates?.[0]?.content?.parts?.[0]?.inlineData?.data ??
    "";

  return extractCypher(text);
}

async function callGeminiToCypher(question) {
  const apiKey = process.env.GEMINI_API_KEY;
  if (!apiKey) {
    throw new Error("Missing GEMINI_API_KEY env var");
  }

  const configuredModel = process.env.GEMINI_MODEL ?? GEMINI_DEFAULT_MODEL;

  const prompt = `Convert the following natural language question into a Cypher query for Neo4j.

Rules:
- Use only these node types: Order, Customer, Delivery, Invoice, Payment, Product
- Use relationships:
  (Order)-[:PLACED_BY]->(Customer)
  (Order)-[:HAS_DELIVERY]->(Delivery)
  (Delivery)-[:BILLED_IN]->(Invoice)
  (Invoice)-[:PAID_BY]->(Payment)
- Prefer read-only aggregations and path tracing.
- When possible, return relevant nodes or paths so UI can highlight matched graph entities.
- Return ONLY the Cypher query
- Do NOT explain anything

Question:
${question}`;

  // Try configured model first. If unavailable, fall back automatically.
  const modelsToTry = [configuredModel, ...GEMINI_FALLBACK_MODELS.filter((m) => m !== configuredModel)];
  let lastError = null;

  for (const model of modelsToTry) {
    try {
      const cypher = await callGeminiGenerate(model, apiKey, prompt);
      if (cypher.trim()) return cypher;
    } catch (err) {
      lastError = err;
      // Retry on model-not-found / transient server errors.
      const status = err?.status;
      if (status !== 404 && status !== 429 && status !== 500 && status !== 503) {
        break;
      }
    }
  }

  throw lastError ?? new Error("Failed to generate Cypher query from Gemini.");
}

export async function POST(req) {
  try {
    const body = await req.json();
    const question = typeof body?.question === "string" ? body.question.trim() : "";

    if (!question) {
      return NextResponse.json({ error: "Missing question" }, { status: 400 });
    }

    const qLower = question.toLowerCase();
    const allowed = ALLOWED_KEYWORDS.some((k) => qLower.includes(k));

    if (!allowed) {
      return NextResponse.json({
        answer: "This system is designed to answer questions related to the dataset only.",
        cypher: null,
        data: [],
        highlightNodeIds: [],
      });
    }

    let cypher = "";
    let params = {};
    try {
      cypher = await callGeminiToCypher(question);
    } catch {
      const fallback = buildFallbackCypherFromQuestion(question);
      if (!fallback) {
        return NextResponse.json(
          {
            answer:
              "I could not translate that into a supported dataset query right now. Please rephrase using order, delivery, invoice, payment, customer, or product terms.",
            cypher: null,
            data: [],
            highlightNodeIds: [],
          },
          { status: 200 }
        );
      }
      cypher = fallback.cypher;
      params = fallback.params;
    }

    // Normalize output.
    cypher = cypher.replace(/```/g, "").trim();
    cypher = cypher.replace(/;\s*$/, "");

    if (!isQueryAllowed(cypher)) {
      return NextResponse.json({
        answer:
          "I could not translate that into a supported dataset query right now. Please rephrase using order, delivery, invoice, payment, customer, or product terms.",
        cypher: null,
        data: [],
        highlightNodeIds: [],
      });
    }

    const session = getNeo4jSession();
    try {
      const result = await session.run(cypher, params);
      const keys = Array.isArray(result?.records?.[0]?.keys)
        ? result.records[0].keys
        : [];
      const data = result.records.map((rec) => {
        const obj = {};
        for (let i = 0; i < keys.length; i += 1) {
          const key = keys[i];
          obj[key] = normalizeValue(rec.get(key));
        }
        return obj;
      });

      const answer = buildGroundedAnswer(data);
      const highlightNodeIds = await resolveHighlightNodeIds(session, data, question);
      return NextResponse.json({ answer, cypher, data, highlightNodeIds });
    } finally {
      await session.close();
    }
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

