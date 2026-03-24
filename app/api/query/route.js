import { NextResponse } from "next/server";
import { getNeo4jSession } from "@/lib/neo4j";
import fs from "fs";
import path from "path";
import OpenAI from "openai";

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
const GROQ_DEFAULT_MODEL = "openai/gpt-oss-20b";
const GROQ_FALLBACK_MODELS = ["openai/gpt-oss-20b"];

function getGroqClient() {
  const apiKey = process.env.GROQ_API_KEY;
  if (!apiKey) throw new Error("Missing GROQ_API_KEY env var");

  const g = globalThis;
  const currentKey = g.__groqApiKey;
  if (!g.__groqClient || currentKey !== apiKey) {
    g.__groqClient = new OpenAI({
      apiKey,
      baseURL: "https://api.groq.com/openai/v1",
    });
    g.__groqApiKey = apiKey;
  }
  return g.__groqClient;
}

async function getDatasetFieldHints() {
  const g = globalThis;
  if (g.__datasetFieldHints) return g.__datasetFieldHints;

  const root = process.cwd();
  const datasetDir = path.join(root, "dataset");
  const hints = [];

  try {
    const folders = await fs.promises.readdir(datasetDir, { withFileTypes: true });
    for (const folder of folders) {
      if (!folder.isDirectory()) continue;
      const folderPath = path.join(datasetDir, folder.name);
      const files = await fs.promises.readdir(folderPath, { withFileTypes: true });
      const jsonl = files.filter((f) => f.isFile() && f.name.endsWith(".jsonl")).slice(0, 2);

      const fieldSet = new Set();
      for (const f of jsonl) {
        const full = path.join(folderPath, f.name);
        const text = await fs.promises.readFile(full, "utf8");
        const line = text.split(/\r?\n/).find((l) => l.trim().length > 0);
        if (!line) continue;
        try {
          const obj = JSON.parse(line);
          for (const k of Object.keys(obj)) fieldSet.add(k);
        } catch {
          // ignore bad sample lines
        }
      }

      const fields = Array.from(fieldSet).slice(0, 20);
      if (fields.length > 0) {
        hints.push(`${folder.name}: ${fields.join(", ")}`);
      }
    }
  } catch {
    // If dataset scan fails, we can still continue with schema-only prompt.
  }

  const fieldHints = hints.slice(0, 40).join("\n");
  g.__datasetFieldHints = fieldHints;
  return fieldHints;
}

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

function sanitizeCypher(cypher) {
  let out = String(cypher ?? "").replace(/```/g, "").trim();
  out = out.replace(/;\s*$/, "");
  return out;
}

function extractLikelyId(question) {
  // Tries to capture ids like 90504204 / 80738040 / 9400000220 / S8907367001003.
  // Important: require at least one digit so words like "highest" are not treated as IDs.
  const matches = String(question ?? "").match(/[A-Za-z]?[A-Za-z0-9_-]{6,}/g) ?? [];
  const withDigit = matches.filter((m) => /\d/.test(m));
  if (withDigit.length === 0) return null;
  return withDigit[withDigit.length - 1];
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

async function callGroqGenerate(model, prompt) {
  const client = getGroqClient();
  try {
    const response = await client.responses.create({
      model,
      input: prompt,
    });
    return extractCypher(response.output_text ?? "");
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    const e = new Error(`Groq request failed (${model}): ${message}`);
    e.status = err?.status;
    throw e;
  }
}

async function callGroqToCypher(question, datasetFieldHints) {
  const configuredModel = process.env.GROQ_MODEL ?? GROQ_DEFAULT_MODEL;

  const prompt = `Convert the following natural language question into a Cypher query for Neo4j.

Rules:
- Use only these node types: Order, OrderItem, Customer, Delivery, Invoice, Payment, Product
- Use relationships:
  (Order)-[:PLACED_BY]->(Customer)
  (Order)-[:HAS_DELIVERY]->(Delivery)
  (Order)-[:HAS_ITEM]->(OrderItem)
  (OrderItem)-[:FOR_PRODUCT]->(Product)
  (Delivery)-[:BILLED_IN]->(Invoice)
  (Invoice)-[:PAID_BY]->(Payment)
- Prefer read-only aggregations and path tracing.
- When possible, return relevant nodes or paths so UI can highlight matched graph entities.
- Common id properties:
  Order.orderId, Customer.customerId, Delivery.deliveryId, Invoice.invoiceId, Payment.paymentId, Product.productId
- Common payment amount properties:
  Payment.amountInTransactionCurrency, Payment.amountInCompanyCodeCurrency
- Use OPTIONAL MATCH when links may be missing.
- Return ONLY the Cypher query
- Do NOT explain anything

Question:
${question}

Dataset fields (discovered from /dataset samples; use these names when relevant):
${datasetFieldHints || "N/A"}`;

  // Try configured model first. If unavailable, fall back automatically.
  const modelsToTry = [configuredModel, ...GROQ_FALLBACK_MODELS.filter((m) => m !== configuredModel)];
  let lastError = null;

  for (const model of modelsToTry) {
    try {
      const cypher = await callGroqGenerate(model, prompt);
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

  throw lastError ?? new Error("Failed to generate Cypher query from Groq.");
}

async function repairCypherWithGroq(question, badCypher, failureReason) {
  const configuredModel = process.env.GROQ_MODEL ?? GROQ_DEFAULT_MODEL;
  const prompt = `Fix this Cypher query for Neo4j.

User question:
${question}

Broken query:
${badCypher}

Failure:
${failureReason}

Rules:
- Output ONLY corrected Cypher
- Read-only query only (MATCH/OPTIONAL MATCH/RETURN/WITH/ORDER BY/LIMIT)
- Use only labels: Order, OrderItem, Customer, Delivery, Invoice, Payment, Product
- Use only relationships: PLACED_BY, HAS_DELIVERY, HAS_ITEM, FOR_PRODUCT, BILLED_IN, PAID_BY
`;

  const modelsToTry = [configuredModel, ...GROQ_FALLBACK_MODELS.filter((m) => m !== configuredModel)];
  for (const model of modelsToTry) {
    try {
      const repaired = await callGroqGenerate(model, prompt);
      const s = sanitizeCypher(repaired);
      if (s) return s;
    } catch {
      // continue
    }
  }
  return null;
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

    const datasetFieldHints = await getDatasetFieldHints();
    let cypher = "";
    const params = {};
    let translatedBy = "groq";

    // Groq-first NLP translation for all dataset questions.
    try {
      cypher = await callGroqToCypher(question, datasetFieldHints);
      cypher = sanitizeCypher(cypher);
    } catch (err) {
      const reason = err instanceof Error ? err.message : String(err);
      return NextResponse.json(
        {
          answer:
            `I could not translate that query via Groq right now. ${reason}`,
          cypher: null,
          data: [],
          highlightNodeIds: [],
          translatedBy: "none",
        },
        { status: 200 }
      );
    }

    // Normalize output.
    cypher = sanitizeCypher(cypher);

    if (!isQueryAllowed(cypher)) {
      // Try one LLM repair attempt before falling back.
      if (translatedBy === "groq") {
        const repaired = await repairCypherWithGroq(
          question,
          cypher,
          "Guardrail validation failed (unsafe labels/relationships or non-read-only query)"
        );
        if (repaired && isQueryAllowed(repaired)) {
          cypher = repaired;
        }
      }
    }

    if (!isQueryAllowed(cypher)) {
      return NextResponse.json({
        answer:
          "The generated query was outside the allowed dataset graph scope. Please rephrase your question.",
        cypher: null,
        data: [],
        highlightNodeIds: [],
        translatedBy: "none",
      });
    }

    const session = getNeo4jSession();
    try {
      let result;
      try {
        result = await session.run(cypher, params);
      } catch (queryErr) {
        // One repair attempt on Neo4j execution errors when source was Groq.
        if (translatedBy === "groq") {
          const reason = queryErr instanceof Error ? queryErr.message : String(queryErr);
          const repaired = await repairCypherWithGroq(question, cypher, reason);
          if (repaired && isQueryAllowed(repaired)) {
            cypher = repaired;
            result = await session.run(cypher, params);
          } else {
            const reason = queryErr instanceof Error ? queryErr.message : String(queryErr);
            return NextResponse.json({
              answer:
                `The generated query could not be executed on the dataset. ${reason}`,
              cypher: null,
              data: [],
              highlightNodeIds: [],
              translatedBy: "none",
            });
          }
        } else {
          const reason = queryErr instanceof Error ? queryErr.message : String(queryErr);
          return NextResponse.json({
            answer:
              `The generated query could not be executed on the dataset. ${reason}`,
            cypher: null,
            data: [],
            highlightNodeIds: [],
            translatedBy: "none",
          });
        }
      }
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
      return NextResponse.json({ answer, cypher, data, highlightNodeIds, translatedBy });
    } finally {
      await session.close();
    }
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

