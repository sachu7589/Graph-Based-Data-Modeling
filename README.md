## Graph Query System (Neo4j + Cytoscape + NL->Cypher via Groq)

This repo is a Next.js (App Router) app that:
1. Imports an order-to-cash dataset (JSONL) into Neo4j using an idempotent Cypher-based import script.
2. Visualizes the graph in the browser with Cytoscape (expand nodes + highlight matched entities).
3. Provides a chat UI where natural language questions are converted to Cypher (NL->Cypher) by Groq, executed against Neo4j, and returned as grounded results (not fabricated).

The intended workflow is:
`User question -> Groq generates Cypher (guarded) -> Neo4j executes Cypher -> UI shows results + highlights related nodes.`

---

## Graph Model

The importer builds an order-to-cash style graph. The query system is constrained to the core labels/relationships below.

### Core node labels (used by `/api/query`)
- `Order`
- `OrderItem`
- `Customer`
- `Delivery`
- `Invoice`
- `Payment`
- `Product`

### Core relationship types (used by `/api/query`)
- `(Order)-[:PLACED_BY]->(Customer)`
- `(Order)-[:HAS_DELIVERY]->(Delivery)`
- `(Order)-[:HAS_ITEM]->(OrderItem)`
- `(OrderItem)-[:FOR_PRODUCT]->(Product)`
- `(Delivery)-[:BILLED_IN]->(Invoice)`
- `(Invoice)-[:PAID_BY]->(Payment)`

The graph visualization endpoint (`/api/graph`) is not constrained to only these types; it returns whatever relationships Neo4j has (the query/chat highlighting is constrained by guardrails).

---

## Neo4j Setup

The app expects a live Neo4j instance reachable by `neo4j-driver`.

### Environment variables
Create a `.env.local` (based on `.env.example`) with:

- `NEO4J_URI` (e.g. `bolt://localhost:7687`)
- `NEO4J_USERNAME` (or `NEO4J_USER`)
- `NEO4J_PASSWORD`
- `NEO4J_DATABASE` (optional; used for multi-database Neo4j)

For the Groq-backed NL->Cypher:
- `GROQ_API_KEY`
- `GROQ_MODEL` (optional; defaults to `openai/gpt-oss-20b` in the code)

### Where the Neo4j driver is configured
- `lib/neo4j.ts` centralizes `neo4j-driver` connection + sessions.
- The driver uses `disableLosslessIntegers: true` to avoid Neo4j Integer serialization issues in the runtime.

---

## Dataset Import (JSONL -> Neo4j)

The dataset is expected at `./dataset/`.

### Dataset layout
- `dataset/` contains multiple folders.
- Each folder contains one or more `.jsonl` files.
- Each `.jsonl` file contains many JSON objects (one per line).

### Import script
The actual importer implementation is:
- `scripts/import_v2.js`

There is also `scripts/import.js` in the repo, but `import_v2.js` is the clean/primary implementation to run.

#### Run the importer
```bash
node scripts/import_v2.js
```

#### Import configuration
You can optionally set:
- `DATASET_DIR` (absolute or relative path to the dataset root; default: `./dataset`)
- `IMPORT_BATCH_SIZE` (default `2000`)
- `MAX_ERROR_SAMPLES` (default `5`)

The importer is designed to be idempotent via Cypher `MERGE`, and it uses batching (Cypher `UNWIND`-style) and streaming JSONL parsing to handle large files.

---

## Graph Visualization (Cytoscape)

The frontend fetches graph data from these Next.js API routes:

### `GET /api/graph`
Returns the initial Cytoscape elements:
- `nodes: [{ data: { ...properties, id, label } }]`
- `edges: [{ data: { id, source, target, label } }]`

Implementation details:
- It queries a limited neighborhood from Neo4j (`MATCH (n)-[r]->(m) ... LIMIT 200`).
- Node/edge properties are normalized into JSON-safe values for the UI.
- Edges include a stable `id` derived from `source`, `target`, and relationship type.

### `GET /api/graph/expand?nodeId=...&limit=...`
Expands around a clicked node:
- Uses Neo4j internal identity (`id(n)`) matching the clicked node’s `data.id`.
- Fetches both outgoing and incoming relationships.

---

## Chat Query System (Groq NL->Cypher -> Neo4j -> Grounded Answer)

The NL->Cypher pipeline lives in:
- `app/api/query/route.js`

### `POST /api/query`
Request body:
```json
{ "question": "your natural language question" }
```

Response shape:
- `answer: string` (grounded summary derived from Neo4j result rows only)
- `cypher: string | null` (generated Cypher, if available)
- `data: Array<object>` (raw Neo4j result rows normalized into JSON-safe values)
- `highlightNodeIds: string[]` (Neo4j internal IDs used by the UI to highlight nodes)
- `translatedBy: "groq" | "none"`

### Guardrails (VERY IMPORTANT)
Before calling Groq, the route checks the question:
- It must be dataset-related (based on allowed keyword list such as `order`, `delivery`, `invoice`, `payment`, `customer`, `product`, etc).
- If not allowed, it returns a friendly message and an empty `data` array.

After Groq generates Cypher, the route validates it via `isQueryAllowed()`:
- Must be read-only (only `MATCH/OPTIONAL MATCH/RETURN/WITH/ORDER BY/LIMIT` patterns).
- Must only use allowed labels: `Order`, `OrderItem`, `Customer`, `Delivery`, `Invoice`, `Payment`, `Product`.
- Must only use allowed relationship types: `PLACED_BY`, `HAS_DELIVERY`, `HAS_ITEM`, `FOR_PRODUCT`, `BILLED_IN`, `PAID_BY`.
- Must include `MATCH` and `RETURN`.

### Groq translation prompt + dataset field hints
The Groq prompt includes:
- Hard rules (node labels, relationship types, and output format: return ONLY Cypher).
- “Common id properties” to steer Cypher generation:
  - `Order.orderId`, `Customer.customerId`, `Delivery.deliveryId`, `Invoice.invoiceId`, `Payment.paymentId`, `Product.productId`
- “Dataset fields (discovered from /dataset samples)”:
  - The code scans `dataset/*/*.jsonl` and extracts the first non-empty JSON line keys (up to a small sample window).
  - These field names are injected into the prompt so Groq can reference real dataset property names.

### Self-healing repair loop
There are two repair attempts:
1. If the generated Cypher fails guardrail validation, Groq is asked to “fix this Cypher” using the guardrails again.
2. If the Cypher executes but Neo4j errors, Groq is asked to repair the query based on the Neo4j failure reason.

If repair still fails, the API returns `cypher: null`, `data: []`, and `translatedBy: "none"` with a diagnostic message.

### Grounded answer logic
`buildGroundedAnswer(data)` only summarizes the number of records / small preview of the first row.
If there are no results, it returns:
`No matching records were found in the dataset.`

### Highlighting matched nodes
`resolveHighlightNodeIds()` tries to extract `id/labels`-like structures from the Neo4j result rows.
If that fails (scalar-only results), it extracts a likely id token from the question (digits-only heuristic) and runs a fallback Neo4j lookup across the domain id properties (orderId/customerId/etc) to map it back to Neo4j internal identities.

---

## Groq Health Endpoint

`GET /api/query/health`

This endpoint checks:
- Whether `GROQ_API_KEY` is present
- Whether Groq calls work (a tiny `responses.create` test)

Useful for quick debugging when the chat returns “Groq request failed” or “Missing GROQ_API_KEY”.

---

## Frontend UI

The root page renders a split view:
- Graph area: ~70%
- Chat area: ~30%

Key components:

### `src/components/GraphWithChat.js`
- Split layout + “Granular Overlay” toggle + “Minimize” button for chat.
- Owns `highlightNodeIds`, derived from `/api/query` results.
- Shows a floating card on the graph with either:
  - clicked node details, or
  - the top match row (and it is structured to show multiple key/value rows).

### `src/components/Graph.js`
- Cytoscape setup + screenshot-like styling:
  - tiny dots for nodes
  - thin light-blue arrow edges
  - no clutter labels
- Node click behavior:
  - sets the selected node
  - expands the graph around it via `/api/graph/expand`
  - applies highlight styles based on `highlightNodeIds`

### `src/components/ChatBox.js`
- Input + send button
- Posts `question` to `/api/query`
- Displays per-turn output:
  - user question
  - generated `cypher`
  - `data` (Neo4j rows)
  - a grounded `answer`

---

## Running Locally

### 1. Install dependencies
```bash
npm install
```

### 2. Configure `.env.local`
Start from `.env.example` and fill:
- `NEO4J_*`
- `GROQ_API_KEY` and (optionally) `GROQ_MODEL`

### 3. Import the dataset (one-time)
```bash
node scripts/import_v2.js
```

### 4. Start the app
```bash
npm run dev
```

Open:
- `http://localhost:3000`

You can also verify Groq connectivity with:
- `GET http://localhost:3000/api/query/health`

---

## Supported Question Types (Examples)

The `/api/query` route accepts dataset-related queries. Examples that fit the guardrails:
- “Which products have the highest number of billing documents?”
- “Trace billing document 90504204 flow”
- “Show incomplete flows (delivered not billed, billed without delivery)”
- “details of customer 310000108”
- “sale order of id 740509”

If the question is outside the allowed dataset domain, the API returns:
`This system is designed to answer questions related to the dataset only.`

---

## Troubleshooting Notes

### Neo4j serialization / Integer issues
If you see errors related to Neo4j integers (or Cytoscape crashes):
- The Neo4j driver config already uses `disableLosslessIntegers: true` in both `lib/neo4j.ts` and the importer driver wrapper.

### `/api/query/health` fails
Common causes:
- missing/blank `GROQ_API_KEY`
- wrong `GROQ_MODEL`

The health endpoint will include an error sample to help pinpoint which step failed.
