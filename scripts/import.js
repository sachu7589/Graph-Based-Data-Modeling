// Wrapper kept for backwards compatibility.
// Run: `node scripts/import.js` (delegates to import_v2.js)
import "./import_v2.js";

console.log("import placeholder");

// Usage: node scripts/import.js
// Requires ESM (package.json "type":"module")

import fs from "fs";
import path from "path";
import readline from "readline";
import { fileURLToPath } from "url";

import driver from "../src/lib/neo4j.js";

const DATASET_DIR = process.env.DATASET_DIR
  ? path.resolve(process.env.DATASET_DIR)
  : (() => {
      const scriptsDir = path.dirname(fileURLToPath(import.meta.url));
      const projectRoot = path.resolve(scriptsDir, "..");
      return path.join(projectRoot, "dataset");
    })();

const BATCH_SIZE = Number.parseInt(process.env.IMPORT_BATCH_SIZE ?? "2000", 10);
const MAX_ERROR_SAMPLES = Number.parseInt(process.env.MAX_ERROR_SAMPLES ?? "5", 10);

function sanitizeValue(value) {
  if (value === null || value === undefined) return undefined;
  if (typeof value === "string") return value;
  if (typeof value === "number") return value;
  if (typeof value === "boolean") return value;
  return JSON.stringify(value);
}

function sanitizeRecord(record) {
  const out = {};
  for (const [k, v] of Object.entries(record)) {
    const sanitized = sanitizeValue(v);
    if (sanitized !== undefined) out[k] = sanitized;
  }
  return out;
}

async function buildDatasetFileMap(rootDir) {
  const entries = await fs.promises.readdir(rootDir, { withFileTypes: true });
  const map = new Map(); // folderName => string[]

  for (const entry of entries) {
    if (!entry.isDirectory()) continue;
    const folderName = entry.name;
    const folderPath = path.join(rootDir, folderName);
    const files = await fs.promises.readdir(folderPath, { withFileTypes: true });
    const jsonlFiles = files
      .filter((f) => f.isFile() && f.name.endsWith(".jsonl"))
      .map((f) => path.join(folderPath, f.name));

    if (jsonlFiles.length > 0) map.set(folderName, jsonlFiles);
  }

  return map;
}

async function* readJsonlFileInBatches(filePath, { batchSize, mapRecord }) {
  const rl = readline.createInterface({
    input: fs.createReadStream(filePath, { encoding: "utf8" }),
    crlfDelay: Infinity,
  });

  let batch = [];
  let lineNumber = 0;
  const errors = [];

  for await (const line of rl) {
    lineNumber += 1;
    const trimmed = line.trim();
    if (!trimmed) continue;

    let parsed;
    try {
      parsed = JSON.parse(trimmed);
    } catch (e) {
      if (errors.length < MAX_ERROR_SAMPLES) {
        errors.push({
          filePath,
          lineNumber,
          error: e instanceof Error ? e.message : String(e),
        });
      }
      continue;
    }

    const mapped = mapRecord(parsed);
    if (!mapped) continue;

    batch.push(mapped);
    if (batch.length >= batchSize) {
      yield batch;
      batch = [];
    }
  }

  if (batch.length > 0) yield batch;

  if (errors.length > 0) {
    console.warn(
      `JSON parse warnings for ${filePath}: ${errors.length} sample(s) (first sample line ${errors[0].lineNumber})`
    );
  }
}

async function processJsonlFilesInFolders({
  datasetMap,
  folderNames,
  batchSize,
  mapRecord,
  onBatch,
}) {
  for (const folderName of folderNames) {
    const files = datasetMap.get(folderName);
    if (!files) continue;

    for (const filePath of files) {
      const base = path.basename(filePath);
      console.log(`  - ${folderName}/${base}`);

      let processed = 0;
      let batches = 0;

      for await (const batch of readJsonlFileInBatches(filePath, { batchSize, mapRecord })) {
        batches += 1;
        processed += batch.length;
        await onBatch(batch);
      }

      console.log(`    Imported ${processed.toLocaleString()} records in ${batches} batch(es).`);
    }
  }
}

async function importCustomers(session, datasetMap) {
  console.log("Importing Customers...");

  const cypher = `
    UNWIND $batch AS row
    MERGE (c:Customer { customerId: row.customerId })
    SET c += row
  `;

  let total = 0;
  await processJsonlFilesInFolders({
    datasetMap,
    folderNames: ["business_partners"],
    batchSize: BATCH_SIZE,
    mapRecord: (r) => {
      const customerId = r.customer ?? r.businessPartner;
      if (!customerId) return null;
      return sanitizeRecord({ customerId: String(customerId), ...r });
    },
    onBatch: async (batch) => {
      total += batch.length;
      await session.executeWrite(async (tx) => tx.run(cypher, { batch }));
    },
  });

  console.log(`Done. Customers imported: ${total.toLocaleString()}`);
}

async function importAddresses(session, datasetMap) {
  console.log("Importing Addresses...");

  const cypher = `
    UNWIND $batch AS row
    MERGE (a:Address { addressId: row.addressId })
    SET a += row
  `;

  let total = 0;
  await processJsonlFilesInFolders({
    datasetMap,
    folderNames: ["business_partner_addresses"],
    batchSize: BATCH_SIZE,
    mapRecord: (r) => {
      if (!r.addressId) return null;
      return sanitizeRecord({ addressId: String(r.addressId), ...r });
    },
    onBatch: async (batch) => {
      total += batch.length;
      await session.executeWrite(async (tx) => tx.run(cypher, { batch }));
    },
  });

  console.log(`Done. Addresses imported: ${total.toLocaleString()}`);
}

async function importProducts(session, datasetMap) {
  console.log("Importing Products...");

  const cypher = `
    UNWIND $batch AS row
    MERGE (p:Product { productId: row.productId })
    SET p += row
  `;

  let total = 0;
  await processJsonlFilesInFolders({
    datasetMap,
    folderNames: ["products"],
    batchSize: BATCH_SIZE,
    mapRecord: (r) => {
      if (!r.product) return null;
      return sanitizeRecord({ productId: String(r.product), ...r });
    },
    onBatch: async (batch) => {
      total += batch.length;
      await session.executeWrite(async (tx) => tx.run(cypher, { batch }));
    },
  });

  console.log(`Done. Products imported: ${total.toLocaleString()}`);
}

async function importPlants(session, datasetMap) {
  console.log("Importing Plants...");

  const cypher = `
    UNWIND $batch AS row
    MERGE (pl:Plant { plantId: row.plantId })
    SET pl += row
  `;

  let total = 0;
  await processJsonlFilesInFolders({
    datasetMap,
    folderNames: ["plants"],
    batchSize: BATCH_SIZE,
    mapRecord: (r) => {
      if (!r.plant) return null;
      return sanitizeRecord({
        plantId: String(r.plant),
        addressId: r.addressId ? String(r.addressId) : null,
        ...r,
      });
    },
    onBatch: async (batch) => {
      total += batch.length;
      await session.executeWrite(async (tx) => tx.run(cypher, { batch }));
    },
  });

  console.log(`Done. Plants imported: ${total.toLocaleString()}`);
}

async function importOrders(session, datasetMap) {
  console.log("Importing Orders...");

  const cypher = `
    UNWIND $batch AS row
    MERGE (o:Order { orderId: row.orderId })
    SET o += row
  `;

  let total = 0;
  await processJsonlFilesInFolders({
    datasetMap,
    folderNames: ["sales_order_headers"],
    batchSize: BATCH_SIZE,
    mapRecord: (r) => {
      const orderId = r.salesOrder;
      if (!orderId) return null;
      return sanitizeRecord({
        orderId: String(orderId),
        customerId: r.soldToParty ? String(r.soldToParty) : null,
        ...r,
      });
    },
    onBatch: async (batch) => {
      total += batch.length;
      await session.executeWrite(async (tx) => tx.run(cypher, { batch }));
    },
  });

  console.log(`Done. Orders imported: ${total.toLocaleString()}`);
}

async function importOrderItems(session, datasetMap) {
  console.log("Importing Order Items...");

  const cypher = `
    UNWIND $batch AS row
    MERGE (oi:OrderItem { orderId: row.orderId, itemId: row.itemId })
    SET oi += row
  `;

  let total = 0;
  await processJsonlFilesInFolders({
    datasetMap,
    folderNames: ["sales_order_items"],
    batchSize: BATCH_SIZE,
    mapRecord: (r) => {
      const orderId = r.salesOrder;
      const itemId = r.salesOrderItem;
      const productId = r.material;
      if (!orderId || !itemId || !productId) return null;
      return sanitizeRecord({
        orderId: String(orderId),
        itemId: String(itemId),
        productId: String(productId),
        ...r,
      });
    },
    onBatch: async (batch) => {
      total += batch.length;
      await session.executeWrite(async (tx) => tx.run(cypher, { batch }));
    },
  });

  console.log(`Done. Order Items imported: ${total.toLocaleString()}`);
}

async function importDeliveries(session, datasetMap) {
  console.log("Importing Deliveries...");

  const cypher = `
    UNWIND $batch AS row
    MERGE (d:Delivery { deliveryId: row.deliveryId })
    SET d += row
  `;

  let total = 0;
  await processJsonlFilesInFolders({
    datasetMap,
    folderNames: ["outbound_delivery_headers"],
    batchSize: BATCH_SIZE,
    mapRecord: (r) => {
      const deliveryId = r.deliveryDocument;
      if (!deliveryId) return null;
      return sanitizeRecord({ deliveryId: String(deliveryId), ...r });
    },
    onBatch: async (batch) => {
      total += batch.length;
      await session.executeWrite(async (tx) => tx.run(cypher, { batch }));
    },
  });

  console.log(`Done. Deliveries imported: ${total.toLocaleString()}`);
}

async function importInvoices(session, datasetMap) {
  console.log("Importing Invoices...");

  const cypher = `
    UNWIND $batch AS row
    MERGE (i:Invoice { invoiceId: row.invoiceId })
    SET i += row
  `;

  let total = 0;
  await processJsonlFilesInFolders({
    datasetMap,
    folderNames: ["billing_document_headers"],
    batchSize: BATCH_SIZE,
    mapRecord: (r) => {
      const invoiceId = r.billingDocument;
      if (!invoiceId) return null;
      return sanitizeRecord({
        invoiceId: String(invoiceId),
        customerId: r.soldToParty ? String(r.soldToParty) : null,
        ...r,
      });
    },
    onBatch: async (batch) => {
      total += batch.length;
      await session.executeWrite(async (tx) => tx.run(cypher, { batch }));
    },
  });

  console.log(`Done. Invoices imported: ${total.toLocaleString()}`);
}

async function importPayments(session, datasetMap) {
  console.log("Importing Payments...");

  const cypher = `
    UNWIND $batch AS row
    MERGE (p:Payment { paymentId: row.paymentId })
    SET p += row
  `;

  let total = 0;
  await processJsonlFilesInFolders({
    datasetMap,
    folderNames: ["payments_accounts_receivable"],
    batchSize: BATCH_SIZE,
    mapRecord: (r) => {
      const paymentId = r.accountingDocument;
      if (!paymentId) return null;
      return sanitizeRecord({
        paymentId: String(paymentId),
        customerId: r.customer ? String(r.customer) : null,
        invoiceId: r.invoiceReference ? String(r.invoiceReference) : null,
        ...r,
      });
    },
    onBatch: async (batch) => {
      total += batch.length;
      await session.executeWrite(async (tx) => tx.run(cypher, { batch }));
    },
  });

  console.log(`Done. Payments imported: ${total.toLocaleString()}`);
}

async function linkRelationships(session, datasetMap) {
  console.log("Linking Relationships...");

  // (Order)-[:PLACED_BY]->(Customer)
  {
    const cypher = `
      UNWIND $batch AS row
      MATCH (o:Order { orderId: row.orderId })
      MATCH (c:Customer { customerId: row.customerId })
      MERGE (o)-[:PLACED_BY]->(c)
    `;

    let total = 0;
    await processJsonlFilesInFolders({
      datasetMap,
      folderNames: ["sales_order_headers"],
      batchSize: BATCH_SIZE,
      mapRecord: (r) => {
        if (!r.salesOrder || !r.soldToParty) return null;
        total += 1;
        return sanitizeRecord({ orderId: String(r.salesOrder), customerId: String(r.soldToParty) });
      },
      onBatch: async (batch) => {
        await session.executeWrite(async (tx) => tx.run(cypher, { batch }));
      },
    });
  }

  // (Order)-[:HAS_ITEM]->(OrderItem)
  {
    const cypher = `
      UNWIND $batch AS row
      MATCH (o:Order { orderId: row.orderId })
      MATCH (oi:OrderItem { orderId: row.orderId, itemId: row.itemId })
      MERGE (o)-[:HAS_ITEM]->(oi)
    `;

    let total = 0;
    await processJsonlFilesInFolders({
      datasetMap,
      folderNames: ["sales_order_items"],
      batchSize: BATCH_SIZE,
      mapRecord: (r) => {
        const orderId = r.salesOrder;
        const itemId = r.salesOrderItem;
        if (!orderId || !itemId) return null;
        total += 1;
        return sanitizeRecord({ orderId: String(orderId), itemId: String(itemId) });
      },
      onBatch: async (batch) => {
        await session.executeWrite(async (tx) => tx.run(cypher, { batch }));
      },
    });
  }

  // (OrderItem)-[:FOR_PRODUCT]->(Product)
  {
    const cypher = `
      UNWIND $batch AS row
      MATCH (oi:OrderItem { orderId: row.orderId, itemId: row.itemId })
      MATCH (p:Product { productId: row.productId })
      MERGE (oi)-[:FOR_PRODUCT]->(p)
    `;

    await processJsonlFilesInFolders({
      datasetMap,
      folderNames: ["sales_order_items"],
      batchSize: BATCH_SIZE,
      mapRecord: (r) => {
        const orderId = r.salesOrder;
        const itemId = r.salesOrderItem;
        const productId = r.material;
        if (!orderId || !itemId || !productId) return null;
        return sanitizeRecord({
          orderId: String(orderId),
          itemId: String(itemId),
          productId: String(productId),
        });
      },
      onBatch: async (batch) => {
        await session.executeWrite(async (tx) => tx.run(cypher, { batch }));
      },
    });
  }

  // (Delivery)-[:FROM_PLANT]->(Plant)
  {
    const cypher = `
      UNWIND $batch AS row
      MATCH (d:Delivery { deliveryId: row.deliveryId })
      MATCH (pl:Plant { plantId: row.plantId })
      MERGE (d)-[:FROM_PLANT]->(pl)
    `;

    await processJsonlFilesInFolders({
      datasetMap,
      folderNames: ["outbound_delivery_items"],
      batchSize: BATCH_SIZE,
      mapRecord: (r) => {
        const deliveryId = r.deliveryDocument;
        const plantId = r.plant;
        if (!deliveryId || !plantId) return null;
        return sanitizeRecord({ deliveryId: String(deliveryId), plantId: String(plantId) });
      },
      onBatch: async (batch) => {
        await session.executeWrite(async (tx) => tx.run(cypher, { batch }));
      },
    });
  }

  // (Delivery)-[:BILLED_IN]->(Invoice)
  {
    const cypher = `
      UNWIND $batch AS row
      MATCH (d:Delivery { deliveryId: row.deliveryId })
      MATCH (i:Invoice { invoiceId: row.invoiceId })
      MERGE (d)-[:BILLED_IN]->(i)
    `;

    await processJsonlFilesInFolders({
      datasetMap,
      folderNames: ["billing_document_items"],
      batchSize: BATCH_SIZE,
      mapRecord: (r) => {
        const deliveryId = r.referenceSdDocument;
        const invoiceId = r.billingDocument;
        if (!deliveryId || !invoiceId) return null;
        return sanitizeRecord({ deliveryId: String(deliveryId), invoiceId: String(invoiceId) });
      },
      onBatch: async (batch) => {
        await session.executeWrite(async (tx) => tx.run(cypher, { batch }));
      },
    });
  }

  // (Invoice)-[:PAID_BY]->(Payment)
  {
    const cypher = `
      UNWIND $batch AS row
      MATCH (i:Invoice { invoiceId: row.invoiceId })
      MATCH (p:Payment { paymentId: row.paymentId })
      MERGE (i)-[:PAID_BY]->(p)
    `;

    await processJsonlFilesInFolders({
      datasetMap,
      folderNames: ["payments_accounts_receivable"],
      batchSize: BATCH_SIZE,
      mapRecord: (r) => {
        if (!r.accountingDocument || !r.invoiceReference) return null;
        return sanitizeRecord({
          paymentId: String(r.accountingDocument),
          invoiceId: String(r.invoiceReference),
        });
      },
      onBatch: async (batch) => {
        await session.executeWrite(async (tx) => tx.run(cypher, { batch }));
      },
    });
  }

  // (Customer)-[:HAS_DELIVERY]->(Delivery) derived from Order -> Delivery
  {
    // orderId -> customerId map
    const orderToCustomer = new Map();

    await processJsonlFilesInFolders({
      datasetMap,
      folderNames: ["sales_order_headers"],
      batchSize: BATCH_SIZE,
      mapRecord: (r) => {
        if (!r.salesOrder || !r.soldToParty) return null;
        return sanitizeRecord({ orderId: String(r.salesOrder), customerId: String(r.soldToParty) });
      },
      onBatch: async (batch) => {
        for (const row of batch) {
          orderToCustomer.set(row.orderId, row.customerId);
        }
      },
    });

    const cypher = `
      UNWIND $batch AS row
      MATCH (c:Customer { customerId: row.customerId })
      MATCH (d:Delivery { deliveryId: row.deliveryId })
      MERGE (c)-[:HAS_DELIVERY]->(d)
    `;

    await processJsonlFilesInFolders({
      datasetMap,
      folderNames: ["outbound_delivery_items"],
      batchSize: BATCH_SIZE,
      mapRecord: (r) => {
        const orderId = r.referenceSdDocument;
        const deliveryId = r.deliveryDocument;
        if (!orderId || !deliveryId) return null;
        const customerId = orderToCustomer.get(String(orderId));
        if (!customerId) return null;
        return sanitizeRecord({ customerId: String(customerId), deliveryId: String(deliveryId) });
      },
      onBatch: async (batch) => {
        await session.executeWrite(async (tx) => tx.run(cypher, { batch }));
      },
    });
  }

  // (Customer)-[:HAS_ADDRESS]->(Address)
  {
    const cypher = `
      UNWIND $batch AS row
      MATCH (c:Customer { customerId: row.customerId })
      MATCH (a:Address { addressId: row.addressId })
      MERGE (c)-[:HAS_ADDRESS]->(a)
    `;

    await processJsonlFilesInFolders({
      datasetMap,
      folderNames: ["business_partner_addresses"],
      batchSize: BATCH_SIZE,
      mapRecord: (r) => {
        if (!r.businessPartner || !r.addressId) return null;
        return sanitizeRecord({ customerId: String(r.businessPartner), addressId: String(r.addressId) });
      },
      onBatch: async (batch) => {
        await session.executeWrite(async (tx) => tx.run(cypher, { batch }));
      },
    });
  }

  // (Plant)-[:HAS_ADDRESS]->(Address)
  {
    const cypher = `
      UNWIND $batch AS row
      MATCH (pl:Plant { plantId: row.plantId })
      MATCH (a:Address { addressId: row.addressId })
      MERGE (pl)-[:HAS_ADDRESS]->(a)
    `;

    await processJsonlFilesInFolders({
      datasetMap,
      folderNames: ["plants"],
      batchSize: BATCH_SIZE,
      mapRecord: (r) => {
        if (!r.plant || !r.addressId) return null;
        return sanitizeRecord({ plantId: String(r.plant), addressId: String(r.addressId) });
      },
      onBatch: async (batch) => {
        await session.executeWrite(async (tx) => tx.run(cypher, { batch }));
      },
    });
  }

  console.log("Done. Relationships linked.");
}

async function main() {
  console.log(`Dataset dir: ${DATASET_DIR}`);
  if (!fs.existsSync(DATASET_DIR)) throw new Error(`DATASET_DIR not found: ${DATASET_DIR}`);

  const datasetMap = await buildDatasetFileMap(DATASET_DIR);
  console.log(`Found ${datasetMap.size} folder(s) with .jsonl files under dataset.`);

  const session = driver.session();

  try {
    await importCustomers(session, datasetMap);
    await importAddresses(session, datasetMap);
    await importProducts(session, datasetMap);
    await importPlants(session, datasetMap);
    await importOrders(session, datasetMap);
    await importOrderItems(session, datasetMap);
    await importDeliveries(session, datasetMap);
    await importInvoices(session, datasetMap);
    await importPayments(session, datasetMap);
    await linkRelationships(session, datasetMap);
  } finally {
    await session.close();
    await driver.close();
  }
}

main()
  .then(() => {
    console.log("Import script finished successfully.");
    process.exit(0);
  })
  .catch((err) => {
    console.error("Import script failed:", err);
    process.exit(1);
  });

// Usage: node scripts/import.js
// Requires ESM (package.json "type":"module")

import fs from "fs";
import path from "path";
import readline from "readline";
import { fileURLToPath } from "url";

import driver from "../src/lib/neo4j.js";

const DATASET_DIR = process.env.DATASET_DIR
  ? path.resolve(process.env.DATASET_DIR)
  : (() => {
      const scriptsDir = path.dirname(fileURLToPath(import.meta.url)); // .../scripts
      const projectRoot = path.resolve(scriptsDir, "..");
      return path.join(projectRoot, "dataset");
    })();

const BATCH_SIZE = Number.parseInt(process.env.IMPORT_BATCH_SIZE ?? "2000", 10);
const MAX_ERROR_SAMPLES = Number.parseInt(process.env.MAX_ERROR_SAMPLES ?? "5", 10);

function sanitizeValue(value) {
  if (value === null || value === undefined) return undefined;
  if (typeof value === "string") return value;
  if (typeof value === "number") return value;
  if (typeof value === "boolean") return value;
  return JSON.stringify(value);
}

function sanitizeRecord(record) {
  const out = {};
  for (const [k, v] of Object.entries(record)) {
    const sanitized = sanitizeValue(v);
    if (sanitized !== undefined) out[k] = sanitized;
  }
  return out;
}

async function buildDatasetFileMap(rootDir) {
  const entries = await fs.promises.readdir(rootDir, { withFileTypes: true });
  const map = new Map(); // folderName => string[]

  for (const entry of entries) {
    if (!entry.isDirectory()) continue;
    const folderName = entry.name;
    const folderPath = path.join(rootDir, folderName);
    const files = await fs.promises.readdir(folderPath, { withFileTypes: true });
    const jsonlFiles = files
      .filter((f) => f.isFile() && f.name.endsWith(".jsonl"))
      .map((f) => path.join(folderPath, f.name));

    if (jsonlFiles.length > 0) map.set(folderName, jsonlFiles);
  }

  return map;
}

async function* readJsonlFileInBatches(filePath, { batchSize, mapRecord }) {
  const rl = readline.createInterface({
    input: fs.createReadStream(filePath, { encoding: "utf8" }),
    crlfDelay: Infinity,
  });

  let batch = [];
  let lineNumber = 0;
  const errors = [];

  for await (const line of rl) {
    lineNumber += 1;
    const trimmed = line.trim();
    if (!trimmed) continue;

    let parsed;
    try {
      parsed = JSON.parse(trimmed);
    } catch (e) {
      if (errors.length < MAX_ERROR_SAMPLES) {
        errors.push({
          filePath,
          lineNumber,
          error: e instanceof Error ? e.message : String(e),
        });
      }
      continue;
    }

    const mapped = mapRecord(parsed);
    if (mapped) {
      batch.push(mapped);
      if (batch.length >= batchSize) {
        yield batch;
        batch = [];
      }
    }
  }

  if (batch.length > 0) yield batch;

  if (errors.length > 0) {
    console.warn(
      `JSON parse warnings for ${filePath}: ${errors.length} sample(s) (first sample line ${errors[0].lineNumber})`
    );
  }
}

async function processJsonlFilesInFolders({
  datasetMap,
  folderNames,
  batchSize,
  mapRecord,
  onBatch,
}) {
  for (const folderName of folderNames) {
    const files = datasetMap.get(folderName);
    if (!files) continue;

    for (const filePath of files) {
      const base = path.basename(filePath);
      console.log(`  - ${folderName}/${base}`);

      let processed = 0;
      let batches = 0;

      for await (const batch of readJsonlFileInBatches(filePath, { batchSize, mapRecord })) {
        batches += 1;
        processed += batch.length;
        await onBatch(batch);
      }

      console.log(`    Imported ${processed.toLocaleString()} records in ${batches} batch(es).`);
    }
  }
}

async function mergeNodesByKey(session, { label, keyField, folderNames, mapRecord }) {
  const cypher = `
    UNWIND $batch AS row
    MERGE (n:${label} { ${keyField}: row.${keyField} })
    SET n += row
  `;

  let total = 0;
  await processJsonlFilesInFolders({
    datasetMap: globalThis.__datasetMap,
    folderNames,
    batchSize: BATCH_SIZE,
    mapRecord,
    onBatch: async (batch) => {
      total += batch.length;
      await session.executeWrite(async (tx) => {
        await tx.run(cypher, { batch });
      });
    },
  });

  return total;
}

async function importCustomers(session, datasetMap) {
  console.log("Importing Customers...");

  const cypher = `
    UNWIND $batch AS row
    MERGE (c:Customer { customerId: row.customerId })
    SET c += row
  `;

  let total = 0;
  await processJsonlFilesInFolders({
    datasetMap,
    folderNames: ["business_partners"],
    batchSize: BATCH_SIZE,
    mapRecord: (r) => {
      const customerId = r.customer ?? r.businessPartner;
      if (!customerId) return null;
      return sanitizeRecord({ customerId: String(customerId), ...r });
    },
    onBatch: async (batch) => {
      total += batch.length;
      await session.executeWrite(async (tx) => tx.run(cypher, { batch }));
    },
  });

  console.log(`Done. Customers imported: ${total.toLocaleString()}`);
}

async function importAddresses(session, datasetMap) {
  console.log("Importing Addresses...");

  const cypher = `
    UNWIND $batch AS row
    MERGE (a:Address { addressId: row.addressId })
    SET a += row
  `;

  let total = 0;
  await processJsonlFilesInFolders({
    datasetMap,
    folderNames: ["business_partner_addresses"],
    batchSize: BATCH_SIZE,
    mapRecord: (r) => {
      if (!r.addressId) return null;
      return sanitizeRecord({ addressId: String(r.addressId), ...r });
    },
    onBatch: async (batch) => {
      total += batch.length;
      await session.executeWrite(async (tx) => tx.run(cypher, { batch }));
    },
  });

  console.log(`Done. Addresses imported: ${total.toLocaleString()}`);
}

async function importProducts(session, datasetMap) {
  console.log("Importing Products...");

  const cypher = `
    UNWIND $batch AS row
    MERGE (p:Product { productId: row.productId })
    SET p += row
  `;

  let total = 0;
  await processJsonlFilesInFolders({
    datasetMap,
    folderNames: ["products"],
    batchSize: BATCH_SIZE,
    mapRecord: (r) => {
      if (!r.product) return null;
      return sanitizeRecord({ productId: String(r.product), ...r });
    },
    onBatch: async (batch) => {
      total += batch.length;
      await session.executeWrite(async (tx) => tx.run(cypher, { batch }));
    },
  });

  console.log(`Done. Products imported: ${total.toLocaleString()}`);
}

async function importPlants(session, datasetMap) {
  console.log("Importing Plants...");

  const cypher = `
    UNWIND $batch AS row
    MERGE (pl:Plant { plantId: row.plantId })
    SET pl += row
  `;

  let total = 0;
  await processJsonlFilesInFolders({
    datasetMap,
    folderNames: ["plants"],
    batchSize: BATCH_SIZE,
    mapRecord: (r) => {
      if (!r.plant) return null;
      return sanitizeRecord({
        plantId: String(r.plant),
        addressId: r.addressId ? String(r.addressId) : null,
        ...r,
      });
    },
    onBatch: async (batch) => {
      total += batch.length;
      await session.executeWrite(async (tx) => tx.run(cypher, { batch }));
    },
  });

  console.log(`Done. Plants imported: ${total.toLocaleString()}`);
}

async function importOrders(session, datasetMap) {
  console.log("Importing Orders...");

  const cypher = `
    UNWIND $batch AS row
    MERGE (o:Order { orderId: row.orderId })
    SET o += row
  `;

  let total = 0;
  await processJsonlFilesInFolders({
    datasetMap,
    folderNames: ["sales_order_headers"],
    batchSize: BATCH_SIZE,
    mapRecord: (r) => {
      const orderId = r.salesOrder;
      if (!orderId) return null;
      return sanitizeRecord({
        orderId: String(orderId),
        customerId: r.soldToParty ? String(r.soldToParty) : null,
        ...r,
      });
    },
    onBatch: async (batch) => {
      total += batch.length;
      await session.executeWrite(async (tx) => tx.run(cypher, { batch }));
    },
  });

  console.log(`Done. Orders imported: ${total.toLocaleString()}`);
}

async function importOrderItems(session, datasetMap) {
  console.log("Importing Order Items...");

  const cypher = `
    UNWIND $batch AS row
    MERGE (oi:OrderItem { orderId: row.orderId, itemId: row.itemId })
    SET oi += row
  `;

  let total = 0;
  await processJsonlFilesInFolders({
    datasetMap,
    folderNames: ["sales_order_items"],
    batchSize: BATCH_SIZE,
    mapRecord: (r) => {
      const orderId = r.salesOrder;
      const itemId = r.salesOrderItem;
      const productId = r.material;
      if (!orderId || !itemId || !productId) return null;
      return sanitizeRecord({
        orderId: String(orderId),
        itemId: String(itemId),
        productId: String(productId),
        ...r,
      });
    },
    onBatch: async (batch) => {
      total += batch.length;
      await session.executeWrite(async (tx) => tx.run(cypher, { batch }));
    },
  });

  console.log(`Done. Order Items imported: ${total.toLocaleString()}`);
}

async function importDeliveries(session, datasetMap) {
  console.log("Importing Deliveries...");

  const cypher = `
    UNWIND $batch AS row
    MERGE (d:Delivery { deliveryId: row.deliveryId })
    SET d += row
  `;

  let total = 0;
  await processJsonlFilesInFolders({
    datasetMap,
    folderNames: ["outbound_delivery_headers"],
    batchSize: BATCH_SIZE,
    mapRecord: (r) => {
      const deliveryId = r.deliveryDocument;
      if (!deliveryId) return null;
      return sanitizeRecord({ deliveryId: String(deliveryId), ...r });
    },
    onBatch: async (batch) => {
      total += batch.length;
      await session.executeWrite(async (tx) => tx.run(cypher, { batch }));
    },
  });

  console.log(`Done. Deliveries imported: ${total.toLocaleString()}`);
}

async function importInvoices(session, datasetMap) {
  console.log("Importing Invoices...");

  const cypher = `
    UNWIND $batch AS row
    MERGE (i:Invoice { invoiceId: row.invoiceId })
    SET i += row
  `;

  let total = 0;
  await processJsonlFilesInFolders({
    datasetMap,
    folderNames: ["billing_document_headers"],
    batchSize: BATCH_SIZE,
    mapRecord: (r) => {
      const invoiceId = r.billingDocument;
      if (!invoiceId) return null;
      return sanitizeRecord({
        invoiceId: String(invoiceId),
        customerId: r.soldToParty ? String(r.soldToParty) : null,
        ...r,
      });
    },
    onBatch: async (batch) => {
      total += batch.length;
      await session.executeWrite(async (tx) => tx.run(cypher, { batch }));
    },
  });

  console.log(`Done. Invoices imported: ${total.toLocaleString()}`);
}

async function importPayments(session, datasetMap) {
  console.log("Importing Payments...");

  const cypher = `
    UNWIND $batch AS row
    MERGE (p:Payment { paymentId: row.paymentId })
    SET p += row
  `;

  let total = 0;
  await processJsonlFilesInFolders({
    datasetMap,
    folderNames: ["payments_accounts_receivable"],
    batchSize: BATCH_SIZE,
    mapRecord: (r) => {
      const paymentId = r.accountingDocument;
      if (!paymentId) return null;
      return sanitizeRecord({
        paymentId: String(paymentId),
        customerId: r.customer ? String(r.customer) : null,
        invoiceId: r.invoiceReference ? String(r.invoiceReference) : null,
        ...r,
      });
    },
    onBatch: async (batch) => {
      total += batch.length;
      await session.executeWrite(async (tx) => tx.run(cypher, { batch }));
    },
  });

  console.log(`Done. Payments imported: ${total.toLocaleString()}`);
}

async function linkRelationships(session, datasetMap) {
  console.log("Linking Relationships...");

  // Order -> Customer (PLACED_BY)
  {
    const cypher = `
      UNWIND $batch AS row
      MATCH (o:Order { orderId: row.orderId })
      MATCH (c:Customer { customerId: row.customerId })
      MERGE (o)-[:PLACED_BY]->(c)
    `;

    let total = 0;
    await processJsonlFilesInFolders({
      datasetMap,
      folderNames: ["sales_order_headers"],
      batchSize: BATCH_SIZE,
      mapRecord: (r) => {
        const orderId = r.salesOrder;
        const customerId = r.soldToParty;
        if (!orderId || !customerId) return null;
        return sanitizeRecord({
          orderId: String(orderId),
          customerId: String(customerId),
        });
      },
      onBatch: async (batch) => {
        total += batch.length;
        await session.executeWrite(async (tx) => tx.run(cypher, { batch }));
      },
    });

    console.log(`  PLACED_BY linked rows: ${total.toLocaleString()}`);
  }

  // Purchase Order -> Purchase Order Item (HAS_ITEM)
  {
    const cypher = `
      UNWIND $batch AS row
      MATCH (o:Order { orderId: row.orderId })
      MATCH (oi:OrderItem { orderId: row.orderId, itemId: row.itemId })
      MERGE (o)-[:HAS_ITEM]->(oi)
    `;

    let total = 0;
    await processJsonlFilesInFolders({
      datasetMap,
      folderNames: ["sales_order_items"],
      batchSize: BATCH_SIZE,
      mapRecord: (r) => {
        const orderId = r.salesOrder;
        const itemId = r.salesOrderItem;
        if (!orderId || !itemId) return null;
        return sanitizeRecord({ orderId: String(orderId), itemId: String(itemId) });
      },
      onBatch: async (batch) => {
        total += batch.length;
        await session.executeWrite(async (tx) => tx.run(cypher, { batch }));
      },
    });

    console.log(`  HAS_ITEM linked rows: ${total.toLocaleString()}`);
  }

  // Purchase Order Item -> Material (FOR_PRODUCT -> Product)
  {
    const cypher = `
      UNWIND $batch AS row
      MATCH (oi:OrderItem { orderId: row.orderId, itemId: row.itemId })
      MATCH (p:Product { productId: row.productId })
      MERGE (oi)-[:FOR_PRODUCT]->(p)
    `;

    let total = 0;
    await processJsonlFilesInFolders({
      datasetMap,
      folderNames: ["sales_order_items"],
      batchSize: BATCH_SIZE,
      mapRecord: (r) => {
        const orderId = r.salesOrder;
        const itemId = r.salesOrderItem;
        const productId = r.material;
        if (!orderId || !itemId || !productId) return null;
        return sanitizeRecord({
          orderId: String(orderId),
          itemId: String(itemId),
          productId: String(productId),
        });
      },
      onBatch: async (batch) => {
        total += batch.length;
        await session.executeWrite(async (tx) => tx.run(cypher, { batch }));
      },
    });

    console.log(`  FOR_PRODUCT linked rows: ${total.toLocaleString()}`);
  }

  // Delivery -> Plant (FROM_PLANT)
  {
    const cypher = `
      UNWIND $batch AS row
      MATCH (d:Delivery { deliveryId: row.deliveryId })
      MATCH (pl:Plant { plantId: row.plantId })
      MERGE (d)-[:FROM_PLANT]->(pl)
    `;

    let total = 0;
    await processJsonlFilesInFolders({
      datasetMap,
      folderNames: ["outbound_delivery_items"],
      batchSize: BATCH_SIZE,
      mapRecord: (r) => {
        const deliveryId = r.deliveryDocument;
        const plantId = r.plant;
        if (!deliveryId || !plantId) return null;
        return sanitizeRecord({
          deliveryId: String(deliveryId),
          plantId: String(plantId),
        });
      },
      onBatch: async (batch) => {
        total += batch.length;
        await session.executeWrite(async (tx) => tx.run(cypher, { batch }));
      },
    });

    console.log(`  FROM_PLANT linked rows: ${total.toLocaleString()}`);
  }

  // Delivery -> Invoice (BILLED_IN)
  {
    const cypher = `
      UNWIND $batch AS row
      MATCH (d:Delivery { deliveryId: row.deliveryId })
      MATCH (i:Invoice { invoiceId: row.invoiceId })
      MERGE (d)-[:BILLED_IN]->(i)
    `;

    let total = 0;
    await processJsonlFilesInFolders({
      datasetMap,
      folderNames: ["billing_document_items"],
      batchSize: BATCH_SIZE,
      mapRecord: (r) => {
        const deliveryId = r.referenceSdDocument;
        const invoiceId = r.billingDocument;
        if (!deliveryId || !invoiceId) return null;
        return sanitizeRecord({
          deliveryId: String(deliveryId),
          invoiceId: String(invoiceId),
        });
      },
      onBatch: async (batch) => {
        total += batch.length;
        await session.executeWrite(async (tx) => tx.run(cypher, { batch }));
      },
    });

    console.log(`  BILLED_IN linked rows: ${total.toLocaleString()}`);
  }

  // Invoice -> Payment (PAID_BY)
  {
    const cypher = `
      UNWIND $batch AS row
      MATCH (i:Invoice { invoiceId: row.invoiceId })
      MATCH (p:Payment { paymentId: row.paymentId })
      MERGE (i)-[:PAID_BY]->(p)
    `;

    let total = 0;
    await processJsonlFilesInFolders({
      datasetMap,
      folderNames: ["payments_accounts_receivable"],
      batchSize: BATCH_SIZE,
      mapRecord: (r) => {
        const paymentId = r.accountingDocument;
        const invoiceId = r.invoiceReference;
        if (!paymentId || !invoiceId) return null;
        return sanitizeRecord({
          paymentId: String(paymentId),
          invoiceId: String(invoiceId),
        });
      },
      onBatch: async (batch) => {
        total += batch.length;
        await session.executeWrite(async (tx) => tx.run(cypher, { batch }));
      },
    });

    console.log(`  PAID_BY linked rows: ${total.toLocaleString()}`);
  }

  // Customer -> Delivery (HAS_DELIVERY)
  // Derive: order -> delivery from outbound_delivery_items, then customer from sales_order_headers.
  {
    // Build orderId -> customerId map streaming.
    const orderToCustomer = new Map();
    await processJsonlFilesInFolders({
      datasetMap,
      folderNames: ["sales_order_headers"],
      batchSize: BATCH_SIZE,
      mapRecord: (r) => {
        const orderId = r.salesOrder;
        const customerId = r.soldToParty;
        if (!orderId || !customerId) return null;
        return { orderId: String(orderId), customerId: String(customerId) };
      },
      onBatch: async (batch) => {
        for (const row of batch) {
          orderToCustomer.set(row.orderId, row.customerId);
        }
      },
    });

    const cypher = `
      UNWIND $batch AS row
      MATCH (c:Customer { customerId: row.customerId })
      MATCH (d:Delivery { deliveryId: row.deliveryId })
      MERGE (c)-[:HAS_DELIVERY]->(d)
    `;

    let total = 0;
    await processJsonlFilesInFolders({
      datasetMap,
      folderNames: ["outbound_delivery_items"],
      batchSize: BATCH_SIZE,
      mapRecord: (r) => {
        const orderId = r.referenceSdDocument;
        const deliveryId = r.deliveryDocument;
        if (!orderId || !deliveryId) return null;
        const customerId = orderToCustomer.get(String(orderId));
        if (!customerId) return null;
        return sanitizeRecord({
          customerId: String(customerId),
          deliveryId: String(deliveryId),
        });
      },
      onBatch: async (batch) => {
        total += batch.length;
        await session.executeWrite(async (tx) => tx.run(cypher, { batch }));
      },
    });

    console.log(`  HAS_DELIVERY linked rows: ${total.toLocaleString()}`);
  }

  // Customer -> Address (HAS_ADDRESS)
  {
    const cypher = `
      UNWIND $batch AS row
      MATCH (c:Customer { customerId: row.customerId })
      MATCH (a:Address { addressId: row.addressId })
      MERGE (c)-[:HAS_ADDRESS]->(a)
    `;

    let total = 0;
    await processJsonlFilesInFolders({
      datasetMap,
      folderNames: ["business_partner_addresses"],
      batchSize: BATCH_SIZE,
      mapRecord: (r) => {
        if (!r.businessPartner || !r.addressId) return null;
        return sanitizeRecord({
          customerId: String(r.businessPartner),
          addressId: String(r.addressId),
        });
      },
      onBatch: async (batch) => {
        total += batch.length;
        await session.executeWrite(async (tx) => tx.run(cypher, { batch }));
      },
    });

    console.log(`  HAS_ADDRESS (Customer) linked rows: ${total.toLocaleString()}`);
  }

  // Plant -> Address (HAS_ADDRESS)
  {
    const cypher = `
      UNWIND $batch AS row
      MATCH (pl:Plant { plantId: row.plantId })
      MATCH (a:Address { addressId: row.addressId })
      MERGE (pl)-[:HAS_ADDRESS]->(a)
    `;

    let total = 0;
    await processJsonlFilesInFolders({
      datasetMap,
      folderNames: ["plants"],
      batchSize: BATCH_SIZE,
      mapRecord: (r) => {
        if (!r.plant || !r.addressId) return null;
        return sanitizeRecord({
          plantId: String(r.plant),
          addressId: String(r.addressId),
        });
      },
      onBatch: async (batch) => {
        total += batch.length;
        await session.executeWrite(async (tx) => tx.run(cypher, { batch }));
      },
    });

    console.log(`  HAS_ADDRESS (Plant) linked rows: ${total.toLocaleString()}`);
  }

  console.log("Done. Relationships linked.");
}

async function main() {
  console.log(`Dataset dir: ${DATASET_DIR}`);
  if (!fs.existsSync(DATASET_DIR)) throw new Error(`DATASET_DIR not found: ${DATASET_DIR}`);

  const datasetMap = await buildDatasetFileMap(DATASET_DIR);
  console.log(`Found ${datasetMap.size} folder(s) with .jsonl files under dataset.`);

  // optional, for helpers
  globalThis.__datasetMap = datasetMap;

  const session = driver.session();

  try {
    await importCustomers(session, datasetMap);
    await importAddresses(session, datasetMap);
    await importProducts(session, datasetMap);
    await importPlants(session, datasetMap);

    await importOrders(session, datasetMap);
    await importOrderItems(session, datasetMap);
    await importDeliveries(session, datasetMap);
    await importInvoices(session, datasetMap);
    await importPayments(session, datasetMap);

    await linkRelationships(session, datasetMap);
  } finally {
    await session.close();
    await driver.close();
  }
}

main()
  .then(() => {
    console.log("Import script finished successfully.");
    process.exit(0);
  })
  .catch((err) => {
    console.error("Import script failed:", err);
    process.exit(1);
  });

// Usage: node scripts/import.js
// Requires ESM (package.json "type":"module")

import fs from "fs";
import path from "path";
import readline from "readline";
import { fileURLToPath } from "url";

import driver from "../src/lib/neo4j.js";

const DATASET_DIR = process.env.DATASET_DIR
  ? path.resolve(process.env.DATASET_DIR)
  : (() => {
      const scriptsDir = path.dirname(fileURLToPath(import.meta.url)); // .../scripts
      const projectRoot = path.resolve(scriptsDir, "..");
      return path.join(projectRoot, "dataset");
    })();

const BATCH_SIZE = Number.parseInt(process.env.IMPORT_BATCH_SIZE ?? "2000", 10);
const MAX_ERROR_SAMPLES = Number.parseInt(
  process.env.MAX_ERROR_SAMPLES ?? "5",
  10
);

function sanitizeValue(value) {
  if (value === null || value === undefined) return undefined;
  if (typeof value === "string") return value;
  if (typeof value === "number") return value;
  if (typeof value === "boolean") return value;
  return JSON.stringify(value);
}

function sanitizeRecord(record) {
  const out = {};
  for (const [k, v] of Object.entries(record)) {
    const sanitized = sanitizeValue(v);
    if (sanitized !== undefined) out[k] = sanitized;
  }
  return out;
}

async function buildDatasetFileMap(rootDir) {
  const entries = await fs.promises.readdir(rootDir, { withFileTypes: true });
  const map = new Map(); // folderName => string[]

  for (const entry of entries) {
    if (!entry.isDirectory()) continue;
    const folderName = entry.name;
    const folderPath = path.join(rootDir, folderName);
    const files = await fs.promises.readdir(folderPath, { withFileTypes: true });
    const jsonlFiles = files
      .filter((f) => f.isFile() && f.name.endsWith(".jsonl"))
      .map((f) => path.join(folderPath, f.name));

    if (jsonlFiles.length > 0) map.set(folderName, jsonlFiles);
  }

  return map;
}

async function* readJsonlFileInBatches(filePath, { batchSize, mapRecord }) {
  const rl = readline.createInterface({
    input: fs.createReadStream(filePath, { encoding: "utf8" }),
    crlfDelay: Infinity,
  });

  let batch = [];
  let lineNumber = 0;
  const errors = [];

  for await (const line of rl) {
    lineNumber += 1;
    const trimmed = line.trim();
    if (!trimmed) continue;

    let parsed;
    try {
      parsed = JSON.parse(trimmed);
    } catch (e) {
      if (errors.length < MAX_ERROR_SAMPLES) {
        errors.push({
          filePath,
          lineNumber,
          error: e instanceof Error ? e.message : String(e),
        });
      }
      continue;
    }

    const mapped = mapRecord(parsed);
    if (mapped) {
      batch.push(mapped);
      if (batch.length >= batchSize) {
        yield batch;
        batch = [];
      }
    }
  }

  if (batch.length > 0) yield batch;

  if (errors.length > 0) {
    console.warn(
      `JSON parse warnings for ${filePath}: ${errors.length} sample(s) (first sample line ${errors[0].lineNumber})`
    );
  }
}

async function processJsonlFilesInFolders({
  datasetMap,
  folderNames,
  batchSize,
  mapRecord,
  onBatch,
}) {
  for (const folderName of folderNames) {
    const files = datasetMap.get(folderName);
    if (!files) continue;

    for (const filePath of files) {
      const base = path.basename(filePath);
      console.log(`  - ${folderName}/${base}`);

      let processed = 0;
      let batches = 0;

      for await (const batch of readJsonlFileInBatches(filePath, { batchSize, mapRecord })) {
        batches += 1;
        processed += batch.length;
        await onBatch(batch);
      }

      console.log(`    Imported ${processed.toLocaleString()} records in ${batches} batch(es).`);
    }
  }
}

async function importNodes(session, {
  label,
  keyFields,
  folderNames,
  mapRecord,
}) {
  const keyMatcher = Object.keys(keyFields)
    .map((k) => `${k}: row.${k}`)
    .join(", ");

  const setClause = Object.keys(keyFields)
    .map((k) => `n.${k} = row.${k}`)
    .join(", ");

  // MERGE by key, then set all mapped properties.
  const cypher = `
    UNWIND $batch AS row
    MERGE (n:${label} { ${keyMatcher} })
    SET n += row
  `;

  let total = 0;

  await processJsonlFilesInFolders({
    datasetMap: globalThis.__datasetMap,
    folderNames,
    batchSize: BATCH_SIZE,
    mapRecord,
    onBatch: async (batch) => {
      total += batch.length;
      await session.executeWrite(async (tx) => {
        await tx.run(cypher, { batch });
      });
    },
  });

  return total;
}

function getIdentityKey(row, field) {
  const v = row[field];
  if (v === null || v === undefined || v === "") return null;
  return String(v);
}

async function importCustomers(session, datasetMap) {
  console.log("Importing Customers...");

  const cypher = `
    UNWIND $batch AS row
    MERGE (c:Customer { customerId: row.customerId })
    SET c += row
  `;

  let total = 0;
  await processJsonlFilesInFolders({
    datasetMap,
    folderNames: ["business_partners"],
    batchSize: BATCH_SIZE,
    mapRecord: (r) => {
      const customerId = r.customer ?? r.businessPartner;
      if (!customerId) return null;

      return sanitizeRecord({
        customerId: String(customerId),
        ...r,
      });
    },
    onBatch: async (batch) => {
      total += batch.length;
      await session.executeWrite(async (tx) => {
        await tx.run(cypher, { batch });
      });
    },
  });

  console.log(`Done. Customers imported: ${total.toLocaleString()}`);
}

async function importAddresses(session, datasetMap) {
  console.log("Importing Addresses...");

  const cypher = `
    UNWIND $batch AS row
    MERGE (a:Address { addressId: row.addressId })
    SET a += row
  `;

  let total = 0;
  await processJsonlFilesInFolders({
    datasetMap,
    folderNames: ["business_partner_addresses"],
    batchSize: BATCH_SIZE,
    mapRecord: (r) => {
      const addressId = r.addressId;
      if (!addressId) return null;
      return sanitizeRecord({
        addressId: String(addressId),
        ...r,
      });
    },
    onBatch: async (batch) => {
      total += batch.length;
      await session.executeWrite(async (tx) => {
        await tx.run(cypher, { batch });
      });
    },
  });

  console.log(`Done. Addresses imported: ${total.toLocaleString()}`);
}

async function importProducts(session, datasetMap) {
  console.log("Importing Products...");

  const cypher = `
    UNWIND $batch AS row
    MERGE (p:Product { productId: row.productId })
    SET p += row
  `;

  let total = 0;
  await processJsonlFilesInFolders({
    datasetMap,
    folderNames: ["products"],
    batchSize: BATCH_SIZE,
    mapRecord: (r) => {
      const productId = r.product;
      if (!productId) return null;
      return sanitizeRecord({
        productId: String(productId),
        ...r,
      });
    },
    onBatch: async (batch) => {
      total += batch.length;
      await session.executeWrite(async (tx) => {
        await tx.run(cypher, { batch });
      });
    },
  });

  console.log(`Done. Products imported: ${total.toLocaleString()}`);
}

async function importPlants(session, datasetMap) {
  console.log("Importing Plants...");

  const cypher = `
    UNWIND $batch AS row
    MERGE (pl:Plant { plantId: row.plantId })
    SET pl += row
  `;

  let total = 0;
  await processJsonlFilesInFolders({
    datasetMap,
    folderNames: ["plants"],
    batchSize: BATCH_SIZE,
    mapRecord: (r) => {
      const plantId = r.plant;
      if (!plantId) return null;
      return sanitizeRecord({
        plantId: String(plantId),
        addressId: r.addressId ? String(r.addressId) : null,
        ...r,
      });
    },
    onBatch: async (batch) => {
      total += batch.length;
      await session.executeWrite(async (tx) => {
        await tx.run(cypher, { batch });
      });
    },
  });

  console.log(`Done. Plants imported: ${total.toLocaleString()}`);
}

async function importOrders(session, datasetMap) {
  console.log("Importing Orders...");

  const cypher = `
    UNWIND $batch AS row
    MERGE (o:Order { orderId: row.orderId })
    SET o += row
  `;

  let total = 0;
  await processJsonlFilesInFolders({
    datasetMap,
    folderNames: ["sales_order_headers"],
    batchSize: BATCH_SIZE,
    mapRecord: (r) => {
      const orderId = r.salesOrder;
      if (!orderId) return null;

      return sanitizeRecord({
        orderId: String(orderId),
        customerId: r.soldToParty ? String(r.soldToParty) : null,
        ...r,
      });
    },
    onBatch: async (batch) => {
      total += batch.length;
      await session.executeWrite(async (tx) => {
        await tx.run(cypher, { batch });
      });
    },
  });

  console.log(`Done. Orders imported: ${total.toLocaleString()}`);
}

async function importOrderItems(session, datasetMap) {
  console.log("Importing Order Items...");

  const cypher = `
    UNWIND $batch AS row
    MERGE (oi:OrderItem { orderId: row.orderId, itemId: row.itemId })
    SET oi += row
  `;

  let total = 0;
  await processJsonlFilesInFolders({
    datasetMap,
    folderNames: ["sales_order_items"],
    batchSize: BATCH_SIZE,
    mapRecord: (r) => {
      const orderId = r.salesOrder;
      const itemId = r.salesOrderItem;
      const productId = r.material;
      if (!orderId || !itemId || !productId) return null;

      return sanitizeRecord({
        orderId: String(orderId),
        itemId: String(itemId),
        productId: String(productId),
        ...r,
      });
    },
    onBatch: async (batch) => {
      total += batch.length;
      await session.executeWrite(async (tx) => {
        await tx.run(cypher, { batch });
      });
    },
  });

  console.log(`Done. Order Items imported: ${total.toLocaleString()}`);
}

async function importDeliveries(session, datasetMap) {
  console.log("Importing Deliveries...");

  const cypher = `
    UNWIND $batch AS row
    MERGE (d:Delivery { deliveryId: row.deliveryId })
    SET d += row
  `;

  let total = 0;
  await processJsonlFilesInFolders({
    datasetMap,
    folderNames: ["outbound_delivery_headers"],
    batchSize: BATCH_SIZE,
    mapRecord: (r) => {
      const deliveryId = r.deliveryDocument;
      if (!deliveryId) return null;
      return sanitizeRecord({
        deliveryId: String(deliveryId),
        ...r,
      });
    },
    onBatch: async (batch) => {
      total += batch.length;
      await session.executeWrite(async (tx) => {
        await tx.run(cypher, { batch });
      });
    },
  });

  console.log(`Done. Deliveries imported: ${total.toLocaleString()}`);
}

async function importInvoices(session, datasetMap) {
  console.log("Importing Invoices...");

  const cypher = `
    UNWIND $batch AS row
    MERGE (i:Invoice { invoiceId: row.invoiceId })
    SET i += row
  `;

  let total = 0;
  await processJsonlFilesInFolders({
    datasetMap,
    folderNames: ["billing_document_headers"],
    batchSize: BATCH_SIZE,
    mapRecord: (r) => {
      const invoiceId = r.billingDocument;
      if (!invoiceId) return null;
      return sanitizeRecord({
        invoiceId: String(invoiceId),
        customerId: r.soldToParty ? String(r.soldToParty) : null,
        ...r,
      });
    },
    onBatch: async (batch) => {
      total += batch.length;
      await session.executeWrite(async (tx) => {
        await tx.run(cypher, { batch });
      });
    },
  });

  console.log(`Done. Invoices imported: ${total.toLocaleString()}`);
}

async function importPayments(session, datasetMap) {
  console.log("Importing Payments...");

  const cypher = `
    UNWIND $batch AS row
    MERGE (p:Payment { paymentId: row.paymentId })
    SET p += row
  `;

  let total = 0;
  await processJsonlFilesInFolders({
    datasetMap,
    folderNames: ["payments_accounts_receivable"],
    batchSize: BATCH_SIZE,
    mapRecord: (r) => {
      const paymentId = r.accountingDocument;
      if (!paymentId) return null;
      return sanitizeRecord({
        paymentId: String(paymentId),
        customerId: r.customer ? String(r.customer) : null,
        invoiceId: r.invoiceReference ? String(r.invoiceReference) : null,
        ...r,
      });
    },
    onBatch: async (batch) => {
      total += batch.length;
      await session.executeWrite(async (tx) => {
        await tx.run(cypher, { batch });
      });
    },
  });

  console.log(`Done. Payments imported: ${total.toLocaleString()}`);
}

async function linkRelationships(session, datasetMap) {
  console.log("Linking Relationships...");

  // Orders -> Customers (PLACED_BY)
  {
    const cypher = `
      UNWIND $batch AS row
      MATCH (o:Order { orderId: row.orderId })
      MATCH (c:Customer { customerId: row.customerId })
      MERGE (o)-[:PLACED_BY]->(c)
    `;

    let total = 0;
    await processJsonlFilesInFolders({
      datasetMap,
      folderNames: ["sales_order_headers"],
      batchSize: BATCH_SIZE,
      mapRecord: (r) => {
        const orderId = r.salesOrder;
        const customerId = r.soldToParty;
        if (!orderId || !customerId) return null;
        return sanitizeRecord({ orderId: String(orderId), customerId: String(customerId) });
      },
      onBatch: async (batch) => {
        total += batch.length;
        await session.executeWrite(async (tx) => {
          await tx.run(cypher, { batch });
        });
      },
    });

    console.log(`  PLACED_BY linked rows: ${total.toLocaleString()}`);
  }

  // Order -> OrderItem (HAS_ITEM)
  {
    const cypher = `
      UNWIND $batch AS row
      MATCH (o:Order { orderId: row.orderId })
      MATCH (oi:OrderItem { orderId: row.orderId, itemId: row.itemId })
      MERGE (o)-[:HAS_ITEM]->(oi)
    `;

    let total = 0;
    await processJsonlFilesInFolders({
      datasetMap,
      folderNames: ["sales_order_items"],
      batchSize: BATCH_SIZE,
      mapRecord: (r) => {
        const orderId = r.salesOrder;
        const itemId = r.salesOrderItem;
        if (!orderId || !itemId) return null;
        return sanitizeRecord({ orderId: String(orderId), itemId: String(itemId) });
      },
      onBatch: async (batch) => {
        total += batch.length;
        await session.executeWrite(async (tx) => {
          await tx.run(cypher, { batch });
        });
      },
    });

    console.log(`  HAS_ITEM linked rows: ${total.toLocaleString()}`);
  }

  // OrderItem -> Product (FOR_PRODUCT) (Purchase Order Item -> Material)
  {
    const cypher = `
      UNWIND $batch AS row
      MATCH (oi:OrderItem { orderId: row.orderId, itemId: row.itemId })
      MATCH (p:Product { productId: row.productId })
      MERGE (oi)-[:FOR_PRODUCT]->(p)
    `;

    let total = 0;
    await processJsonlFilesInFolders({
      datasetMap,
      folderNames: ["sales_order_items"],
      batchSize: BATCH_SIZE,
      mapRecord: (r) => {
        const orderId = r.salesOrder;
        const itemId = r.salesOrderItem;
        const productId = r.material;
        if (!orderId || !itemId || !productId) return null;
        return sanitizeRecord({
          orderId: String(orderId),
          itemId: String(itemId),
          productId: String(productId),
        });
      },
      onBatch: async (batch) => {
        total += batch.length;
        await session.executeWrite(async (tx) => {
          await tx.run(cypher, { batch });
        });
      },
    });

    console.log(`  FOR_PRODUCT linked rows: ${total.toLocaleString()}`);
  }

  // Delivery -> Plant (Delivery -> Plant)
  {
    const cypher = `
      UNWIND $batch AS row
      MATCH (d:Delivery { deliveryId: row.deliveryId })
      MATCH (pl:Plant { plantId: row.plantId })
      MERGE (d)-[:FROM_PLANT]->(pl)
    `;

    let total = 0;
    await processJsonlFilesInFolders({
      datasetMap,
      folderNames: ["outbound_delivery_items"],
      batchSize: BATCH_SIZE,
      mapRecord: (r) => {
        const deliveryId = r.deliveryDocument;
        const plantId = r.plant;
        if (!deliveryId || !plantId) return null;
        return sanitizeRecord({ deliveryId: String(deliveryId), plantId: String(plantId) });
      },
      onBatch: async (batch) => {
        total += batch.length;
        await session.executeWrite(async (tx) => {
          await tx.run(cypher, { batch });
        });
      },
    });

    console.log(`  FROM_PLANT linked rows: ${total.toLocaleString()}`);
  }

  // Delivery -> Invoice (BILLED_IN)
  {
    const cypher = `
      UNWIND $batch AS row
      MATCH (d:Delivery { deliveryId: row.deliveryId })
      MATCH (i:Invoice { invoiceId: row.invoiceId })
      MERGE (d)-[:BILLED_IN]->(i)
    `;

    let total = 0;
    await processJsonlFilesInFolders({
      datasetMap,
      folderNames: ["billing_document_items"],
      batchSize: BATCH_SIZE,
      mapRecord: (r) => {
        const deliveryId = r.referenceSdDocument;
        const invoiceId = r.billingDocument;
        if (!deliveryId || !invoiceId) return null;
        return sanitizeRecord({
          deliveryId: String(deliveryId),
          invoiceId: String(invoiceId),
        });
      },
      onBatch: async (batch) => {
        total += batch.length;
        await session.executeWrite(async (tx) => {
          await tx.run(cypher, { batch });
        });
      },
    });

    console.log(`  BILLED_IN linked rows: ${total.toLocaleString()}`);
  }

  // Invoice -> Payment (PAID_BY)
  {
    const cypher = `
      UNWIND $batch AS row
      MATCH (i:Invoice { invoiceId: row.invoiceId })
      MATCH (p:Payment { paymentId: row.paymentId })
      MERGE (i)-[:PAID_BY]->(p)
    `;

    let total = 0;
    await processJsonlFilesInFolders({
      datasetMap,
      folderNames: ["payments_accounts_receivable"],
      batchSize: BATCH_SIZE,
      mapRecord: (r) => {
        const paymentId = r.accountingDocument;
        const invoiceId = r.invoiceReference;
        if (!paymentId || !invoiceId) return null;
        return sanitizeRecord({
          paymentId: String(paymentId),
          invoiceId: String(invoiceId),
        });
      },
      onBatch: async (batch) => {
        total += batch.length;
        await session.executeWrite(async (tx) => {
          await tx.run(cypher, { batch });
        });
      },
    });

    console.log(`  PAID_BY linked rows: ${total.toLocaleString()}`);
  }

  // Customer -> Delivery (HAS_DELIVERY) derived via Order -> Delivery using delivery items
  // 1) Create Order -> Delivery edges
  {
    const cypher = `
      UNWIND $batch AS row
      MATCH (o:Order { orderId: row.orderId })
      MATCH (d:Delivery { deliveryId: row.deliveryId })
      MERGE (o)-[:HAS_DELIVERY]->(d)
    `;

    await processJsonlFilesInFolders({
      datasetMap,
      folderNames: ["outbound_delivery_items"],
      batchSize: BATCH_SIZE,
      mapRecord: (r) => {
        const orderId = r.referenceSdDocument;
        const deliveryId = r.deliveryDocument;
        if (!orderId || !deliveryId) return null;
        return sanitizeRecord({
          orderId: String(orderId),
          deliveryId: String(deliveryId),
        });
      },
      onBatch: async (batch) => {
        await session.executeWrite(async (tx) => {
          await tx.run(cypher, { batch });
        });
      },
    });
  }

  // 2) Customer -> Delivery using Customer on Order
  {
    const cypher = `
      MATCH (o:Order)-[:HAS_DELIVERY]->(d:Delivery)
      MATCH (c:Customer { customerId: o.customerId })
      MERGE (c)-[:HAS_DELIVERY]->(d)
    `;
    await session.executeWrite(async (tx) => {
      await tx.run(cypher, {});
    });
  }

  // Customer -> Address (HAS_ADDRESS)
  {
    const cypher = `
      UNWIND $batch AS row
      MATCH (c:Customer { customerId: row.customerId })
      MATCH (a:Address { addressId: row.addressId })
      MERGE (c)-[:HAS_ADDRESS]->(a)
    `;

    let total = 0;
    await processJsonlFilesInFolders({
      datasetMap,
      folderNames: ["business_partner_addresses"],
      batchSize: BATCH_SIZE,
      mapRecord: (r) => {
        const customerId = r.businessPartner;
        const addressId = r.addressId;
        if (!customerId || !addressId) return null;
        return sanitizeRecord({
          customerId: String(customerId),
          addressId: String(addressId),
        });
      },
      onBatch: async (batch) => {
        total += batch.length;
        await session.executeWrite(async (tx) => {
          await tx.run(cypher, { batch });
        });
      },
    });

    console.log(`  HAS_ADDRESS (Customer) linked rows: ${total.toLocaleString()}`);
  }

  // Plant -> Address (HAS_ADDRESS)
  {
    const cypher = `
      UNWIND $batch AS row
      MATCH (pl:Plant { plantId: row.plantId })
      MATCH (a:Address { addressId: row.addressId })
      MERGE (pl)-[:HAS_ADDRESS]->(a)
    `;

    let total = 0;
    await processJsonlFilesInFolders({
      datasetMap,
      folderNames: ["plants"],
      batchSize: BATCH_SIZE,
      mapRecord: (r) => {
        if (!r.plant || !r.addressId) return null;
        return sanitizeRecord({
          plantId: String(r.plant),
          addressId: String(r.addressId),
        });
      },
      onBatch: async (batch) => {
        total += batch.length;
        await session.executeWrite(async (tx) => {
          await tx.run(cypher, { batch });
        });
      },
    });

    console.log(`  HAS_ADDRESS (Plant) linked rows: ${total.toLocaleString()}`);
  }

  console.log("Done. Relationships linked.");
}

async function main() {
  console.log(`Dataset dir: ${DATASET_DIR}`);
  if (!fs.existsSync(DATASET_DIR)) {
    throw new Error(`DATASET_DIR not found: ${DATASET_DIR}`);
  }

  const datasetMap = await buildDatasetFileMap(DATASET_DIR);

  console.log(`Found ${datasetMap.size} folder(s) with .jsonl files under dataset.`);

  // Small hack for optional helpers; not used by all functions.
  globalThis.__datasetMap = datasetMap;

  const session = driver.session();

  try {
    await importCustomers(session, datasetMap);
    await importAddresses(session, datasetMap);
    await importProducts(session, datasetMap);
    await importPlants(session, datasetMap);
    await importOrders(session, datasetMap);
    await importOrderItems(session, datasetMap);
    await importDeliveries(session, datasetMap);
    await importInvoices(session, datasetMap);
    await importPayments(session, datasetMap);
    await linkRelationships(session, datasetMap);
  } finally {
    await session.close();
    await driver.close();
  }
}

main()
  .then(() => {
    console.log("Import script finished successfully.");
    process.exit(0);
  })
  .catch((err) => {
    console.error("Import script failed:", err);
    process.exit(1);
  });

// Usage: node scripts/import.js
// Requires ESM (package.json "type":"module") for `import driver from "../src/lib/neo4j.js"`.
import fs from "fs";
import path from "path";
import readline from "readline";
import { fileURLToPath } from "url";

import driver from "../src/lib/neo4j.js";

const DATASET_DIR = process.env.DATASET_DIR
  ? path.resolve(process.env.DATASET_DIR)
  : (() => {
      const scriptsDir = path.dirname(fileURLToPath(import.meta.url)); // .../scripts
      const projectRoot = path.resolve(scriptsDir, "..");
      return path.join(projectRoot, "dataset");
    })();

const BATCH_SIZE = Number.parseInt(process.env.IMPORT_BATCH_SIZE ?? "2000", 10);
const MAX_ERROR_SAMPLES = Number.parseInt(
  process.env.MAX_ERROR_SAMPLES ?? "5",
  10
);

function sanitizeValue(value) {
  if (value === null || value === undefined) return undefined;
  if (typeof value === "string") return value;
  if (typeof value === "number") return value;
  if (typeof value === "boolean") return value;
  // Neo4j doesn't accept arbitrary objects as property values.
  return JSON.stringify(value);
}

function sanitizeRecord(record) {
  const out = {};
  for (const [k, v] of Object.entries(record)) {
    const sanitized = sanitizeValue(v);
    if (sanitized !== undefined) out[k] = sanitized;
  }
  return out;
}

async function buildDatasetFileMap(rootDir) {
  const entries = await fs.promises.readdir(rootDir, { withFileTypes: true });
  const map = new Map(); // folderName => string[]

  for (const entry of entries) {
    if (!entry.isDirectory()) continue;
    const folderName = entry.name;
    const folderPath = path.join(rootDir, folderName);
    const files = await fs.promises.readdir(folderPath, { withFileTypes: true });
    const jsonlFiles = files
      .filter((f) => f.isFile() && f.name.endsWith(".jsonl"))
      .map((f) => path.join(folderPath, f.name));

    // Keep even empty folders out of the map.
    if (jsonlFiles.length > 0) map.set(folderName, jsonlFiles);
  }

  return map;
}

async function* readJsonlFileInBatches(filePath, { batchSize, mapRecord }) {
  const rl = readline.createInterface({
    input: fs.createReadStream(filePath, { encoding: "utf8" }),
    crlfDelay: Infinity,
  });

  let batch = [];
  let lineNumber = 0;
  const errors = [];

  for await (const line of rl) {
    lineNumber += 1;
    const trimmed = line.trim();
    if (!trimmed) continue;

    let parsed;
    try {
      parsed = JSON.parse(trimmed);
    } catch (e) {
      if (errors.length < MAX_ERROR_SAMPLES) {
        errors.push({
          filePath,
          lineNumber,
          error: e instanceof Error ? e.message : String(e),
        });
      }
      continue;
    }

    const mapped = mapRecord(parsed);
    if (mapped) {
      batch.push(mapped);
      if (batch.length >= batchSize) {
        yield batch;
        batch = [];
      }
    }
  }

  if (batch.length > 0) {
    yield batch;
  }

  if (errors.length > 0) {
    console.warn(
      `JSON parse warnings for ${filePath}: ${errors.length} sample(s) (first sample line ${errors[0].lineNumber})`
    );
  }
}

async function processJsonlFilesInFolders({
  datasetMap,
  folderNames,
  batchSize,
  mapRecord,
  onBatch,
}) {
  for (const folderName of folderNames) {
    const files = datasetMap.get(folderName);
    if (!files) continue;

    for (const filePath of files) {
      const base = path.basename(filePath);
      console.log(`  - ${folderName}/${base}`);

      let processed = 0;
      let batches = 0;

      for await (const batch of readJsonlFileInBatches(filePath, { batchSize, mapRecord })) {
        batches += 1;
        processed += batch.length;
        await onBatch(batch);
      }

      console.log(`    Imported ${processed.toLocaleString()} records in ${batches} batch(es).`);
    }
  }
}

async function importCustomers(session, datasetMap) {
  // business_partners => Customer
  // key: customerId (from `customer` or `businessPartner`)
  // Also keeps full properties for later analytics.
  console.log("Importing Customers...");

  const cypher = `
    UNWIND $batch AS row
    MERGE (n:Customer { customerId: row.customerId })
    SET n += row
  `;

  let total = 0;
  await processJsonlFilesInFolders({
    datasetMap,
    folderNames: ["business_partners"],
    batchSize: BATCH_SIZE,
    mapRecord: (r) => {
      const customerId = r.customer ?? r.businessPartner;
      if (!customerId) return null;
      return sanitizeRecord({
        customerId,
        ...r,
      });
    },
    onBatch: async (batch) => {
      total += batch.length;
      await session.executeWrite(async (tx) => {
        await tx.run(cypher, { batch });
      });
    },
  });

  console.log(`Done. Customers imported: ${total.toLocaleString()}`);
}

async function importOrders(session, datasetMap) {
  // sales_order_headers => Order + OrderItem (nodes only)
  // key: orderId (from `salesOrder`)
  // OrderItem nodes: sales_order_items => OrderItem
  console.log("Importing Orders...");

  const cypherOrders = `
    UNWIND $batch AS row
    MERGE (n:Order { orderId: row.orderId })
    SET n += row
  `;

  let totalOrders = 0;
  await processJsonlFilesInFolders({
    datasetMap,
    folderNames: ["sales_order_headers"],
    batchSize: BATCH_SIZE,
    mapRecord: (r) => {
      const orderId = r.salesOrder;
      if (!orderId) return null;
      return sanitizeRecord({
        orderId,
        customerId: r.soldToParty ?? null,
        ...r,
      });
    },
    onBatch: async (batch) => {
      totalOrders += batch.length;
      await session.executeWrite(async (tx) => {
        await tx.run(cypherOrders, { batch });
      });
    },
  });

  console.log(`Done. Orders imported: ${totalOrders.toLocaleString()}`);
}

async function importDeliveries(session, datasetMap) {
  // outbound_delivery_headers => Delivery
  // key: deliveryId (from `deliveryDocument`)
  console.log("Importing Deliveries...");

  const cypher = `
    UNWIND $batch AS row
    MERGE (n:Delivery { deliveryId: row.deliveryId })
    SET n += row
  `;

  let total = 0;
  await processJsonlFilesInFolders({
    datasetMap,
    folderNames: ["outbound_delivery_headers"],
    batchSize: BATCH_SIZE,
    mapRecord: (r) => {
      const deliveryId = r.deliveryDocument;
      if (!deliveryId) return null;
      return sanitizeRecord({
        deliveryId,
        ...r,
      });
    },
    onBatch: async (batch) => {
      total += batch.length;
      await session.executeWrite(async (tx) => {
        await tx.run(cypher, { batch });
      });
    },
  });

  console.log(`Done. Deliveries imported: ${total.toLocaleString()}`);
}

async function importInvoices(session, datasetMap) {
  // billing_document_headers => Invoice
  // key: invoiceId (from `billingDocument`)
  console.log("Importing Invoices...");

  const cypher = `
    UNWIND $batch AS row
    MERGE (n:Invoice { invoiceId: row.invoiceId })
    SET n += row
  `;

  let total = 0;
  await processJsonlFilesInFolders({
    datasetMap,
    folderNames: ["billing_document_headers"],
    batchSize: BATCH_SIZE,
    mapRecord: (r) => {
      const invoiceId = r.billingDocument;
      if (!invoiceId) return null;
      return sanitizeRecord({
        invoiceId,
        customerId: r.soldToParty ?? null,
        ...r,
      });
    },
    onBatch: async (batch) => {
      total += batch.length;
      await session.executeWrite(async (tx) => {
        await tx.run(cypher, { batch });
      });
    },
  });

  console.log(`Done. Invoices imported: ${total.toLocaleString()}`);
}

async function importPayments(session, datasetMap) {
  // payments_accounts_receivable => Payment
  // key: paymentId (from `accountingDocument`)
  console.log("Importing Payments...");

  const cypher = `
    UNWIND $batch AS row
    MERGE (n:Payment { paymentId: row.paymentId })
    SET n += row
  `;

  let total = 0;
  await processJsonlFilesInFolders({
    datasetMap,
    folderNames: ["payments_accounts_receivable"],
    batchSize: BATCH_SIZE,
    mapRecord: (r) => {
      const paymentId = r.accountingDocument;
      if (!paymentId) return null;
      return sanitizeRecord({
        paymentId,
        customerId: r.customer ?? null,
        // May be null in your dataset; importer will conditionally link if present.
        invoiceId: r.invoiceReference ?? null,
        ...r,
      });
    },
    onBatch: async (batch) => {
      total += batch.length;
      await session.executeWrite(async (tx) => {
        await tx.run(cypher, { batch });
      });
    },
  });

  console.log(`Done. Payments imported: ${total.toLocaleString()}`);
}

async function importProducts(session, datasetMap) {
  // products => Product
  // key: productId (from `product`)
  console.log("Importing Products...");

  const cypher = `
    UNWIND $batch AS row
    MERGE (n:Product { productId: row.productId })
    SET n += row
  `;

  let total = 0;
  await processJsonlFilesInFolders({
    datasetMap,
    folderNames: ["products"],
    batchSize: BATCH_SIZE,
    mapRecord: (r) => {
      const productId = r.product;
      if (!productId) return null;
      return sanitizeRecord({
        productId,
        ...r,
      });
    },
    onBatch: async (batch) => {
      total += batch.length;
      await session.executeWrite(async (tx) => {
        await tx.run(cypher, { batch });
      });
    },
  });

  console.log(`Done. Products imported: ${total.toLocaleString()}`);
}

async function importOrderItems(session, datasetMap) {
  // sales_order_items => OrderItem nodes
  // key: composite (orderId + itemId)
  console.log("Importing Order Items (nodes only)...");

  const cypher = `
    UNWIND $batch AS row
    MERGE (n:OrderItem { orderId: row.orderId, itemId: row.itemId })
    SET n += row
  `;

  let total = 0;
  await processJsonlFilesInFolders({
    datasetMap,
    folderNames: ["sales_order_items"],
    batchSize: BATCH_SIZE,
    mapRecord: (r) => {
      const orderId = r.salesOrder;
      const itemId = r.salesOrderItem;
      const productId = r.material;
      if (!orderId || !itemId || !productId) return null;
      return sanitizeRecord({
        orderId,
        itemId,
        productId,
        ...r,
      });
    },
    onBatch: async (batch) => {
      total += batch.length;
      await session.executeWrite(async (tx) => {
        await tx.run(cypher, { batch });
      });
    },
  });

  console.log(`Done. OrderItem nodes imported: ${total.toLocaleString()}`);
}

async function linkRelationships(session, datasetMap) {
  // Relationship edges:
  // (Order)-[:PLACED_BY]->(Customer)
  // (Order)-[:HAS_DELIVERY]->(Delivery)
  // (Delivery)-[:BILLED_IN]->(Invoice)
  // (Invoice)-[:PAID_BY]->(Payment)
  // (OrderItem)-[:FOR_PRODUCT]->(Product)

  console.log("Linking Relationships...");

  // 1) PLACED_BY
  {
    const cypher = `
      UNWIND $batch AS row
      MATCH (o:Order { orderId: row.orderId })
      MATCH (c:Customer { customerId: row.customerId })
      MERGE (o)-[:PLACED_BY]->(c)
    `;

    let total = 0;
    let batch = [];

    for await (const filesBatch of (async function* () {
      const folderNames = ["sales_order_headers"];
      for (const folderName of folderNames) {
        const files = datasetMap.get(folderName) ?? [];
        for (const filePath of files) {
          for await (const b of readJsonlFileInBatches(filePath, {
            batchSize: BATCH_SIZE,
            mapRecord: (r) => {
              const orderId = r.salesOrder;
              const customerId = r.soldToParty;
              if (!orderId || !customerId) return null;
              return sanitizeRecord({ orderId, customerId });
            },
          })) {
            yield b;
          }
        }
      }
    })()) {
      batch = filesBatch;
      total += batch.length;
      await session.executeWrite(async (tx) => {
        await tx.run(cypher, { batch });
      });
    }

    console.log(`  PLACED_BY linked for ${total.toLocaleString()} order(s).`);
  }

  // 2) HAS_DELIVERY
  {
    // outbound_delivery_items.referenceSdDocument is the sales order id
    const cypher = `
      UNWIND $batch AS row
      MATCH (o:Order { orderId: row.orderId })
      MATCH (d:Delivery { deliveryId: row.deliveryId })
      MERGE (o)-[:HAS_DELIVERY]->(d)
    `;

    let total = 0;
    for await (const batch of (async function* () {
      const folderNames = ["outbound_delivery_items"];
      for (const folderName of folderNames) {
        const files = datasetMap.get(folderName) ?? [];
        for (const filePath of files) {
          for await (const b of readJsonlFileInBatches(filePath, {
            batchSize: BATCH_SIZE,
            mapRecord: (r) => {
              const orderId = r.referenceSdDocument;
              const deliveryId = r.deliveryDocument;
              if (!orderId || !deliveryId) return null;
              return sanitizeRecord({ orderId, deliveryId });
            },
          })) {
            yield b;
          }
        }
      }
    })()) {
      total += batch.length;
      await session.executeWrite(async (tx) => {
        await tx.run(cypher, { batch });
      });
    }

    console.log(`  HAS_DELIVERY linked for ${total.toLocaleString()} delivery-item(s).`);
  }

  // 3) BILLED_IN
  {
    // billing_document_items.referenceSdDocument is the delivery id
    const cypher = `
      UNWIND $batch AS row
      MATCH (d:Delivery { deliveryId: row.deliveryId })
      MATCH (i:Invoice { invoiceId: row.invoiceId })
      MERGE (d)-[:BILLED_IN]->(i)
    `;

    let total = 0;
    for await (const batch of (async function* () {
      const folderNames = ["billing_document_items"];
      for (const folderName of folderNames) {
        const files = datasetMap.get(folderName) ?? [];
        for (const filePath of files) {
          for await (const b of readJsonlFileInBatches(filePath, {
            batchSize: BATCH_SIZE,
            mapRecord: (r) => {
              const deliveryId = r.referenceSdDocument;
              const invoiceId = r.billingDocument;
              if (!deliveryId || !invoiceId) return null;
              return sanitizeRecord({ deliveryId, invoiceId });
            },
          })) {
            yield b;
          }
        }
      }
    })()) {
      total += batch.length;
      await session.executeWrite(async (tx) => {
        await tx.run(cypher, { batch });
      });
    }

    console.log(`  BILLED_IN linked for ${total.toLocaleString()} billing-item(s).`);
  }

  // 4) PAID_BY
  {
    // payments_accounts_receivable.invoiceReference is expected to reference billingDocument/invoiceId.
    const cypher = `
      UNWIND $batch AS row
      MATCH (i:Invoice { invoiceId: row.invoiceId })
      MATCH (p:Payment { paymentId: row.paymentId })
      MERGE (i)-[:PAID_BY]->(p)
    `;

    let total = 0;
    for await (const batch of (async function* () {
      const folderNames = ["payments_accounts_receivable"];
      for (const folderName of folderNames) {
        const files = datasetMap.get(folderName) ?? [];
        for (const filePath of files) {
          for await (const b of readJsonlFileInBatches(filePath, {
            batchSize: BATCH_SIZE,
            mapRecord: (r) => {
              const paymentId = r.accountingDocument;
              const invoiceId = r.invoiceReference;
              if (!paymentId || !invoiceId) return null;
              return sanitizeRecord({ paymentId, invoiceId });
            },
          })) {
            yield b;
          }
        }
      }
    })()) {
      total += batch.length;
      await session.executeWrite(async (tx) => {
        await tx.run(cypher, { batch });
      });
    }

    console.log(`  PAID_BY linked for ${total.toLocaleString()} payment(s) with invoice reference.`);
  }

  // 5) FOR_PRODUCT
  {
    const cypher = `
      UNWIND $batch AS row
      MATCH (oi:OrderItem { orderId: row.orderId, itemId: row.itemId })
      MATCH (p:Product { productId: row.productId })
      MERGE (oi)-[:FOR_PRODUCT]->(p)
    `;

    let total = 0;
    for await (const batch of (async function* () {
      const folderNames = ["sales_order_items"];
      for (const folderName of folderNames) {
        const files = datasetMap.get(folderName) ?? [];
        for (const filePath of files) {
          for await (const b of readJsonlFileInBatches(filePath, {
            batchSize: BATCH_SIZE,
            mapRecord: (r) => {
              const orderId = r.salesOrder;
              const itemId = r.salesOrderItem;
              const productId = r.material;
              if (!orderId || !itemId || !productId) return null;
              return sanitizeRecord({ orderId, itemId, productId });
            },
          })) {
            yield b;
          }
        }
      }
    })()) {
      total += batch.length;
      await session.executeWrite(async (tx) => {
        await tx.run(cypher, { batch });
      });
    }

    console.log(`  FOR_PRODUCT linked for ${total.toLocaleString()} order item(s).`);
  }

  console.log("Done. Relationships linked.");
}

async function main() {
  console.log(`Dataset dir: ${DATASET_DIR}`);
  const exists = fs.existsSync(DATASET_DIR);
  if (!exists) throw new Error(`DATASET_DIR not found: ${DATASET_DIR}`);

  const datasetMap = await buildDatasetFileMap(DATASET_DIR);

  console.log(
    `Found ${Array.from(datasetMap.keys()).length} folder(s) with .jsonl files under dataset.`
  );

  const session = driver.session();

  try {
    await importCustomers(session, datasetMap);
    await importOrders(session, datasetMap);
    await importDeliveries(session, datasetMap);
    await importInvoices(session, datasetMap);
    await importPayments(session, datasetMap);
    await importProducts(session, datasetMap);
    await importOrderItems(session, datasetMap);
    await linkRelationships(session, datasetMap);
  } finally {
    await session.close();
    await driver.close();
  }
}

main()
  .then(() => {
    console.log("Import script finished successfully.");
    process.exit(0);
  })
  .catch((err) => {
    console.error("Import script failed:", err);
    process.exit(1);
  });

