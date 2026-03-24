// Usage: node scripts/import_v2.js
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
  const map = new Map();

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
      await session.executeWrite((tx) => tx.run(cypher, { batch }));
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
      await session.executeWrite((tx) => tx.run(cypher, { batch }));
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
      await session.executeWrite((tx) => tx.run(cypher, { batch }));
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
      await session.executeWrite((tx) => tx.run(cypher, { batch }));
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
      if (!r.salesOrder) return null;
      return sanitizeRecord({
        orderId: String(r.salesOrder),
        customerId: r.soldToParty ? String(r.soldToParty) : null,
        ...r,
      });
    },
    onBatch: async (batch) => {
      total += batch.length;
      await session.executeWrite((tx) => tx.run(cypher, { batch }));
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
      if (!r.salesOrder || !r.salesOrderItem || !r.material) return null;
      return sanitizeRecord({
        orderId: String(r.salesOrder),
        itemId: String(r.salesOrderItem),
        productId: String(r.material),
        ...r,
      });
    },
    onBatch: async (batch) => {
      total += batch.length;
      await session.executeWrite((tx) => tx.run(cypher, { batch }));
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
      if (!r.deliveryDocument) return null;
      return sanitizeRecord({ deliveryId: String(r.deliveryDocument), ...r });
    },
    onBatch: async (batch) => {
      total += batch.length;
      await session.executeWrite((tx) => tx.run(cypher, { batch }));
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
      if (!r.billingDocument) return null;
      return sanitizeRecord({
        invoiceId: String(r.billingDocument),
        customerId: r.soldToParty ? String(r.soldToParty) : null,
        ...r,
      });
    },
    onBatch: async (batch) => {
      total += batch.length;
      await session.executeWrite((tx) => tx.run(cypher, { batch }));
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
      if (!r.accountingDocument) return null;
      return sanitizeRecord({
        paymentId: String(r.accountingDocument),
        customerId: r.customer ? String(r.customer) : null,
        invoiceId: r.invoiceReference ? String(r.invoiceReference) : null,
        ...r,
      });
    },
    onBatch: async (batch) => {
      total += batch.length;
      await session.executeWrite((tx) => tx.run(cypher, { batch }));
    },
  });

  console.log(`Done. Payments imported: ${total.toLocaleString()}`);
}

async function collectOrderToCustomer(datasetMap) {
  const orderToCustomer = new Map();

  const folderNames = ["sales_order_headers"];
  for (const folderName of folderNames) {
    const files = datasetMap.get(folderName);
    if (!files) continue;

    for (const filePath of files) {
      for await (const batch of readJsonlFileInBatches(filePath, {
        batchSize: BATCH_SIZE,
        mapRecord: (r) => {
          if (!r.salesOrder || !r.soldToParty) return null;
          return { orderId: String(r.salesOrder), customerId: String(r.soldToParty) };
        },
      })) {
        for (const row of batch) orderToCustomer.set(row.orderId, row.customerId);
      }
    }
  }

  return orderToCustomer;
}

async function linkRelationships(session, datasetMap) {
  console.log("Linking Relationships...");

  const linkFromSalesOrderHeaders = async (cypher, mapRecord) => {
    await processJsonlFilesInFolders({
      datasetMap,
      folderNames: ["sales_order_headers"],
      batchSize: BATCH_SIZE,
      mapRecord,
      onBatch: async (batch) => session.executeWrite((tx) => tx.run(cypher, { batch })),
    });
  };

  // Order -> Customer
  await linkFromSalesOrderHeaders(
    `
      UNWIND $batch AS row
      MATCH (o:Order { orderId: row.orderId })
      MATCH (c:Customer { customerId: row.customerId })
      MERGE (o)-[:PLACED_BY]->(c)
    `,
    (r) => {
      if (!r.salesOrder || !r.soldToParty) return null;
      return sanitizeRecord({ orderId: String(r.salesOrder), customerId: String(r.soldToParty) });
    }
  );

  // Order -> OrderItem
  await processJsonlFilesInFolders({
    datasetMap,
    folderNames: ["sales_order_items"],
    batchSize: BATCH_SIZE,
    mapRecord: (r) => {
      if (!r.salesOrder || !r.salesOrderItem) return null;
      return sanitizeRecord({ orderId: String(r.salesOrder), itemId: String(r.salesOrderItem) });
    },
    onBatch: async (batch) =>
      session.executeWrite((tx) =>
        tx.run(
          `
          UNWIND $batch AS row
          MATCH (o:Order { orderId: row.orderId })
          MATCH (oi:OrderItem { orderId: row.orderId, itemId: row.itemId })
          MERGE (o)-[:HAS_ITEM]->(oi)
          `,
          { batch }
        )
      ),
  });

  // OrderItem -> Product
  await processJsonlFilesInFolders({
    datasetMap,
    folderNames: ["sales_order_items"],
    batchSize: BATCH_SIZE,
    mapRecord: (r) => {
      if (!r.salesOrder || !r.salesOrderItem || !r.material) return null;
      return sanitizeRecord({
        orderId: String(r.salesOrder),
        itemId: String(r.salesOrderItem),
        productId: String(r.material),
      });
    },
    onBatch: async (batch) =>
      session.executeWrite((tx) =>
        tx.run(
          `
          UNWIND $batch AS row
          MATCH (oi:OrderItem { orderId: row.orderId, itemId: row.itemId })
          MATCH (p:Product { productId: row.productId })
          MERGE (oi)-[:FOR_PRODUCT]->(p)
          `,
          { batch }
        )
      ),
  });

  // Order -> Delivery (HAS_DELIVERY)
  // Derived from outbound_delivery_items:
  //   - referenceSdDocument = sales order id (Order.orderId)
  //   - deliveryDocument = delivery id (Delivery.deliveryId)
  await processJsonlFilesInFolders({
    datasetMap,
    folderNames: ["outbound_delivery_items"],
    batchSize: BATCH_SIZE,
    mapRecord: (r) => {
      if (!r.referenceSdDocument || !r.deliveryDocument) return null;
      return sanitizeRecord({
        orderId: String(r.referenceSdDocument),
        deliveryId: String(r.deliveryDocument),
      });
    },
    onBatch: async (batch) =>
      session.executeWrite((tx) =>
        tx.run(
          `
          UNWIND $batch AS row
          MATCH (o:Order { orderId: row.orderId })
          MATCH (d:Delivery { deliveryId: row.deliveryId })
          MERGE (o)-[:HAS_DELIVERY]->(d)
          `,
          { batch }
        )
      ),
  });

  // Delivery -> Plant
  await processJsonlFilesInFolders({
    datasetMap,
    folderNames: ["outbound_delivery_items"],
    batchSize: BATCH_SIZE,
    mapRecord: (r) => {
      if (!r.deliveryDocument || !r.plant) return null;
      return sanitizeRecord({ deliveryId: String(r.deliveryDocument), plantId: String(r.plant) });
    },
    onBatch: async (batch) =>
      session.executeWrite((tx) =>
        tx.run(
          `
          UNWIND $batch AS row
          MATCH (d:Delivery { deliveryId: row.deliveryId })
          MATCH (pl:Plant { plantId: row.plantId })
          MERGE (d)-[:FROM_PLANT]->(pl)
          `,
          { batch }
        )
      ),
  });

  // Delivery -> Invoice
  await processJsonlFilesInFolders({
    datasetMap,
    folderNames: ["billing_document_items"],
    batchSize: BATCH_SIZE,
    mapRecord: (r) => {
      if (!r.referenceSdDocument || !r.billingDocument) return null;
      return sanitizeRecord({
        deliveryId: String(r.referenceSdDocument),
        invoiceId: String(r.billingDocument),
      });
    },
    onBatch: async (batch) =>
      session.executeWrite((tx) =>
        tx.run(
          `
          UNWIND $batch AS row
          MATCH (d:Delivery { deliveryId: row.deliveryId })
          MATCH (i:Invoice { invoiceId: row.invoiceId })
          MERGE (d)-[:BILLED_IN]->(i)
          `,
          { batch }
        )
      ),
  });

  // Invoice -> Payment
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
    onBatch: async (batch) =>
      session.executeWrite((tx) =>
        tx.run(
          `
          UNWIND $batch AS row
          MATCH (i:Invoice { invoiceId: row.invoiceId })
          MATCH (p:Payment { paymentId: row.paymentId })
          MERGE (i)-[:PAID_BY]->(p)
          `,
          { batch }
        )
      ),
  });

  // Customer -> Delivery (customer from order header, delivery from delivery items)
  const orderToCustomer = await collectOrderToCustomer(datasetMap);
  await processJsonlFilesInFolders({
    datasetMap,
    folderNames: ["outbound_delivery_items"],
    batchSize: BATCH_SIZE,
    mapRecord: (r) => {
      if (!r.referenceSdDocument || !r.deliveryDocument) return null;
      const customerId = orderToCustomer.get(String(r.referenceSdDocument));
      if (!customerId) return null;
      return sanitizeRecord({
        customerId: String(customerId),
        deliveryId: String(r.deliveryDocument),
      });
    },
    onBatch: async (batch) =>
      session.executeWrite((tx) =>
        tx.run(
          `
          UNWIND $batch AS row
          MATCH (c:Customer { customerId: row.customerId })
          MATCH (d:Delivery { deliveryId: row.deliveryId })
          MERGE (c)-[:HAS_DELIVERY]->(d)
          `,
          { batch }
        )
      ),
  });

  // Customer -> Address
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
    onBatch: async (batch) =>
      session.executeWrite((tx) =>
        tx.run(
          `
          UNWIND $batch AS row
          MATCH (c:Customer { customerId: row.customerId })
          MATCH (a:Address { addressId: row.addressId })
          MERGE (c)-[:HAS_ADDRESS]->(a)
          `,
          { batch }
        )
      ),
  });

  // Plant -> Address
  await processJsonlFilesInFolders({
    datasetMap,
    folderNames: ["plants"],
    batchSize: BATCH_SIZE,
    mapRecord: (r) => {
      if (!r.plant || !r.addressId) return null;
      return sanitizeRecord({ plantId: String(r.plant), addressId: String(r.addressId) });
    },
    onBatch: async (batch) =>
      session.executeWrite((tx) =>
        tx.run(
          `
          UNWIND $batch AS row
          MATCH (pl:Plant { plantId: row.plantId })
          MATCH (a:Address { addressId: row.addressId })
          MERGE (pl)-[:HAS_ADDRESS]->(a)
          `,
          { batch }
        )
      ),
  });

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

