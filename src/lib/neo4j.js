import fs from "fs";
import path from "path";
import neo4j from "neo4j-driver";
import dotenv from "dotenv";
import { fileURLToPath } from "url";

function getRequiredEnv(name) {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env var: ${name}`);
  return value;
}

function getNeo4jUser() {
  // Support both common env var names.
  return process.env.NEO4J_USER ?? process.env.NEO4J_USERNAME;
}

// If someone runs `node scripts/import.js`, Next won't auto-load `.env.local`,
// so we optionally load it here (only when NEO4J_URI is missing).
if (!process.env.NEO4J_URI) {
  const moduleDir = path.dirname(fileURLToPath(import.meta.url)); // .../src/lib
  const projectRoot = path.resolve(moduleDir, "../.."); // repo root

  const configured = process.env.DOTENV_PATH;
  const dotenvPath = configured
    ? path.isAbsolute(configured)
      ? configured
      : path.resolve(projectRoot, configured)
    : path.join(projectRoot, ".env.local");

  if (fs.existsSync(dotenvPath)) {
    dotenv.config({ path: dotenvPath });
  } else {
    const fallback = path.join(projectRoot, ".env");
    if (fs.existsSync(fallback)) dotenv.config({ path: fallback });
  }
}

const uri = getRequiredEnv("NEO4J_URI");
const user = getNeo4jUser();
if (!user) throw new Error("Missing required env var: NEO4J_USER (or NEO4J_USERNAME)");
const password = getRequiredEnv("NEO4J_PASSWORD");

const neo4jDriver = neo4j.driver(uri, neo4j.auth.basic(user, password), {
  // For large imports, safer integer handling avoids truncation surprises.
  disableLosslessIntegers: true,
});

const database = process.env.NEO4J_DATABASE;

// The importer expects `driver.session()` to exist.
const driver = {
  session(options) {
    if (database) return neo4jDriver.session({ ...(options ?? {}), database });
    return neo4jDriver.session(options);
  },
  close() {
    return neo4jDriver.close();
  },
};

export default driver;

