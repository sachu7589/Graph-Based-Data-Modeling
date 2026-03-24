import neo4j from "neo4j-driver";
import type { Driver, Session } from "neo4j-driver";

type Neo4jConfig = {
  uri: string;
  user: string;
  password: string;
  database?: string;
};

function getRequiredEnv(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required env var: ${name}`);
  }
  return value;
}

function getNeo4jUserFromEnv(): string {
  // Support both common names to make onboarding easier.
  // - `NEO4J_USER` (used by this project originally)
  // - `NEO4J_USERNAME` (common for Aura examples)
  return process.env.NEO4J_USER ?? getRequiredEnv("NEO4J_USERNAME");
}

function getNeo4jConfigFromEnv(): Neo4jConfig {
  return {
    uri: getRequiredEnv("NEO4J_URI"),
    user: getNeo4jUserFromEnv(),
    password: getRequiredEnv("NEO4J_PASSWORD"),
    database: process.env.NEO4J_DATABASE,
  };
}

export function getNeo4jDriver(): Driver {
  const config = getNeo4jConfigFromEnv();

  // Keep a single driver instance around during dev/hot-reload.
  const g = globalThis as unknown as {
    __neo4jDriver?: Driver;
    __neo4jDriverKey?: string;
  };

  const driverKey = JSON.stringify({
    uri: config.uri,
    user: config.user,
    // Intentionally include password to force re-init if it changes.
    password: config.password,
    database: config.database ?? "",
    disableLosslessIntegers: true,
  });

  if (!g.__neo4jDriver || g.__neo4jDriverKey !== driverKey) {
    g.__neo4jDriver = neo4j.driver(
      config.uri,
      neo4j.auth.basic(config.user, config.password),
      {
        // Avoid returning neo4j-driver Integer objects (which can cause serialization edge cases).
        disableLosslessIntegers: true,
      }
    );
    g.__neo4jDriverKey = driverKey;
  }

  return g.__neo4jDriver;
}

export function getNeo4jSession(): Session {
  const config = getNeo4jConfigFromEnv();
  const driver = getNeo4jDriver();
  return config.database
    ? driver.session({ database: config.database })
    : driver.session();
}

export async function testNeo4jConnection(): Promise<{
  ok: true;
  database: string | null;
  queryResult: unknown;
}> {
  const config = getNeo4jConfigFromEnv();
  const session = getNeo4jSession();

  try {
    const result = await session.run("RETURN 1 AS ok");
    const ok = result.records[0]?.get("ok") ?? null;

    return {
      ok: true,
      database: config.database ?? null,
      queryResult: ok,
    };
  } finally {
    await session.close();
  }
}

