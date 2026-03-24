import { NextResponse } from "next/server";
import { testNeo4jConnection } from "@/lib/neo4j";

export const runtime = "nodejs";

export async function GET() {
  try {
    const data = await testNeo4jConnection();
    return NextResponse.json(data);
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json(
      {
        ok: false,
        error: message,
      },
      { status: 500 }
    );
  }
}

