import { NextResponse } from "next/server";
import OpenAI from "openai";

export const runtime = "nodejs";

const GROQ_DEFAULT_MODEL = "openai/gpt-oss-20b";

export async function GET() {
  const apiKey = process.env.GROQ_API_KEY;
  const model = process.env.GROQ_MODEL ?? GROQ_DEFAULT_MODEL;

  if (!apiKey) {
    return NextResponse.json(
      {
        ok: false,
        error: "Missing GROQ_API_KEY env var",
        model,
      },
      { status: 500 }
    );
  }

  try {
    const client = new OpenAI({
      apiKey,
      baseURL: "https://api.groq.com/openai/v1",
    });

    const response = await client.responses.create({
      model,
      input: "Return exactly: OK",
    });

    return NextResponse.json({
      ok: true,
      model,
      responseSample: String(response.output_text ?? "").trim(),
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json(
      {
        ok: false,
        model,
        rawStatus: err?.status ?? null,
        error: message,
      },
      { status: 500 }
    );
  }
}

