import { createDecompressStream } from "@mongodb-js/zstd";
import readline from "node:readline";
import { Readable } from "node:stream";

const must = (k) => {
  const v = process.env[k];
  if (!v) throw new Error(`Missing env var: ${k}`);
  return v;
};

// ETG Gateway
const ETG_GATEWAY_URL = must("ETG_GATEWAY_URL");       // e.g. http://35.195.210.59:8080
const ETG_GATEWAY_TOKEN = must("ETG_GATEWAY_TOKEN");   // x-internal-token
const ETG_TARGET = must("ETG_TARGET");                 // usually "sandbox" per your gateway mapping
const ETG_DUMP_PATH = must("ETG_DUMP_PATH");           // e.g. "hotel/info/dump/"
const ETG_LANGUAGE = must("ETG_LANGUAGE");             // "en"
const ETG_INVENTORY = must("ETG_INVENTORY");           // "all" or "direct_fast"

// Supabase Edge Function (existing function)
const SUPABASE_FUNCTION_URL = must("SUPABASE_FUNCTION_URL"); // FULL endpoint to upsert function
// e.g. https://xxxxx.supabase.co/functions/v1/ratehawk-hotels-batch-upsert

// Optional auth (only used if your function expects it)
const SYNC_API_TOKEN = process.env.SYNC_API_TOKEN || "";

// Batching
const BATCH_SIZE = Number(process.env.BATCH_SIZE || 200);     // start smaller, increase later
const LOG_EVERY = Number(process.env.LOG_EVERY || 5000);

// Helper: convert Web stream -> Node stream (required for zstd stream)
function toNodeReadable(webStream) {
  // Node 18+ supports this
  return Readable.fromWeb(webStream);
}

async function getDumpUrlViaGateway() {
  const res = await fetch(`${ETG_GATEWAY_URL}/etg/proxy`, {
    method: "POST",
    headers: {
      "x-internal-token": ETG_GATEWAY_TOKEN,
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      target: ETG_TARGET,
      path: ETG_DUMP_PATH,
      options: {
        method: "POST",
        body: { language: ETG_LANGUAGE, inventory: ETG_INVENTORY }
      }
    })
  });

  const text = await res.text();
  if (!res.ok) throw new Error(`Gateway error ${res.status}: ${text}`);

  const json = JSON.parse(text);
  const url = json?.data?.url;
  if (!url) throw new Error(`No dump url in gateway response: ${text}`);
  return url;
}

async function upsertBatch(hotels, meta = {}) {
  const headers = { "Content-Type": "application/json" };
  if (SYNC_API_TOKEN) headers["Authorization"] = `Bearer ${SYNC_API_TOKEN}`;

  const res = await fetch(SUPABASE_FUNCTION_URL, {
    method: "POST",
    headers,
    body: JSON.stringify({ hotels, ...meta })
  });

  if (!res.ok) {
    const t = await res.text();
    throw new Error(`Upsert failed ${res.status}: ${t}`);
  }
}

async function run() {
  console.log("🚀 Starting ETG sync…");
  console.log(`Inventory: ${ETG_INVENTORY}`);

  console.log("🔑 Fetching dump URL via gateway…");
  const dumpUrl = await getDumpUrlViaGateway();
  console.log("📦 Got dump URL:", dumpUrl);

  console.log("⬇️ Downloading dump (.zst)…");
  const res = await fetch(dumpUrl);
  if (!res.ok || !res.body) {
    throw new Error(`Dump download failed ${res.status}`);
  }

  console.log("🔓 Decompressing ZSTD (stream)…");
  const nodeReadable = toNodeReadable(res.body);
  const zstdStream = nodeReadable.pipe(createDecompressStream());

  const rl = readline.createInterface({
    input: zstdStream,
    crlfDelay: Infinity
  });

  let batch = [];
  let total = 0;

  for await (const line of rl) {
    if (!line) continue;

    // Each line should be JSON object
    const obj = JSON.parse(line);
    batch.push(obj);

    if (batch.length >= BATCH_SIZE) {
      await upsertBatch(batch, { inventory: ETG_INVENTORY });
      total += batch.length;
      batch = [];

      if (total % LOG_EVERY === 0) {
        console.log(`✅ Upserted ${total}`);
      }
    }
  }

  if (batch.length) {
    await upsertBatch(batch, { inventory: ETG_INVENTORY });
    total += batch.length;
  }

  console.log(`🎉 Done. Total hotels: ${total}`);
}

run().catch((err) => {
  console.error("❌ Fatal error:", err?.stack || err?.message || err);
  process.exit(1);
});
