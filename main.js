import readline from "node:readline";
import { Readable } from "node:stream";
import { createDecompressStream } from "@mongodb-js/zstd";

function must(name) {
  const v = process.env[name];
  if (!v) throw new Error(`Missing env var: ${name}`);
  return v;
}

const ETG_GATEWAY_URL = must("ETG_GATEWAY_URL");
const ETG_GATEWAY_TOKEN = must("ETG_GATEWAY_TOKEN");
const ETG_TARGET = must("ETG_TARGET");
const ETG_DUMP_PATH = must("ETG_DUMP_PATH");
const ETG_LANGUAGE = must("ETG_LANGUAGE");
const ETG_INVENTORY = must("ETG_INVENTORY");

// IMPORTANT: Set this to your REAL upsert edge function URL, e.g.
// https://<project>.supabase.co/functions/v1/ratehawk-hotels-batch-upsert
const SUPABASE_FUNCTION_URL = must("SUPABASE_FUNCTION_URL");

// Optional (if your function requires auth)
const SYNC_API_TOKEN = process.env.SYNC_API_TOKEN || "";

const BATCH_SIZE = Number(process.env.BATCH_SIZE || 500);
const LOG_EVERY = Number(process.env.LOG_EVERY || 5000);

function upsertHeaders() {
  const h = {
    "Content-Type": "application/json",
    "Accept": "application/json",
  };
  if (SYNC_API_TOKEN) h["Authorization"] = `Bearer ${SYNC_API_TOKEN}`;
  return h;
}

async function getDumpUrlViaGateway() {
  console.log("🔐 Fetching dump URL via gateway...");

  const res = await fetch(`${ETG_GATEWAY_URL}/etg/proxy`, {
    method: "POST",
    headers: {
      "x-internal-token": ETG_GATEWAY_TOKEN,
      "Content-Type": "application/json",
      "Accept": "application/json",
    },
    body: JSON.stringify({
      target: ETG_TARGET,
      path: ETG_DUMP_PATH,
      options: {
        method: "POST",
        body: { language: ETG_LANGUAGE, inventory: ETG_INVENTORY },
        headers: {
          "Content-Type": "application/json",
          "Accept": "application/json",
          "User-Agent": "exora-etg-worker (Render Cron)",
        },
      },
    }),
  });

  const text = await res.text();
  let json;
  try {
    json = JSON.parse(text);
  } catch {
    json = null;
  }

  if (!res.ok || !json?.data?.url) {
    throw new Error(`Gateway failed (${res.status}): ${text}`);
  }

  return json.data.url;
}

async function upsertBatch(download_id, hotels, batch_index) {
  const res = await fetch(SUPABASE_FUNCTION_URL, {
    method: "POST",
    headers: upsertHeaders(),
    body: JSON.stringify({
      download_id,
      hotels,
      batch_index,
    }),
  });

  const text = await res.text();
  let json;
  try {
    json = JSON.parse(text);
  } catch {
    json = null;
  }

  if (!res.ok) {
    throw new Error(`Upsert failed (${res.status}): ${text}`);
  }
  if (json && json.success === false) {
    throw new Error(`Upsert returned success=false: ${text}`);
  }

  return json;
}

async function fetchDumpStreamWithRetry(dumpUrl, retries = 1) {
  let lastErr = null;

  for (let attempt = 0; attempt <= retries; attempt++) {
    if (attempt > 0) console.log(`🔁 Retrying dump download (attempt ${attempt + 1})...`);

    try {
      const dumpRes = await fetch(dumpUrl, {
        method: "GET",
        headers: { "Accept": "*/*" },
      });

      if (!dumpRes.ok) {
        const body = await dumpRes.text().catch(() => "");
        throw new Error(`Dump download failed (${dumpRes.status}): ${body}`);
      }

      // Node 18+ fetch returns a Web ReadableStream. Convert it to Node stream:
      if (!dumpRes.body) throw new Error("Dump response has no body stream");

      const nodeReadable = Readable.fromWeb(dumpRes.body);
      return nodeReadable;
    } catch (e) {
      lastErr = e;
    }
  }

  throw lastErr;
}

async function run() {
  console.log("🚀 Starting ETG sync");
  console.log("Inventory:", ETG_INVENTORY);
  console.log("Batch size:", BATCH_SIZE);

  const download_id = `render-${Date.now()}`;

  const dumpUrl = await getDumpUrlViaGateway();
  console.log("📦 Got dump URL:", dumpUrl);
  console.log("⬇️ Streaming + decompressing ZSTD (no full-file buffering)...");

  // Stream download -> zstd stream -> readline
  const dumpNodeStream = await fetchDumpStreamWithRetry(dumpUrl, 1);
  const zstdStream = dumpNodeStream.pipe(createDecompressStream());

  const rl = readline.createInterface({
    input: zstdStream,
    crlfDelay: Infinity,
  });

  let batch = [];
  let total = 0;
  let batchIndex = 0;

  for await (const line of rl) {
    if (!line || !line.trim()) continue;

    let obj;
    try {
      obj = JSON.parse(line);
    } catch {
      // If any random bad line exists, skip it (safer than crashing whole job)
      continue;
    }

    batch.push(obj);

    if (batch.length >= BATCH_SIZE) {
      await upsertBatch(download_id, batch, batchIndex);
      total += batch.length;
      batch = [];
      batchIndex++;

      if (total % LOG_EVERY === 0) {
        console.log(`✅ Upserted ${total}`);
      }
    }
  }

  if (batch.length) {
    await upsertBatch(download_id, batch, batchIndex);
    total += batch.length;
  }

  console.log(`🎉 Done. Total processed: ${total}`);
}

run().catch((err) => {
  console.error("❌ Fatal error:", err?.stack || err);
  process.exit(1);
});
