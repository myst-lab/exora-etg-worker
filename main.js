import { spawn } from "node:child_process";
import readline from "node:readline";
import { createDecompressStream } from "@mongodb-js/zstd";

function must(name) {
  const v = process.env[name];
  if (!v) throw new Error(`Missing env var: ${name}`);
  return v;
}

// ===== ETG Gateway =====
const ETG_GATEWAY_URL = must("ETG_GATEWAY_URL");       // http://35.195.210.59:8080
const ETG_GATEWAY_TOKEN = must("ETG_GATEWAY_TOKEN");   // your token
const ETG_TARGET = must("ETG_TARGET");                 // sandbox
const ETG_DUMP_PATH = must("ETG_DUMP_PATH");           // hotel/info/dump/
const ETG_LANGUAGE = must("ETG_LANGUAGE");             // en
const ETG_INVENTORY = must("ETG_INVENTORY");           // direct_fast or all

// ===== Supabase Edge Functions (Ratehawk) =====
const RATEHAWK_BATCH_UPSERT_URL = must("RATEHAWK_BATCH_UPSERT_URL");
// example: https://ibieannzbpwoamcowznu.supabase.co/functions/v1/ratehawk-hotels-batch-upsert

const RATEHAWK_DUMP_COMPLETE_URL = process.env.RATEHAWK_DUMP_COMPLETE_URL || null;
// example: https://ibieannzbpwoamcowznu.supabase.co/functions/v1/ratehawk-dump-complete

// If functions require auth, set this; if verify_jwt=false, leave empty
const SYNC_API_TOKEN = process.env.SYNC_API_TOKEN || null;

const BATCH_SIZE = Number(process.env.BATCH_SIZE || 500);
const LOG_EVERY = Number(process.env.LOG_EVERY || 5000);

function authHeaders() {
  return SYNC_API_TOKEN ? { Authorization: `Bearer ${SYNC_API_TOKEN}` } : {};
}

async function getDumpUrlViaGateway() {
  console.log("🔐 Fetching dump URL via gateway...");

  const res = await fetch(`${ETG_GATEWAY_URL}/etg/proxy`, {
    method: "POST",
    headers: {
      "x-internal-token": ETG_GATEWAY_TOKEN,
      "Content-Type": "application/json",
      "Accept": "application/json"
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
  let json;
  try { json = JSON.parse(text); } catch { json = null; }

  if (!res.ok || !json?.data?.url) {
    throw new Error(`Gateway failed (${res.status}): ${text.slice(0, 300)}`);
  }

  return json.data.url;
}

async function upsertBatch(download_id, hotels, batch_index) {
  const res = await fetch(RATEHAWK_BATCH_UPSERT_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Accept": "application/json",
      ...authHeaders()
    },
    body: JSON.stringify({
      download_id,
      hotels,
      batch_index
    })
  });

  const text = await res.text();
  let json;
  try { json = JSON.parse(text); } catch { json = null; }

  if (!res.ok || (json && json.success === false)) {
    throw new Error(`Batch upsert failed (${res.status}): ${text.slice(0, 500)}`);
  }
  return json;
}

async function completeDump(download_id, total_hotels, duration_ms) {
  if (!RATEHAWK_DUMP_COMPLETE_URL) return;

  const res = await fetch(RATEHAWK_DUMP_COMPLETE_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Accept": "application/json",
      ...authHeaders()
    },
    body: JSON.stringify({
      download_id,
      total_hotels,
      duration_ms
    })
  });

  if (!res.ok) {
    console.log("⚠️ dump-complete failed:", await res.text());
  }
}

async function run() {
  const startTime = Date.now();
  console.log("🚀 Starting ETG sync");
  console.log("Inventory:", ETG_INVENTORY);

  const download_id = `render-${Date.now()}`;

  const dumpUrl = await getDumpUrlViaGateway();
  console.log("📦 Got dump URL:", dumpUrl);

  console.log("⬇️ Downloading dump stream...");
  const dumpRes = await fetch(dumpUrl);
  if (!dumpRes.ok) {
    throw new Error(`Dump download failed (${dumpRes.status}): ${(await dumpRes.text()).slice(0, 300)}`);
  }

  console.log("🔓 Streaming ZSTD decompression...");
  const decompressedStream = dumpRes.body.pipe(createDecompressStream());

  const rl = readline.createInterface({
    input: decompressedStream,
    crlfDelay: Infinity
  });

  let batch = [];
  let total = 0;
  let batchIndex = 0;

  console.log("🧾 Parsing JSONL + uploading batches...");
  for await (const line of rl) {
    if (!line || !line.trim()) continue;

    let obj;
    try { obj = JSON.parse(line); } catch { continue; }

    batch.push(obj);

    if (batch.length >= BATCH_SIZE) {
      const result = await upsertBatch(download_id, batch, batchIndex++);
      total += batch.length;
      batch = [];

      if (total % LOG_EVERY === 0) {
        console.log(`✅ Upserted ${total} (last upserted=${result?.upserted ?? "?"})`);
      }
    }
  }

  if (batch.length) {
    const result = await upsertBatch(download_id, batch, batchIndex);
    total += batch.length;
    console.log(`✅ Final batch (upserted=${result?.upserted ?? "?"})`);
  }

  const duration = Date.now() - startTime;
  await completeDump(download_id, total, duration);

  console.log(`🎉 Done. Total processed: ${total} in ${(duration / 1000).toFixed(1)}s`);
}

run().catch((err) => {
  console.error("❌ Fatal error:", err);
  process.exit(1);
});
