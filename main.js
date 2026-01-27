import readline from "node:readline";
import { decompress } from "fzstd";

function must(name) {
  const v = process.env[name];
  if (!v) throw new Error(`Missing env var: ${name}`);
  return v;
}

// ===== ENV =====
const ETG_GATEWAY_URL = must("ETG_GATEWAY_URL");       // http://35.195.210.59:8080
const ETG_GATEWAY_TOKEN = must("ETG_GATEWAY_TOKEN");   // secret
const ETG_TARGET = must("ETG_TARGET");                 // sandbox
const ETG_DUMP_PATH = must("ETG_DUMP_PATH");           // hotel/info/dump/
const ETG_LANGUAGE = must("ETG_LANGUAGE");             // en
const ETG_INVENTORY = must("ETG_INVENTORY");           // direct_fast or all

// Use existing deployed function:
const SUPABASE_UPSERT_URL = must("SUPABASE_UPSERT_URL"); // .../functions/v1/ratehawk-hotels-batch-upsert

// Optional: only if your function requires auth (many are verify_jwt=false)
const SYNC_API_TOKEN = process.env.SYNC_API_TOKEN || null;

const BATCH_SIZE = Number(process.env.BATCH_SIZE || 500);
const LOG_EVERY = Number(process.env.LOG_EVERY || 5000);

// ===== Helpers =====
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
  try { json = JSON.parse(text); } catch { json = {}; }

  if (!res.ok || !json?.data?.url) {
    throw new Error(`Gateway failed (${res.status}): ${text}`);
  }

  return json.data.url;
}

function headersJson() {
  const h = {
    "Content-Type": "application/json",
    "Accept": "application/json"
  };
  if (SYNC_API_TOKEN) h["Authorization"] = `Bearer ${SYNC_API_TOKEN}`;
  return h;
}

async function upsertBatch(download_id, hotels, batch_index) {
  const res = await fetch(SUPABASE_UPSERT_URL, {
    method: "POST",
    headers: headersJson(),
    body: JSON.stringify({
      download_id,
      hotels,
      batch_index
    })
  });

  const text = await res.text();
  let json;
  try { json = JSON.parse(text); } catch { json = {}; }

  if (!res.ok || json?.success === false) {
    throw new Error(`Upsert failed (${res.status}): ${text}`);
  }
  return json;
}

// ===== Main =====
async function run() {
  console.log("🚀 Starting ETG sync...");
  console.log("Inventory:", ETG_INVENTORY);

  // If your upsert function requires download_id tracking, use a simple id:
  // (ratehawk-dump-url would generate one, but this works if it’s optional)
  const download_id = `render-${Date.now()}`;

  const dumpUrl = await getDumpUrlViaGateway();
  console.log("📦 Got dump URL:", dumpUrl);

  console.log("⬇️ Downloading dump (.zst)...");
  const dumpRes = await fetch(dumpUrl);
  if (!dumpRes.ok) throw new Error(`Dump download failed: ${dumpRes.status} ${await dumpRes.text()}`);

  console.log("🔓 Decompressing ZSTD...");
  const compressed = Buffer.from(await dumpRes.arrayBuffer());
  const decompressed = decompress(compressed); // Uint8Array

  console.log("🧾 Parsing JSONL + uploading batches...");
  const rl = readline.createInterface({
    input: decompressed.toString("utf8").split("\n")[Symbol.iterator](),
    crlfDelay: Infinity
  });

  let batch = [];
  let total = 0;
  let batchIndex = 0;

  for await (cons
