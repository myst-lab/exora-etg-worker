import { fetch } from "undici";
import readline from "node:readline";
import { decompress } from "fzstd";

const must = (k) => {
  const v = process.env[k];
  if (!v) throw new Error(`Missing env var: ${k}`);
  return v;
};

// ---- ENV ----
const ETG_GATEWAY_URL = must("ETG_GATEWAY_URL");       // http://35.195.210.59:8080
const ETG_GATEWAY_TOKEN = must("ETG_GATEWAY_TOKEN");   // secret token
const ETG_TARGET = must("ETG_TARGET");                 // sandbox
const ETG_DUMP_PATH = must("ETG_DUMP_PATH");           // hotel/info/dump/
const ETG_LANGUAGE = must("ETG_LANGUAGE");             // en
const ETG_INVENTORY = must("ETG_INVENTORY");           // direct_fast or all

const SUPABASE_FUNCTION_URL = must("SUPABASE_FUNCTION_URL");
const SYNC_API_TOKEN = must("SYNC_API_TOKEN");

const BATCH_SIZE = Number(process.env.BATCH_SIZE || 500);
const LOG_EVERY = Number(process.env.LOG_EVERY || 5000);

// ---- Helpers ----
async function getDumpUrl() {
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
        body: {
          language: ETG_LANGUAGE,
          inventory: ETG_INVENTORY
        },
        headers: {
          "User-Agent": "ExoraApp/1.0 (Render Cron; Hotel Sync)",
          "Content-Type": "application/json",
          "Accept": "application/json"
        }
      }
    })
  });

  if (!res.ok) {
    const t = await res.text();
    throw new Error(`Gateway request failed: ${res.status} ${t}`);
  }

  const json = await res.json();
  const url = json?.data?.url;
  if (!url) throw new Error(`Failed to get dump URL: ${JSON.stringify(json)}`);
  return url;
}

async function callSync(body) {
  const res = await fetch(SUPABASE_FUNCTION_URL, {
    method: "POST",
    headers: {
      "Authorization": `Bearer ${SYNC_API_TOKEN}`,
      "Content-Type": "application/json",
      "Accept": "application/json"
    },
    body: JSON.stringify(body)
  });

  const text = await res.text();
  let json = {};
  try { json = JSON.parse(text); } catch { /* keep text */ }

  if (!res.ok || json?.success === false) {
    throw new Error(`Sync function failed (${res.status}): ${text}`);
  }
  return json;
}

async function startSync() {
  const start = await callSync({
    action: "start_sync",
    inventory: ETG_INVENTORY
  });

  // Some implementations may return download_id, some may not.
  // If yours doesn’t, we fall back to a static id.
  return start.download_id || "render-sync";
}

async function upsertBatch(download_id, hotels, batch_index) {
  await callSync({
    action: "upsert_batch",
    download_id,
    batch_index,
    total_batches: null,
    hotels
  });
}

async function completeSync(download_id, total_hotels) {
  await callSync({
    action: "complete_sync",
    download_id,
    total_hotels
  });
}

// ---- Main ----
async function run() {
  console.log("🚀 Starting ETG sync...");
  console.log(`Inventory: ${ETG_INVENTORY}`);

  // 1) Start sync session (optional but matches Lovable spec)
  let download_id = "render-sync";
  try {
    download_id = await startSync();
    console.log("🧾 download_id:", download_id);
  } catch (e) {
    console.log("⚠️ start_sync not supported or failed, continuing without it:", e.message);
  }

  // 2) Get signed dump URL via gateway
  console.log("🔑 Fetching dump URL via gateway...");
  const dumpUrl = await getDumpUrl();
  console.log("📦 Got dump URL:", dumpUrl);

  // 3) Download dump (compressed)
  console.log("⬇️ Downloading dump (.zst)...");
  const res = await fetch(dumpUrl);
  if (!res.ok) {
    const t = await res.text();
    throw new Error(`Dump download failed: ${res.status} ${t}`);
  }

  // 4) Decompress using pure JS fzstd
  console.log("🔓 Decompressing ZSTD...");
  const compressed = Buffer.from(await res.arrayBuffer());
  const decompressed = decompress(compressed); // Uint8Array

  console.log("🧾 Parsing JSONL and uploading batches...");
  const rl = readline.createInterface({
    input: decompressed.toString("utf8").split("\n")[Symbol.iterator](),
    crlfDelay: Infinity
  });

  let batch = [];
  let total = 0;
  let batchIndex = 0;

  for await (const line of rl) {
    if (!line || !line.trim()) continue;

    let obj;
    try {
      obj = JSON.parse(line);
    } catch (e) {
      // Skip bad lines rather than dying
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

  // 5) Complete sync (optional)
  try {
    await completeSync(download_id, total);
  } catch (e) {
    console.log("⚠️ complete_sync not supported or failed:", e.message);
  }

  console.log(`🎉 Done. Total hotels processed: ${total}`);
}

run().catch((err) => {
  console.error("❌ Fatal error:", err);
  process.exit(1);
});
