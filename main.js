import { fetch } from "undici";
import readline from "node:readline";
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

const SUPABASE_UPSERT_URL = must("SUPABASE_UPSERT_URL");
const SYNC_API_TOKEN = process.env.SYNC_API_TOKEN || null;

const BATCH_SIZE = Number(process.env.BATCH_SIZE || 500);
const LOG_EVERY = Number(process.env.LOG_EVERY || 5000);

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
        body: { language: ETG_LANGUAGE, inventory: ETG_INVENTORY }
      }
    })
  });

  const text = await res.text();
  let json;
  try { json = JSON.parse(text); } catch {}
  if (!res.ok || !json?.data?.url) {
    throw new Error(`Failed to get dump URL (${res.status}): ${text.slice(0, 300)}`);
  }
  return json.data.url;
}

async function upsertBatch(download_id, hotels, batch_index) {
  const res = await fetch(SUPABASE_UPSERT_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      ...(SYNC_API_TOKEN ? { Authorization: `Bearer ${SYNC_API_TOKEN}` } : {})
    },
    body: JSON.stringify({ download_id, hotels, batch_index })
  });

  if (!res.ok) {
    throw new Error(`Upsert failed: ${await res.text()}`);
  }
}

async function run() {
  console.log("🚀 Starting ETG sync");
  console.log("Inventory:", ETG_INVENTORY);

  const download_id = `render-${Date.now()}`;

  console.log("🔐 Fetching dump URL...");
  const dumpUrl = await getDumpUrl();
  console.log("📦 Dump URL:", dumpUrl);

  const res = await fetch(dumpUrl);
  if (!res.ok) {
    const t = await res.text();
    throw new Error(`Dump download failed (${res.status}): ${t.slice(0, 300)}`);
  }

  console.log("🔓 Streaming ZSTD decompression...");
  const decompressed = res.body.pipe(createDecompressStream());

  const rl = readline.createInterface({
    input: decompressed,
    crlfDelay: Infinity
  });

  let batch = [];
  let total = 0;
  let batchIndex = 0;

  for await (const line of rl) {
    if (!line?.trim()) continue;

    let obj;
    try { obj = JSON.parse(line); } catch { continue; }
    batch.push(obj);

    if (batch.length >= BATCH_SIZE) {
      await upsertBatch(download_id, batch, batchIndex++);
      total += batch.length;
      batch = [];

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

run().catch(err => {
  console.error("❌ Fatal error:", err);
  process.exit(1);
});
