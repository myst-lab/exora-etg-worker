import readline from "node:readline";
import { createDecompressStream } from "@mongodb-js/zstd";

function must(k) {
  const v = process.env[k];
  if (!v) throw new Error(`Missing env var: ${k}`);
  return v;
}

// ===== ENV =====
const ETG_GATEWAY_URL = must("ETG_GATEWAY_URL");
const ETG_GATEWAY_TOKEN = must("ETG_GATEWAY_TOKEN");
const ETG_TARGET = must("ETG_TARGET");
const ETG_DUMP_PATH = must("ETG_DUMP_PATH");
const ETG_LANGUAGE = must("ETG_LANGUAGE");
const ETG_INVENTORY = must("ETG_INVENTORY");

const SUPABASE_FUNCTION_URL = must("SUPABASE_FUNCTION_URL");
const SYNC_API_TOKEN = must("SYNC_API_TOKEN");

const BATCH_SIZE = Number(process.env.BATCH_SIZE || 250);
const LOG_EVERY = Number(process.env.LOG_EVERY || 5000);

// ===== ETG =====
async function getDumpUrl() {
  console.log("🔐 Fetching dump URL via gateway...");

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
        body: {
          language: ETG_LANGUAGE,
          inventory: ETG_INVENTORY
        }
      }
    })
  });

  if (!res.ok) {
    throw new Error(`Gateway failed: ${res.status}`);
  }

  const json = await res.json();
  if (!json?.data?.url) throw new Error("No dump URL returned");

  return json.data.url;
}

// ===== SUPABASE =====
async function upsertBatch(hotels) {
  const res = await fetch(SUPABASE_FUNCTION_URL, {
    method: "POST",
    headers: {
      "Authorization": `Bearer ${SYNC_API_TOKEN}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify({ hotels })
  });

  if (!res.ok) {
    throw new Error(`Supabase error: ${await res.text()}`);
  }
}

// ===== MAIN =====
async function run() {
  console.log("🚀 Starting ETG sync");
  console.log("📦 Inventory:", ETG_INVENTORY);

  const dumpUrl = await getDumpUrl();
  console.log("📥 Dump URL:", dumpUrl);

  const res = await fetch(dumpUrl);
  if (!res.ok || !res.body) {
    throw new Error("Failed to download dump");
  }

  console.log("🧊 Streaming + decompressing ZSTD...");

  const rl = readline.createInterface({
    input: res.body.pipe(createDecompressStream()),
    crlfDelay: Infinity
  });

  let batch = [];
  let total = 0;

  for await (const line of rl) {
    if (!line) continue;

    batch.push(JSON.parse(line));

    if (batch.length >= BATCH_SIZE) {
      await upsertBatch(batch);
      total += batch.length;
      batch = [];

      if (total % LOG_EVERY === 0) {
        console.log(`✅ Upserted ${total}`);
      }
    }
  }

  if (batch.length) {
    await upsertBatch(batch);
    total += batch.length;
  }

  console.log(`🎉 DONE — Total hotels: ${total}`);
}

run().catch(err => {
  console.error("❌ FATAL:", err);
  process.exit(1);
});
