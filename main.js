import { fetch } from "undici";
import readline from "node:readline";
import { createDecompressStream } from "@mongodb-js/zstd";

const must = (k) => {
  const v = process.env[k];
  if (!v) throw new Error(`Missing env var: ${k}`);
  return v;
};

const ETG_GATEWAY_URL = must("ETG_GATEWAY_URL");
const ETG_GATEWAY_TOKEN = must("ETG_GATEWAY_TOKEN");
const ETG_TARGET = must("ETG_TARGET");
const ETG_DUMP_PATH = must("ETG_DUMP_PATH");
const ETG_LANGUAGE = must("ETG_LANGUAGE");
const ETG_INVENTORY = must("ETG_INVENTORY");

const SUPABASE_FUNCTION_URL = must("SUPABASE_FUNCTION_URL");
const SYNC_API_TOKEN = must("SYNC_API_TOKEN");

const BATCH_SIZE = Number(process.env.BATCH_SIZE || 500);
const LOG_EVERY = Number(process.env.LOG_EVERY || 5000);

async function getDumpUrl() {
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

  const json = await res.json();
  if (!json?.data?.url) throw new Error("Failed to get dump URL");
  return json.data.url;
}

async function upsertBatch(hotels) {
  const res = await fetch(SUPABASE_FUNCTION_URL, {
    method: "POST",
    headers: {
      "Authorization": `Bearer ${SYNC_API_TOKEN}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      action: "upsert_batch",
      hotels
    })
  });

  if (!res.ok) {
    throw new Error(await res.text());
  }
}

async function run() {
  console.log("🚀 Starting ETG sync");
  const dumpUrl = await getDumpUrl();
  console.log("📦 Got dump URL");

  const res = await fetch(dumpUrl);
  const rl = readline.createInterface({
    input: res.body.pipe(createDecompressStream()),
    crlfDelay: Infinity
  });

  let batch = [];
  let total = 0;

  for await (const line of rl) {
    if (!line) continue;
    batch.push(JSON.parse(line));

    if (batch.length === BATCH_SIZE) {
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

  console.log(`🎉 Done. Total hotels: ${total}`);
}

run().catch(err => {
  console.error("❌ Fatal
