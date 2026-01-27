import { spawn } from "node:child_process";
import readline from "node:readline";

function must(k) {
  const v = process.env[k];
  if (!v) throw new Error(`Missing env var: ${k}`);
  return v;
}

const ETG_GATEWAY_URL = must("ETG_GATEWAY_URL");       // http://35.195.210.59:8080
const ETG_GATEWAY_TOKEN = must("ETG_GATEWAY_TOKEN");
const ETG_TARGET = must("ETG_TARGET");                 // sandbox
const ETG_DUMP_PATH = must("ETG_DUMP_PATH");           // hotel/info/dump/
const ETG_LANGUAGE = must("ETG_LANGUAGE");             // en
const ETG_INVENTORY = must("ETG_INVENTORY");           // direct_fast or all

// Existing Ratehawk function (they told you it exists)
const RATEHAWK_BATCH_UPSERT_URL = must("RATEHAWK_BATCH_UPSERT_URL"); 
// example: https://ibieannzbpwoamcowznu.supabase.co/functions/v1/ratehawk-hotels-batch-upsert

const SYNC_API_TOKEN = process.env.SYNC_API_TOKEN || null;

const BATCH_SIZE = Number(process.env.BATCH_SIZE || 500);
const LOG_EVERY = Number(process.env.LOG_EVERY || 5000);

function headersJson() {
  const h = { "Content-Type": "application/json", "Accept": "application/json" };
  if (SYNC_API_TOKEN) h.Authorization = `Bearer ${SYNC_API_TOKEN}`;
  return h;
}

async function getDumpUrlViaGateway() {
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
  if (!res.ok || !json?.data?.url) throw new Error(`Gateway failed (${res.status}): ${text.slice(0, 500)}`);
  return json.data.url;
}

async function upsertBatch(download_id, hotels, batch_index) {
  const res = await fetch(RATEHAWK_BATCH_UPSERT_URL, {
    method: "POST",
    headers: headersJson(),
    body: JSON.stringify({ download_id, hotels, batch_index })
  });

  const text = await res.text();
  let json;
  try { json = JSON.parse(text); } catch { json = null; }
  if (!res.ok || (json && json.success === false)) {
    throw new Error(`Upsert failed (${res.status}): ${text.slice(0, 500)}`);
  }
  return json;
}

async function run() {
  console.log("🚀 Starting ETG sync:", { inventory: ETG_INVENTORY });

  const download_id = `render-${Date.now()}`;

  const dumpUrl = await getDumpUrlViaGateway();
  console.log("📦 Dump URL:", dumpUrl);

  const dumpRes = await fetch(dumpUrl);
  if (!dumpRes.ok) throw new Error(`Dump download failed (${dumpRes.status}): ${(await dumpRes.text()).slice(0, 500)}`);

  console.log("🔓 Streaming zstd decompress...");
  const zstd = spawn("zstd", ["-d", "-c"]);

  dumpRes.body.pipe(zstd.stdin);

  zstd.stderr.on("data", (d) => {
    const s = d.toString().trim();
    if (s) console.log("zstd:", s);
  });

  const rl = readline.createInterface({ input: zstd.stdout, crlfDelay: Infinity });

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

      if (total % LOG_EVERY === 0) console.log(`✅ Upserted ${total}`);
    }
  }

  if (batch.length) {
    await upsertBatch(download_id, batch, batchIndex);
    total += batch.length;
  }

  const code = await new Promise((resolve) => zstd.on("close", resolve));
  if (code !== 0) throw new Error(`zstd exited with code ${code}`);

  console.log(`🎉 Done. Total processed: ${total}`);
}

run().catch((err) => {
  console.error("❌ Fatal:", err);
  process.exit(1);
});
