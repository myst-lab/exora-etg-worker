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

function jsonHeaders() {
  const h = {
    "Content-Type": "application/json",
    "Accept": "application/json"
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
      "Accept": "application/json"
    },
    body: JSON.stringify({
      target: ETG_TARGET,
      path: ETG_DUMP_PATH,
      options: {
        method: "POST",
        body: { language: ETG_LANGUAGE, inventory: ETG_INVENTORY },
        headers: {
          "Content-Type": "application/json",
          "Accept": "application/json"
        }
      }
    })
  });

  const text = await res.text();
  let json;
  try { json = JSON.parse(text); } catch { json = null; }

  if (!res.ok || !json?.data?.url) {
    throw new Error(`Gateway failed (${res.status}): ${text.slice(0, 1200)}`);
  }

  return json.data.url;
}

async function upsertBatch(download_id, hotels, batch_index) {
  const res = await fetch(SUPABASE_UPSERT_URL, {
    method: "POST",
    headers: jsonHeaders(),
    body: JSON.stringify({ download_id, hotels, batch_index })
  });

  const text = await res.text();
  if (!res.ok) throw new Error(`Upsert failed (${res.status}): ${text.slice(0, 1200)}`);

  let json = null;
  try { json = JSON.parse(text); } catch {}
  if (json && json.success === false) {
    throw new Error(`Upsert returned success=false: ${text.slice(0, 1200)}`);
  }
  return json;
}

async function run() {
  console.log("🚀 Starting ETG sync");
  console.log("Inventory:", ETG_INVENTORY);

  const download_id = `render-${Date.now()}`;

  const dumpUrl = await getDumpUrlViaGateway();
  console.log("📦 Got dump URL:", dumpUrl);

  console.log("⬇️ Downloading dump (.zst)...");
  const dumpRes = await fetch(dumpUrl, { redirect: "follow" });
  if (!dumpRes.ok) {
    const preview = await dumpRes.text().catch(() => "");
    throw new Error(`Dump download failed (${dumpRes.status}): ${preview.slice(0, 1200)}`);
  }

  const ct = (dumpRes.headers.get("content-type") || "").toLowerCase();
  if (ct.includes("text/html") || ct.includes("application/xml") || ct.includes("text/xml")) {
    const preview = await dumpRes.text().catch(() => "");
    throw new Error(
      `Dump URL returned HTML/XML not zstd (content-type=${ct}).\n` +
      `Preview:\n${preview.slice(0, 1200)}`
    );
  }

  if (!dumpRes.body) {
    throw new Error("Dump response has no body stream (unexpected).");
  }

  console.log("🔓 Decompressing ZSTD (streaming)...");
  const zstdStream = dumpRes.body.pipe(createDecompressStream());

  console.log("🧾 Parsing JSONL + uploading batches...");
  const rl = readline.createInterface({
    input: zstdStream,
    crlfDelay: Infinity
  });

  let batch = [];
  let total = 0;
  let batchIndex = 0;

  for await (const line of rl) {
    if (!line || !line.trim()) continue;

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

run().catch((err) => {
  console.error("❌ Fatal error:", err?.stack || err);
  process.exit(1);
});
