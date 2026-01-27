import readline from "node:readline";
import { Readable } from "node:stream";
import { Transform } from "node:stream";
import { createDecompressStream } from "@mongodb-js/zstd";

function must(name) {
  const v = process.env[name];
  if (!v) throw new Error(`Missing env var: ${name}`);
  return v;
}

// Required env
const ETG_GATEWAY_URL = must("ETG_GATEWAY_URL");
const ETG_GATEWAY_TOKEN = must("ETG_GATEWAY_TOKEN");
const ETG_TARGET = must("ETG_TARGET");
const ETG_DUMP_PATH = must("ETG_DUMP_PATH");
const ETG_LANGUAGE = must("ETG_LANGUAGE");
const ETG_INVENTORY = must("ETG_INVENTORY");

const SUPABASE_UPSERT_URL = must("SUPABASE_UPSERT_URL");

// Optional env
const SYNC_API_TOKEN = process.env.SYNC_API_TOKEN || "";
const BATCH_SIZE = Number(process.env.BATCH_SIZE || 500);
const LOG_EVERY = Number(process.env.LOG_EVERY || 5000);

function jsonHeaders() {
  const h = { "Content-Type": "application/json", "Accept": "application/json" };
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
        body: { language: ETG_LANGUAGE, inventory: ETG_INVENTORY }
      }
    })
  });

  const text = await res.text();
  let json = null;
  try { json = JSON.parse(text); } catch {}

  if (!res.ok || !json?.data?.url) {
    throw new Error(`Gateway failed (${res.status}): ${text}`);
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
  let json = null;
  try { json = JSON.parse(text); } catch {}

  if (!res.ok || (json && json.success === false)) {
    throw new Error(`Upsert failed (${res.status}): ${text}`);
  }
  return json;
}

// Checks first bytes for ZSTD frame magic: 28 B5 2F FD
function zstdMagicGuard() {
  let checked = false;

  return new Transform({
    transform(chunk, _enc, cb) {
      if (!checked) {
        checked = true;
        const b = chunk instanceof Buffer ? chunk : Buffer.from(chunk);
        const hex = b.subarray(0, 8).toString("hex");
        const magic = b.subarray(0, 4).toString("hex"); // expect 28b52ffd

        if (magic !== "28b52ffd") {
          cb(
            new Error(
              `Downloaded data is NOT raw .zst (magic=${magic}, first8=${hex}). ` +
              `This usually means the URL returned HTML/JSON error OR HTTP compression changed the bytes.`
            )
          );
          return;
        }
      }
      cb(null, chunk);
    }
  });
}

async function run() {
  console.log("🚀 Starting ETG sync");
  console.log("Inventory:", ETG_INVENTORY);

  const download_id = `render-${Date.now()}`;

  const dumpUrl = await getDumpUrlViaGateway();
  console.log("📦 Got dump URL:", dumpUrl);

  console.log("⬇️ Downloading dump (.zst)...");
  const dumpRes = await fetch(dumpUrl, {
    // CRITICAL: ensure we get the raw bytes, not gzip/br-decoded content
    headers: { "Accept-Encoding": "identity" }
  });

  if (!dumpRes.ok) {
    const t = await dumpRes.text();
    throw new Error(`Dump download failed (${dumpRes.status}): ${t}`);
  }
  if (!dumpRes.body) throw new Error("Dump response has no body stream");

  // Convert Web ReadableStream -> Node Readable
  const nodeBody = Readable.fromWeb(dumpRes.body);

  console.log("🔓 Decompressing ZSTD (streaming)...");
  const decompressedStream = nodeBody
    .pipe(zstdMagicGuard())
    .pipe(createDecompressStream());

  console.log("🧾 Parsing JSONL + uploading batches...");
  const rl = readline.createInterface({
    input: decompressedStream,
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
