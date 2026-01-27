import readline from "node:readline";
import { decompress } from "fzstd";

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

// Hard guard: Render cron + fzstd cannot safely handle huge dumps
if (ETG_INVENTORY === "all") {
  throw new Error(
    "ETG_INVENTORY=all is too large for Render + fzstd (in-memory decompress). " +
    "Set ETG_INVENTORY=direct_fast for this cron, or move 'all' to an external worker (VM/Docker)."
  );
}

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
        body: { language: ETG_LANGUAGE, inventory: ETG_INVENTORY }
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
  let json;
  try { json = JSON.parse(text); } catch { json = null; }

  if (!res.ok || (json && json.success === false)) {
    throw new Error(`Upsert failed (${res.status}): ${text.slice(0, 1200)}`);
  }
  return json;
}

// Helps if signed URL sometimes returns HTML/XML error page
async function fetchWithRetry(url, tries = 3) {
  let lastErr;
  for (let i = 0; i < tries; i++) {
    try {
      const res = await fetch(url);
      if (!res.ok) {
        const preview = await res.text().catch(() => "");
        throw new Error(`HTTP ${res.status}: ${preview.slice(0, 800)}`);
      }
      return res;
    } catch (e) {
      lastErr = e;
      console.log(`⚠️ fetch retry ${i + 1}/${tries} failed:`, e?.message || e);
      await new Promise(r => setTimeout(r, 1000 * (i + 1)));
    }
  }
  throw lastErr;
}

async function run() {
  console.log("🚀 Starting ETG sync");
  console.log("Inventory:", ETG_INVENTORY);

  const download_id = `render-${Date.now()}`;

  const dumpUrl = await getDumpUrlViaGateway();
  console.log("📦 Got dump URL:", dumpUrl);

  console.log("⬇️ Downloading dump (.zst)...");
  const dumpRes = await fetchWithRetry(dumpUrl, 3);

  // If S3 returns an error page, fzstd will say "invalid zstd data"
  const ct = (dumpRes.headers.get("content-type") || "").toLowerCase();
  if (ct.includes("xml") || ct.includes("html")) {
    const preview = await dumpRes.text().catch(() => "");
    throw new Error(
      `Dump URL did not return zstd (content-type=${ct}). ` +
      `Signed URL likely expired/blocked.\nPreview:\n${preview.slice(0, 1200)}`
    );
  }

  console.log("📦 Reading compressed bytes...");
  const compressed = Buffer.from(await dumpRes.arrayBuffer());
  console.log("📦 Compressed size MB:", (compressed.length / 1024 / 1024).toFixed(2));

  console.log("🔓 Decompressing ZSTD...");
  let decompressed;
  try {
    decompressed = decompress(compressed);
  } catch (e) {
    throw new Error(`ZSTD decompress failed: ${e?.message || e}`);
  }

  console.log("✅ Decompressed bytes MB:", (decompressed.length / 1024 / 1024).toFixed(2));
  console.log("🧾 Parsing JSONL + uploading batches...");

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
