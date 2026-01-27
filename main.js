import { spawn } from "node:child_process";
import readline from "node:readline";

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
  const h = { "Content-Type": "application/json", "Accept": "application/json" };
  if (SYNC_API_TOKEN) h["Authorization"] = `Bearer ${SYNC_API_TOKEN}`;
  return h;
}

async function getDumpUrlViaGateway() {
  const res = await fetch(`${ETG_GATEWAY_URL}/etg/proxy`, {
    method: "POST",
    headers: {
      "x-internal-token": ETG_GATEWAY_TOKEN,
      "Content-Type": "application/json",
      "Accept": "application/json",
    },
    body: JSON.stringify({
      target: ETG_TARGET,
      path: ETG_DUMP_PATH,
      options: {
        method: "POST",
        body: { language: ETG_LANGUAGE, inventory: ETG_INVENTORY },
      },
    }),
  });

  const text = await res.text();
  let json;
  try { json = JSON.parse(text); } catch { json = null; }
  if (!res.ok || !json?.data?.url) throw new Error(`Gateway failed (${res.status}): ${text}`);
  return json.data.url;
}

async function upsertBatch(download_id, hotels, batch_index) {
  const res = await fetch(SUPABASE_UPSERT_URL, {
    method: "POST",
    headers: jsonHeaders(),
    body: JSON.stringify({ download_id, hotels, batch_index }),
  });
  if (!res.ok) throw new Error(`Upsert failed: ${await res.text()}`);
}

async function run() {
  console.log("🚀 Starting ETG sync");
  console.log("Inventory:", ETG_INVENTORY);

  const download_id = `render-${Date.now()}`;

  console.log("🔐 Fetching dump URL via gateway...");
  const dumpUrl = await getDumpUrlViaGateway();
  console.log("📦 Got dump URL:", dumpUrl);

  console.log("⬇️ Streaming dump download...");
  const dumpRes = await fetch(dumpUrl);

  // IMPORTANT: ensure we got the file, not an error page
  if (!dumpRes.ok) {
    const t = await dumpRes.text();
    throw new Error(`Dump download failed (${dumpRes.status}): ${t.slice(0, 500)}`);
  }
  const ctype = dumpRes.headers.get("content-type") || "";
  console.log("ℹ️ content-type:", ctype);

  console.log("🔓 Streaming zstd decompress...");
  const zstd = spawn("zstd", ["-d", "-c"]);

  dumpRes.body.pipe(zstd.stdin);

  zstd.stderr.on("data", (d) => {
    // zstd prints warnings/errors here
    console.log("zstd:", d.toString().trim());
  });

  const rl = readline.createInterface({ input: zstd.stdout, crlfDelay: Infinity });

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

      if (total % LOG_EVERY === 0) console.log(`✅ Upserted ${total}`);
    }
  }

  if (batch.length) {
    await upsertBatch(download_id, batch, batchIndex);
    total += batch.length;
  }

  // If zstd failed, it will exit non-zero
  const code = await new Promise((resolve) => zstd.on("close", resolve));
  if (code !== 0) throw new Error(`zstd exited with code ${code}`);

  console.log(`🎉 Done. Total processed: ${total}`);
}

run().catch((err) => {
  console.error("❌ Fatal error:", err);
  process.exit(1);
});
