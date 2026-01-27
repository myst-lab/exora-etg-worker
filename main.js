import readline from "node:readline";
import { decompress } from "fzstd";

const must = (k) => {
  const v = process.env[k];
  if (!v) throw new Error(`Missing env var: ${k}`);
  return v;
};

const SUPABASE_FUNCTION_BASE = must("SUPABASE_FUNCTION_BASE"); // https://.../functions/v1

const ETG_INVENTORY = must("ETG_INVENTORY"); // direct_fast or all
const BATCH_SIZE = Number(process.env.BATCH_SIZE || 500);
const LOG_EVERY = Number(process.env.LOG_EVERY || 5000);

// Optional auth header if needed (most likely NOT needed if verify_jwt=false)
const SYNC_API_TOKEN = process.env.SYNC_API_TOKEN || null;

function headersJson() {
  const h = {
    "Content-Type": "application/json",
    "Accept": "application/json",
  };
  if (SYNC_API_TOKEN) h["Authorization"] = `Bearer ${SYNC_API_TOKEN}`;
  return h;
}

async function postJson(url, body) {
  const res = await fetch(url, {
    method: "POST",
    headers: headersJson(),
    body: JSON.stringify(body),
  });
  const text = await res.text();
  let json;
  try { json = JSON.parse(text); } catch { json = { raw: text }; }
  if (!res.ok || json?.success === false) {
    throw new Error(`POST ${url} failed (${res.status}): ${text}`);
  }
  return json;
}

async function startSession() {
  // 1) start + get dump_url + download_id
  const url = `${SUPABASE_FUNCTION_BASE}/ratehawk-dump-url`;
  const json = await postJson(url, { inventory: ETG_INVENTORY });

  const download_id = json.download_id;
  const sync_log_id = json.sync_log_id;
  const dump_url = json.dump_url;

  if (!download_id || !dump_url) {
    throw new Error(`Unexpected start response: ${JSON.stringify(json)}`);
  }

  return { download_id, sync_log_id, dump_url };
}

async function upsertBatch(download_id, hotels, batch_index, total_batches) {
  const url = `${SUPABASE_FUNCTION_BASE}/ratehawk-hotels-batch-upsert`;
  return await postJson(url, {
    download_id,
    hotels,
    batch_index,
    total_batches,
  });
}

async function completeSession(download_id, sync_log_id, total_hotels, duration_ms) {
  const url = `${SUPABASE_FUNCTION_BASE}/ratehawk-dump-complete`;
  return await postJson(url, {
    download_id,
    sync_log_id,
    total_hotels,
    duration_ms,
  });
}

async function run() {
  const startTime = Date.now();
  console.log("🚀 Starting ETG sync...");
  console.log("Inventory:", ETG_INVENTORY);

  // Step 1: get dump url + ids
  console.log("🔑 Starting session via ratehawk-dump-url...");
  const { download_id, sync_log_id, dump_url } = await startSession();
  console.log("🧾 download_id:", download_id);
  console.log("📦 dump_url:", dump_url);

  // Step 2: download compressed
  console.log("⬇️ Downloading dump (.zst)...");
  const dumpRes = await fetch(dump_url);
  if (!dumpRes.ok) throw new Error(`Dump download failed: ${dumpRes.status} ${await dumpRes.text()}`);

  // Step 3: decompress (pure JS)
  console.log("🔓 Decompressing ZSTD...");
  const compressed = Buffer.from(await dumpRes.arrayBuffer());
  const decompressed = decompress(compressed); // Uint8Array

  // Step 4: parse JSONL + upload batches
  console.log("🧾 Uploading batches...");
  const rl = readline.createInterface({
    input: decompressed.toString("utf8").split("\n")[Symbol.iterator](),
    crlfDelay: Infinity,
  });

  let batch = [];
  let total = 0;
  let batchIndex = 0;

  // We don’t know total_batches upfront without counting lines.
  // Send null — API says optional.
  const totalBatches = null;

  for await (const line of rl) {
    if (!line || !line.trim()) continue;

    let obj;
    try { obj = JSON.parse(line); } catch { continue; }

    batch.push(obj);

    if (batch.length >= BATCH_SIZE) {
      const result = await upsertBatch(download_id, batch, batchIndex, totalBatches);
      total += batch.length;
      batch = [];
      batchIndex++;

      if (total % LOG_EVERY === 0) console.log(`✅ Upserted ${total} (last upserted=${result.upserted ?? "?"})`);
    }
  }

  if (batch.length) {
    const result = await upsertBatch(download_id, batch, batchIndex, totalBatches);
    total += batch.length;
    console.log(`✅ Final batch upserted=${result.upserted ?? "?"}`);
  }

  // Step 5: complete
  const duration = Date.now() - startTime;
  console.log("🏁 Completing session...");
  await completeSession(download_id, sync_log_id, total, duration);

  console.log(`🎉 Done. Total hotels processed: ${total} in ${(duration / 1000).toFixed(1)}s`);
}

run().catch((err) => {
  console.error("❌ Fatal error:", err);
  process.exit(1);
});
import { fetch } from "undici";
import readline from "node:readline";
import { decompress } from "fzstd";

const must = (k) => {
  const v = process.env[k];
  if (!v) throw new Error(`Missing env var: ${k}`);
  return v;
};

const SUPABASE_FUNCTION_BASE = must("SUPABASE_FUNCTION_BASE"); // https://.../functions/v1

const ETG_INVENTORY = must("ETG_INVENTORY"); // direct_fast or all
const BATCH_SIZE = Number(process.env.BATCH_SIZE || 500);
const LOG_EVERY = Number(process.env.LOG_EVERY || 5000);

// Optional auth header if needed (most likely NOT needed if verify_jwt=false)
const SYNC_API_TOKEN = process.env.SYNC_API_TOKEN || null;

function headersJson() {
  const h = {
    "Content-Type": "application/json",
    "Accept": "application/json",
  };
  if (SYNC_API_TOKEN) h["Authorization"] = `Bearer ${SYNC_API_TOKEN}`;
  return h;
}

async function postJson(url, body) {
  const res = await fetch(url, {
    method: "POST",
    headers: headersJson(),
    body: JSON.stringify(body),
  });
  const text = await res.text();
  let json;
  try { json = JSON.parse(text); } catch { json = { raw: text }; }
  if (!res.ok || json?.success === false) {
    throw new Error(`POST ${url} failed (${res.status}): ${text}`);
  }
  return json;
}

async function startSession() {
  // 1) start + get dump_url + download_id
  const url = `${SUPABASE_FUNCTION_BASE}/ratehawk-dump-url`;
  const json = await postJson(url, { inventory: ETG_INVENTORY });

  const download_id = json.download_id;
  const sync_log_id = json.sync_log_id;
  const dump_url = json.dump_url;

  if (!download_id || !dump_url) {
    throw new Error(`Unexpected start response: ${JSON.stringify(json)}`);
  }

  return { download_id, sync_log_id, dump_url };
}

async function upsertBatch(download_id, hotels, batch_index, total_batches) {
  const url = `${SUPABASE_FUNCTION_BASE}/ratehawk-hotels-batch-upsert`;
  return await postJson(url, {
    download_id,
    hotels,
    batch_index,
    total_batches,
  });
}

async function completeSession(download_id, sync_log_id, total_hotels, duration_ms) {
  const url = `${SUPABASE_FUNCTION_BASE}/ratehawk-dump-complete`;
  return await postJson(url, {
    download_id,
    sync_log_id,
    total_hotels,
    duration_ms,
  });
}

async function run() {
  const startTime = Date.now();
  console.log("🚀 Starting ETG sync...");
  console.log("Inventory:", ETG_INVENTORY);

  // Step 1: get dump url + ids
  console.log("🔑 Starting session via ratehawk-dump-url...");
  const { download_id, sync_log_id, dump_url } = await startSession();
  console.log("🧾 download_id:", download_id);
  console.log("📦 dump_url:", dump_url);

  // Step 2: download compressed
  console.log("⬇️ Downloading dump (.zst)...");
  const dumpRes = await fetch(dump_url);
  if (!dumpRes.ok) throw new Error(`Dump download failed: ${dumpRes.status} ${await dumpRes.text()}`);

  // Step 3: decompress (pure JS)
  console.log("🔓 Decompressing ZSTD...");
  const compressed = Buffer.from(await dumpRes.arrayBuffer());
  const decompressed = decompress(compressed); // Uint8Array

  // Step 4: parse JSONL + upload batches
  console.log("🧾 Uploading batches...");
  const rl = readline.createInterface({
    input: decompressed.toString("utf8").split("\n")[Symbol.iterator](),
    crlfDelay: Infinity,
  });

  let batch = [];
  let total = 0;
  let batchIndex = 0;

  // We don’t know total_batches upfront without counting lines.
  // Send null — API says optional.
  const totalBatches = null;

  for await (const line of rl) {
    if (!line || !line.trim()) continue;

    let obj;
    try { obj = JSON.parse(line); } catch { continue; }

    batch.push(obj);

    if (batch.length >= BATCH_SIZE) {
      const result = await upsertBatch(download_id, batch, batchIndex, totalBatches);
      total += batch.length;
      batch = [];
      batchIndex++;

      if (total % LOG_EVERY === 0) console.log(`✅ Upserted ${total} (last upserted=${result.upserted ?? "?"})`);
    }
  }

  if (batch.length) {
    const result = await upsertBatch(download_id, batch, batchIndex, totalBatches);
    total += batch.length;
    console.log(`✅ Final batch upserted=${result.upserted ?? "?"}`);
  }

  // Step 5: complete
  const duration = Date.now() - startTime;
  console.log("🏁 Completing session...");
  await completeSession(download_id, sync_log_id, total, duration);

  console.log(`🎉 Done. Total hotels processed: ${total} in ${(duration / 1000).toFixed(1)}s`);
}

run().catch((err) => {
  console.error("❌ Fatal error:", err);
  process.exit(1);
});
