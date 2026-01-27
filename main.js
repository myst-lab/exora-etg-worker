import readline from "node:readline";
import { createDecompressStream } from "@mongodb-js/zstd";

function must(name) {
  const v = process.env[name];
  if (!v) throw new Error(`Missing env var: ${name}`);
  return v;
}

const ETG_GATEWAY_URL = must("ETG_GATEWAY_URL");         // e.g. http://35.195.210.59:8080
const ETG_GATEWAY_TOKEN = must("ETG_GATEWAY_TOKEN");     // your token
const ETG_TARGET = must("ETG_TARGET");                   // sandbox or content (for dump use sandbox)
const ETG_DUMP_PATH = must("ETG_DUMP_PATH");             // hotel/info/dump/
const ETG_LANGUAGE = must("ETG_LANGUAGE");               // en
const ETG_INVENTORY = must("ETG_INVENTORY");             // all or direct_fast

// Your Supabase edge function endpoint that accepts { download_id, hotels, batch_index }
const SUPABASE_UPSERT_URL = must("SUPABASE_UPSERT_URL");

// Optional bearer token (only if your function requires it)
const SYNC_API_TOKEN = process.env.SYNC_API_TOKEN || "";

const BATCH_SIZE = Number(process.env.BATCH_SIZE || 500);
const LOG_EVERY = Number(process.env.LOG_EVERY || 5000);

function jsonHeaders() {
  const h = {
    "Content-Type": "application/json",
    "Accept": "application/json",
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
      "Accept": "application/json",
    },
    body: JSON.stringify({
      target: ETG_TARGET,
      path: ETG_DUMP_PATH,
      options: {
        method: "POST",
        body: { language: ETG_LANGUAGE, inventory: ETG_INVENTORY },
        headers: {
          "Content-Type": "application/json",
          "Accept": "application/json",
        },
      },
    }),
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
    body: JSON.stringify({ download_id, hotels, batch_index }),
  });

  const text = await res.text();
  let json = null;
  try { json = JSON.parse(text); } catch {}

  if (!res.ok || (json && json.success === false)) {
    throw new Error(`Upsert failed (${res.status}): ${text}`);
  }
  return json;
}

async function assertLooksLikeZstd(res, dumpUrl) {
  // If S3 returns an error, it often returns XML/HTML which will break zstd
  const ct = (res.headers.get("content-type") || "").toLowerCase();

  if (!res.ok) {
    const preview = await res.text().catch(() => "");
    throw new Error(`Dump download failed (${res.status}) ${dumpUrl}\n${preview.slice(0, 800)}`);
  }

  // not always set correctly, but useful for debugging
  if (ct.includes("text/html") || ct.includes("application/xml") || ct.includes("text/xml")) {
    const preview = await res.text().catch(() => "");
    throw new Error(
      `Dump URL did not return zstd data (content-type=${ct}). Probably expired/blocked signed URL.\n` +
      `URL: ${dumpUrl}\nPreview:\n${preview.slice(0, 800)}`
    );
  }
}

async function run() {
  console.log("🚀 Starting ETG sync");
  console.log("Inventory:", ETG_INVENTORY);

  // Unique id per run (you can change this to whatever your DB expects)
  const download_id = `render-${Date.now()}`;

  const dumpUrl = await getDumpUrlViaGateway();
  console.log("📦 Got dump URL:", dumpUrl);

  console.log("⬇️ Downloading dump (.zst) as stream...");
  const dumpRes = await fetch(dumpUrl);

  // Validate response BEFORE piping into zstd
  await assertLooksLikeZstd(dumpRes, dumpUrl);

  console.log("🔓 Decompressing ZSTD stream...");
  const decompressedStream = dumpRes.body.pipe(createDecompressStream());

  const rl = readline.createInterface({
    input: decompressedStream,
    crlfDelay: Infinity,
  });

  console.log("🧾 Parsing JSONL + uploading batches...");
  let batch = [];
  let total = 0;
  let batchIndex = 0;

  for await (const line of rl) {
    if (!line || !line.trim()) continue;

    let obj;
    try {
      obj = JSON.parse(line);
    } catch {
      continu
