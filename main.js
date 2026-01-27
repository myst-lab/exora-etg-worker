import readline from "node:readline";
import { Readable } from "node:stream";
import { DecompressStream } from "zstd-napi";

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

const SUPABASE_UPSERT_URL = must("SUPABASE_UPSERT_URL"); // e.g. .../ratehawk-hotels-batch-upsert
const SYNC_API_TOKEN = process.env.SYNC_API_TOKEN || null;

const BATCH_SIZE = Number(process.env.BATCH_SIZE || 500);
const LOG_EVERY = Number(process.env.LOG_EVERY || 5000);
const UPSERT_RETRIES = Number(process.env.UPSERT_RETRIES || 5);
const UPSERT_RETRY_MS = Number(process.env.UPSERT_RETRY_MS || 1500);

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
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
    throw new Error(`Gateway failed (${res.status}): ${text}`);
  }

  return json.data.url;
}

async function upsertBatch(download_id, hotels, batch_index) {
  const payload = { download_id, hotels, batch_index };

  for (let attempt = 1; attempt <= UPSERT_RETRIES; attempt++) {
    const res = await fetch(SUPABASE_UPSERT_URL, {
      method: "POST",
      headers: jsonHeaders(),
      body: JSON.stringify(payload)
    });

    const text = await res.text();
    let json;
    try { json = JSON.parse(text); } catch { json = null; }

    if (res.ok && (!json || json.success !== false)) return json;

    // Retry on common transient statuses
    const retryable = [408, 429, 500, 502, 503, 504].includes(res.status);
    const msg = `Upsert failed (${res.status}) attempt ${attempt}/${UPSERT_RETRIES}: ${text}`;

    if (!retryable || attempt === UPSERT_RETRIES) {
      throw new Error(msg);
    }

    console.warn("⚠️", msg);
    await sleep(UPSERT_RETRY_MS * attempt);
  }
}

function looksLikeZstdMagic(buf) {
  // ZSTD frame magic number (little-endian): 28 B5 2F FD
  return buf?.length >= 4 && buf[0] === 0x28 && buf[1] === 0xB5 && buf[2] === 0x2F && buf[3] === 0xFD;
}

async function run() {
  console.log("🚀 Starting ETG sync");
  console.log("Inventory:", ETG_INVENTORY);

  const download_id = `render-${Date.now()}`;

  const dumpUrl = await getDumpUrlViaGateway();
  console.log("📦 Got dump URL:", dumpUrl);

  console.log("⬇️ Downloading dump (.zst) ...");
  const dumpRes = await fetch(dumpUrl, {
    // Important: avoid accidental content-encoding transforms
    headers: { "Accept-Encoding": "identity" }
  });

  if (!dumpRes.ok) {
    const t = await dumpRes.text().catch(() => "");
    throw new Error(`Dump download failed (${dumpRes.status}): ${t}`);
  }

  // Quick sanity check to catch "expired signed URL returns HTML" (common cause of "invalid zstd data")
  // We read the first chunk from the Web stream, check magic bytes, then prepend it back into a Node stream.
  const reader = dumpRes.body.getReader();
  const first = await reader.read();
  if (first.done || !first.value) {
    throw new Error("Dump download returned empty body.");
  }

  const firstBuf = Buffer.from(first.value);
  if (!looksLikeZstdMagic(firstBuf)) {
    // likely HTML/XML error response
    const preview = firstBuf.toString("utf8", 0, Math.min(firstBuf.length, 300));
    throw new Error(
      "Downloaded data is not ZSTD (magic mismatch). Most likely the signed URL returned an error page.\n" +
      `Preview:\n${preview}`
    );
  }

  // Rebuild stream with first chunk + remaining chunks
  const rebuiltWebStream = new ReadableStream({
    start(controller) {
      controller.enqueue(first.value);
      (async () => {
        while (true) {
          const { value, done } = await reader.read();
          if (done) break;
          controller.enqueue(value);
        }
        controller.close();
      })().catch((err) => controller.error(err));
    }
  });

  const compressedNode = Readable.fromWeb(rebuiltWebStream);

  console.log("🔓 Decompressing ZSTD (streaming) ...");
  const decompressed = compressedNode.pipe(new DecompressStream());
  decompressed.setEncoding("utf8");

  const rl = readline.createInterface({
    input: decompressed,
    crlfDelay: Infinity
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
      // skip malformed lines instead of dying
      continue;
    }

    batch.push(obj);

    if (batch.length >= BATCH_SIZE) {
      rl.pause(); // prevent readline buffering tons of lines while we await network
      await upsertBatch(download_id, batch, batchIndex);
      total += batch.length;
      batch = [];
      batchIndex++;
      if (total % LOG_EVERY === 0) console.log(`✅ Upserted ${total}`);
      rl.resume();
    }
  }

  if (batch.length) {
    await upsertBatch(download_id, batch, batchIndex);
    total += batch.length;
  }

  console.log(`🎉 Done. Total processed: ${total}`);
}

run().catch((err) => {
  console.error("❌ Fatal error:", err);
  process.exit(1);
});
