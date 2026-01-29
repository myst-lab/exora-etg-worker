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

const SUPABASE_UPSERT_URL = must("SUPABASE_UPSERT_URL");
const SYNC_API_TOKEN = must("SYNC_API_TOKEN"); // must match the same Supabase project of SUPABASE_UPSERT_URL

const BATCH_SIZE = Number(process.env.BATCH_SIZE || 500);
const LOG_EVERY = Number(process.env.LOG_EVERY || 5000);
const UPSERT_RETRIES = Number(process.env.UPSERT_RETRIES || 5);
const UPSERT_RETRY_MS = Number(process.env.UPSERT_RETRY_MS || 1500);

// ===== Filters (set these in Render env) =====
const MIN_IMAGES = Number(process.env.MIN_IMAGES || 1);
const MIN_STAR_RATING = Number(process.env.MIN_STAR_RATING || 0); // set 3 if you want only 3/4/5
const REQUIRE_GEO = String(process.env.REQUIRE_GEO || "true").toLowerCase() === "true";
const REQUIRE_ADDRESS = String(process.env.REQUIRE_ADDRESS || "true").toLowerCase() === "true";
const REQUIRE_COUNTRY = String(process.env.REQUIRE_COUNTRY || "true").toLowerCase() === "true";
// ============================================

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function jsonHeaders() {
  return {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Authorization": `Bearer ${SYNC_API_TOKEN}`,
  };
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
      options: { method: "POST", body: { language: ETG_LANGUAGE, inventory: ETG_INVENTORY } },
    }),
  });

  const text = await res.text();
  let json;
  try { json = JSON.parse(text); } catch { json = null; }

  if (!res.ok || !json?.data?.url) throw new Error(`Gateway failed (${res.status}): ${text}`);
  return json.data.url;
}

async function upsertBatch(download_id, hotels, batch_index) {
  const payload = { download_id, hotels, batch_index };

  for (let attempt = 1; attempt <= UPSERT_RETRIES; attempt++) {
    const res = await fetch(SUPABASE_UPSERT_URL, {
      method: "POST",
      headers: jsonHeaders(),
      body: JSON.stringify(payload),
    });

    const text = await res.text();
    let json;
    try { json = JSON.parse(text); } catch { json = null; }

    if (res.ok && (!json || json.success !== false)) return json;

    const retryable = [408, 429, 500, 502, 503, 504].includes(res.status);
    const msg = `Upsert failed (${res.status}) attempt ${attempt}/${UPSERT_RETRIES}: ${text}`;

    if (!retryable || attempt === UPSERT_RETRIES) throw new Error(msg);
    console.warn("⚠️", msg);
    await sleep(UPSERT_RETRY_MS * attempt);
  }
}

function looksLikeZstdMagic(buf) {
  return buf?.length >= 4 && buf[0] === 0x28 && buf[1] === 0xB5 && buf[2] === 0x2F && buf[3] === 0xFD;
}

function hasAtLeastNImages(h, n) {
  const imgs = Array.isArray(h?.images) ? h.images : [];
  // optional: ignore empty strings
  const valid = imgs.filter((x) => typeof x === "string" ? x.trim().length > 0 : true);
  return valid.length >= n;
}

function hasGeo(h) {
  const lat = Number(h?.latitude);
  const lng = Number(h?.longitude);
  return Number.isFinite(lat) && Number.isFinite(lng);
}

function hasCountry(h) {
  const cc = h?.region && typeof h.region === "object" ? h.region.country_code : null;
  return typeof cc === "string" && cc.trim().length === 2;
}

function isCompleteHotel(h) {
  if (!h || typeof h !== "object") return false;
  if (!h.id || typeof h.id !== "string") return false;
  if (!h.name || typeof h.name !== "string" || !h.name.trim()) return false;

  const sr = Number(h.star_rating || 0);
  if (sr < MIN_STAR_RATING) return false;

  if (!hasAtLeastNImages(h, MIN_IMAGES)) return false;

  if (REQUIRE_ADDRESS) {
    if (!h.address || typeof h.address !== "string" || !h.address.trim()) return false;
  }

  if (REQUIRE_COUNTRY && !hasCountry(h)) return false;
  if (REQUIRE_GEO && !hasGeo(h)) return false;

  return true;
}

async function run() {
  console.log("🚀 Starting ETG sync");
  console.log("Inventory:", ETG_INVENTORY);
  console.log("Filters:", { MIN_IMAGES, MIN_STAR_RATING, REQUIRE_GEO, REQUIRE_ADDRESS, REQUIRE_COUNTRY });

  const download_id = `render-${Date.now()}`;
  const dumpUrl = await getDumpUrlViaGateway();
  console.log("📦 Got dump URL:", dumpUrl);

  console.log("⬇️ Downloading dump (.zst) ...");
  const dumpRes = await fetch(dumpUrl, { headers: { "Accept-Encoding": "identity" } });
  if (!dumpRes.ok) throw new Error(`Dump download failed (${dumpRes.status}): ${await dumpRes.text().catch(() => "")}`);

  const reader = dumpRes.body.getReader();
  const first = await reader.read();
  if (first.done || !first.value) throw new Error("Dump download returned empty body.");

  const firstBuf = Buffer.from(first.value);
  if (!looksLikeZstdMagic(firstBuf)) {
    const preview = firstBuf.toString("utf8", 0, Math.min(firstBuf.length, 300));
    throw new Error("Downloaded data is not ZSTD (magic mismatch). Preview:\n" + preview);
  }

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
    },
  });

  const compressedNode = Readable.fromWeb(rebuiltWebStream);

  console.log("🔓 Decompressing ZSTD (streaming) ...");
  const decompressed = compressedNode.pipe(new DecompressStream());
  decompressed.setEncoding("utf8");

  const rl = readline.createInterface({ input: decompressed, crlfDelay: Infinity });

  console.log("🧾 Parsing JSONL + filtering + uploading batches...");
  let batch = [];
  let kept = 0;
  let scanned = 0;
  let skipped = 0;
  let batchIndex = 0;

  for await (const line of rl) {
    if (!line || !line.trim()) continue;

    let obj;
    try { obj = JSON.parse(line); } catch { skipped++; continue; }
    scanned++;

    if (!isCompleteHotel(obj)) { skipped++; continue; }

    batch.push(obj);
    kept++;

    if (batch.length >= BATCH_SIZE) {
      rl.pause();
      await upsertBatch(download_id, batch, batchIndex);
      batch = [];
      batchIndex++;

      if (kept % LOG_EVERY === 0) console.log(`✅ Upserted ${kept} (scanned ${scanned}, skipped ${skipped})`);
      rl.resume();
    }
  }

  if (batch.length) await upsertBatch(download_id, batch, batchIndex);

  console.log(`🎉 Done. Kept: ${kept}, Scanned: ${scanned}, Skipped: ${skipped}`);
}

run().catch((err) => {
  console.error("❌ Fatal error:", err);
  process.exit(1);
});
