import readline from "node:readline";
import { createDecompressStream } from "@mongodb-js/zstd";

/**
 * Node 18+ has global fetch
 * DO NOT import fetch from undici
 */

function must(name) {
  const v = process.env[name];
  if (!v) throw new Error(`Missing env var: ${name}`);
  return v;
}

// ===== ENV =====
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

// ===== STEP 1: GET DUMP URL =====
async function getDumpUrl() {
  console.log("🔐 Fetching dump URL via gateway...");

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
        body: {
          language: ETG_LANGUAGE,
          inventory: ETG_INVENTORY
        }
      }
    })
  });

  const json = await res.json();
  if (!json?.data?.url) {
    throw new Error("Failed to obtain dump URL");
  }

  return json.data.url;
}

// ===== STEP 2: UPSERT BATCH =====
async function upsertBatch(hotels) {
  const res = await fetch(SUPABASE_FUNCTION_URL, {
    method: "POST",
    headers: {
      "Authorization": `Bearer ${SYNC_API_TOKEN}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      hotels
    })
  });

  if (!res.ok) {
    throw new Error(await res.text());
  }
}

// ===== MAIN =====
async function run() {
  console.log("🚀 Starting ETG sync");

  const dumpUrl = await getDumpUrl();
  console.log("📦 Dump URL acquired");
  c
