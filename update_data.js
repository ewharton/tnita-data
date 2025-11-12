import fs from "fs";
import path from "path";
import https from "https";
import { Readable } from "stream";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const CONFIG = {
  // failIfDateMismatch: true, // Set false to skip date validation
  failIfDateMismatch: false, // Set false to skip date validation
  // When true, build and upload collectors_artists_agg.json (compact) and include in manifest
  buildAgg: true,
};

// --- ArNS Configuration (no .env needed for these) ---
const ARNS_CONFIG = {
  // Your ArNS name
  name: "network-art-test2",
  // If you know the mainnet ArNS registry contract tx id, set it here.
  // If left undefined, SDK default will be used.
  registryTx: undefined,
  // Optional hard override of the ANT (contract) id for your name.
  // Set this if registry lookup fails, to bypass registry.
  // antContractTxId: undefined,
  antContractTxId: "UI_MJe2atz6KfFbcnh7OcFCOfJNX9TPxfbzHm85oHcY",

  ttlSeconds: 60,
};

function getTodaySuffix() {
  const d = new Date();
  return `${String(d.getMonth() + 1).padStart(2, "0")}_${String(
    d.getDate()
  ).padStart(2, "0")}_${d.getFullYear()}`;
}

function getTodayCompact() {
  const d = new Date();
  return `${d.getFullYear()}${String(d.getMonth() + 1).padStart(2, "0")}${String(
    d.getDate()
  ).padStart(2, "0")}`;
}

function ensureDir(dirPath) {
  if (!fs.existsSync(dirPath)) {
    fs.mkdirSync(dirPath, { recursive: true });
    console.log(`Created directory: ${dirPath}`);
  }
}

function readJsonSafe(filePath) {
  try {
    if (fs.existsSync(filePath)) {
      return JSON.parse(fs.readFileSync(filePath, "utf8"));
    }
  } catch {}
  return null;
}

function writeJsonSafe(filePath, obj) {
  fs.writeFileSync(filePath, JSON.stringify(obj, null, 2), "utf8");
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function resolveRedirectUrl(currentUrl, locationHeader) {
  try {
    // If absolute, URL ctor succeeds; if relative, resolve against current
    const u = new URL(locationHeader, currentUrl);
    return u.toString();
  } catch {
    return locationHeader;
  }
}

async function fetchJsonFollowRedirects(url, { maxRedirects = 5, attempts = 5, retryDelayMs = 2000 } = {}) {
  return new Promise((resolve, reject) => {
    https
      .get(url, (res) => {
        let data = "";
        const status = res.statusCode || 0;
        if ([301, 302, 303, 307, 308].includes(status)) {
          if (maxRedirects <= 0) {
            reject(new Error(`Too many redirects fetching ${url}`));
            return;
          }
          const loc = res.headers.location;
          if (!loc) {
            reject(new Error(`Redirect without Location from ${url}`));
            return;
          }
          const nextUrl = resolveRedirectUrl(url, String(loc));
          resolve(fetchJsonFollowRedirects(nextUrl, { maxRedirects: maxRedirects - 1, attempts, retryDelayMs }));
          return;
        }
        if (status === 202) {
          // Pending; retry with backoff
          if (attempts <= 1) {
            reject(new Error(`Content not yet available (202) at ${url}`));
            return;
          }
          setTimeout(() => {
            resolve(fetchJsonFollowRedirects(url, { maxRedirects, attempts: attempts - 1, retryDelayMs }));
          }, retryDelayMs);
          return;
        }
        if (status !== 200) {
          reject(new Error(`Request failed: ${status}`));
          return;
        }
        res.setEncoding("utf8");
        res.on("data", (chunk) => (data += chunk));
        res.on("end", () => {
          try {
            resolve(JSON.parse(data));
          } catch (err) {
            reject(err);
          }
        });
      })
      .on("error", reject);
  });
}

// Ensure ARWEAVE_JWK_PATH is available; if only ARWEAVE_JWK_B64 is set (e.g., in CI), decode to a temp file
function ensureJwkPathEnv() {
  if (process.env.ARWEAVE_JWK_PATH && fs.existsSync(process.env.ARWEAVE_JWK_PATH)) {
    return process.env.ARWEAVE_JWK_PATH;
  }
  const b64 = process.env.ARWEAVE_JWK_B64;
  if (!b64) return null;
  const tmpDir = process.env.RUNNER_TEMP || process.env.TMPDIR || "/tmp";
  const outPath = path.join(tmpDir, "arweave_jwk.json");
  try {
    const decoded = Buffer.from(b64, "base64").toString("utf8");
    // rudimentary sanity check
    if (!decoded.trim().startsWith("{")) throw new Error("decoded content is not JSON-like");
    fs.writeFileSync(outPath, decoded, "utf8");
    process.env.ARWEAVE_JWK_PATH = outPath;
    return outPath;
  } catch (e) {
    console.error("Failed to decode ARWEAVE_JWK_B64:", e?.message || e);
    return null;
  }
}

// --- Trigger GitHub Actions workflow (for local runs with PAT) ---
async function triggerGithubWorkflow({
  owner = "ewharton",
  repo = "tnita-data",
  workflowFile = "static.yml",
  ref = "main",
  token = process.env.GITHUB_TOKEN,
} = {}) {
  if (!token) {
    console.warn("GITHUB_TOKEN not set; skipping workflow dispatch.");
    return null;
  }
  const url = `https://api.github.com/repos/${owner}/${repo}/actions/workflows/${workflowFile}/dispatches`;
  const res = await fetch(url, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      Accept: "application/vnd.github+json",
    },
    body: JSON.stringify({ ref }),
  });
  if (res.status !== 204) {
    const txt = await res.text().catch(() => "");
    throw new Error(`workflow_dispatch failed: ${res.status} ${txt}`);
  }
  console.log(`Triggered GitHub workflow ${workflowFile} on ${owner}/${repo}@${ref}`);
  return true;
}


async function verifyArnsAndManifestAfterTtl(manifestTxId) {
  const ttlSeconds = Number(ARNS_CONFIG.ttlSeconds);
  const bufferSeconds = 40;
  const waitMs = (ttlSeconds + bufferSeconds) * 1000;
  console.log(`Waiting ${ttlSeconds + bufferSeconds}s for ArNS TTL to elapse before verification...`);
  await delay(waitMs);

  const processId = ARNS_CONFIG.antContractTxId;
  if (!processId) {
    console.warn("Cannot verify ArNS without ANT processId.");
    return false;
  }
  try {
    const { ANT } = await import("@ar.io/sdk");
    const antReadableAO = ANT.init({ processId });
    const rootRecordAO = await antReadableAO.getRecord({ undername: "@" });
    const pointedTx = rootRecordAO?.transactionId || null;
    console.log(`ArNS '@' now points to: ${pointedTx || '<none>'}`);
    if (pointedTx !== manifestTxId) {
      console.warn("ArNS record does not yet point to the expected manifest.");
      return false;
    }

    // Verify by reading embedded metadata inside collectors_artists_agg.json
    const maxAttempts = 5; 
    const retryDelayMs = 5000;
    let matched = false;
    for (let i = 1; i <= maxAttempts; i++) {
      try {
        const agg = await fetchJsonFollowRedirects(`https://arweave.net/${manifestTxId}/collectors_artists_agg.json`);
        const got = agg?.metadata?.snapshot_date || null;
        const expected = getTodayCompact();
        if (got === expected) {
          console.log(`collectors_artists_agg.json metadata.snapshot_date verified: ${got}`);
          matched = true;
          break;
        } else {
          console.warn(`Attempt ${i}/${maxAttempts}: agg.metadata.snapshot_date=${got || '<none>'} expected=${expected}`);
        }
      } catch (e) {
        console.warn(`Attempt ${i}/${maxAttempts} collectors_artists_agg.json not ready: ${e?.message || e}`);
      }
      await delay(retryDelayMs);
    }
    if (!matched) {
      console.warn("Embedded metadata verification did not complete within retries.");
      // In non-strict mode, warn but do not fail/block progression
      if (!CONFIG.failIfDateMismatch) {
        console.warn("Continuing despite date mismatch (failIfDateMismatch=false).");
        return true;
      }
      return false;
    }
    return true;
  } catch (e) {
    console.error("Verification error:", e?.message || e);
    return false;
  }
}

function fetchJson(url) {
  return new Promise((resolve, reject) => {
    https
      .get(url, (res) => {
        let data = "";
        if (res.statusCode !== 200) {
          reject(new Error(`Request failed: ${res.statusCode}`));
          return;
        }
        res.on("data", (chunk) => (data += chunk));
        res.on("end", () => {
          try {
            resolve(JSON.parse(data));
          } catch (err) {
            reject(err);
          }
        });
      })
      .on("error", reject);
  });
}

function fetchText(url) {
  return new Promise((resolve, reject) => {
    https
      .get(url, (res) => {
        let data = "";
        if (res.statusCode !== 200) {
          reject(new Error(`Request failed: ${res.statusCode}`));
          return;
        }
        res.setEncoding("utf8");
        res.on("data", (chunk) => (data += chunk));
        res.on("end", () => resolve(data));
      })
      .on("error", reject);
  });
}

async function fetchTextFollowRedirects(url, { maxRedirects = 5, attempts = 5, retryDelayMs = 2000 } = {}) {
  return new Promise((resolve, reject) => {
    https
      .get(url, (res) => {
        const status = res.statusCode || 0;
        if ([301, 302, 303, 307, 308].includes(status)) {
          if (maxRedirects <= 0) {
            reject(new Error(`Too many redirects fetching ${url}`));
            return;
          }
          const loc = res.headers.location;
          if (!loc) {
            reject(new Error(`Redirect without Location from ${url}`));
            return;
          }
          const nextUrl = resolveRedirectUrl(url, String(loc));
          resolve(fetchTextFollowRedirects(nextUrl, { maxRedirects: maxRedirects - 1, attempts, retryDelayMs }));
          return;
        }
        if (status === 202) {
          if (attempts <= 1) {
            reject(new Error(`Content not yet available (202) at ${url}`));
            return;
          }
          setTimeout(() => {
            resolve(fetchTextFollowRedirects(url, { maxRedirects, attempts: attempts - 1, retryDelayMs }));
          }, retryDelayMs);
          return;
        }
        if (status !== 200) {
          reject(new Error(`Request failed: ${status}`));
          return;
        }
        res.setEncoding("utf8");
        let data = "";
        res.on("data", (chunk) => (data += chunk));
        res.on("end", () => resolve(data));
      })
      .on("error", reject);
  });
}

// --- Download and validate the latest snapshot ---
async function downloadAndValidateSnapshot() {
  const url = "https://api.6529.io/api/consolidated_uploads?page=1&page_size=10";
  console.log("Fetching latest consolidated_uploads snapshot...");

  const json = await fetchJson(url);

  if (!json || !Array.isArray(json.data) || json.data.length === 0) {
    throw new Error("No snapshot data returned from API");
  }

  const latest = json.data[0];
  const { date, block, url: arweaveUrl } = latest;

  if (!date || !arweaveUrl) {
    throw new Error("Malformed snapshot data");
  }

  const txnId = arweaveUrl.replace(/^https:\/\/arweave\.net\//, "").trim();
  const todayCompact = getTodayCompact();

  if (date !== todayCompact) {
    if (CONFIG.failIfDateMismatch) {
      throw new Error(
        `Latest snapshot date (${date}) does not match today (${todayCompact})`
      );
    } else {
      console.warn(
        `WARN: Latest snapshot date (${date}) does not match today (${todayCompact}) – proceeding because failIfDateMismatch=false`
      );
    }
  }

  console.log(`Latest snapshot OK: date=${date}, block=${block}, txnId=${txnId}`);
  return { date, block, txnId };
}

// --- Download collectors_cards.csv from Arweave txn id ---
async function downloadCollectorsCsvFromArweave(dirPath, txnId) {
  const url = `https://arweave.net/${txnId}`;
  const outPath = path.join(dirPath, "collectors_cards.csv");
  console.log(`Downloading collectors_cards.csv from ${url} ...`);
  const csv = await fetchTextFollowRedirects(url);
  fs.writeFileSync(outPath, csv, "utf8");
  console.log(`Saved collectors_cards.csv (${csv.length} bytes)`);
  return outPath;
}

// // --- Write the Arweave manifest ---
// function writeManifest(snapshotMeta, dirPath) {
//   const today = getTodaySuffix();
//   const manifest = {
//     manifest: "arweave/paths",
//     version: "0.1.0",
//     index: { path: "collectors_cards.csv" },
//     paths: {
//       "collectors_cards.csv": { id: snapshotMeta.txnId },
//       // "cards_metadata.csv": { id: "" },
//       "cards_to_artists.csv": { id: "" },
//       "network_profiles.csv": { id: "" },
//     },
//     metadata: {
//       date: snapshotMeta.date,
//       block: snapshotMeta.block,
//     },
//   };

//   // const filePath = path.join(dirPath, "manifest_data.json");
//   const filePath = path.join(dirPath, `manifest__${today}.json`);
//   fs.writeFileSync(filePath, JSON.stringify(manifest, null, 2), "utf8");
//   console.log(`Created manifest.json in ${dirPath}`);
// }

// --- Download network profile data (CSV) ---
async function downloadProfileData(dirPath) {
  const url =
    // "https://api.6529.io/api/tdh/consolidated_metrics?page_size=50&page=1&sort=level&sort_direction=DESC&download_all=true";
     "https://api.6529.io/api/tdh/consolidated_metrics?page_size=50&page=1&sort=level&sort_direction=DESC&content=memes&collector=memes&download_all=true"

  console.log("Downloading profileData...");
  try {
    const today = getTodaySuffix();
    const fileName = `network_profiles__${today}.csv`;
    const filePath = path.join(dirPath, fileName);
    const reset = String(process.env.RESET_DATA || "").toLowerCase() === "true";
    if (!reset && fs.existsSync(filePath)) {
      console.log(`Network profiles CSV already exists for today, skipping`);
      return;
    }
    const csvData = await fetchText(url);
    fs.writeFileSync(filePath, csvData, "utf8");
    console.log(`Saved ${fileName} (${csvData.length} bytes)`);
  } catch (err) {
    console.error("Error downloading profileData:", err.message);
  }
}

// --- Download raw artists data (JSON) ---
async function downloadArtistsRaw(dirPath) {
  console.log("Downloading artists raw data (all pages)...");
  let url = "https://api.6529.io/api/artists";
  const allArtists = [];
  let page = 1;

  try {
    const today = getTodaySuffix();
    const fileName = `artists_raw__${today}.json`;
    const filePath = path.join(dirPath, fileName);
    const reset = String(process.env.RESET_DATA || "").toLowerCase() === "true";
    if (!reset && fs.existsSync(filePath)) {
      console.log(`Artists raw already exists for today, skipping`);
      const existing = readJsonSafe(filePath);
      const arr = Array.isArray(existing?.data) ? existing.data : [];
      return arr;
    }

    while (url) {
      const json = await fetchJson(url);
      const pageArtists = Array.isArray(json?.data) ? json.data : [];
      allArtists.push(...pageArtists);
      console.log(`Fetched page ${page} with ${pageArtists.length} artists`);
      url = typeof json?.next === "string" && json.next.length > 0 ? json.next : null;
      page += 1;
    }

    const toSave = { count: allArtists.length, data: allArtists };
    fs.writeFileSync(filePath, JSON.stringify(toSave, null, 2), "utf8");
    console.log(`Saved ${fileName} with ${allArtists.length} artists`);

    if (allArtists.length === 0) {
      throw new Error("No artists found in paginated response");
    }
    return allArtists;
  } catch (err) {
    console.error("Error downloading artists data:", err.message);
    return [];
  }
}

// --- Process artists into cards_to_artists.csv ---
function processArtistsToCsv(artistsInput, dirPath) {
  console.log("Processing artists into cards_to_artists CSV...");

  const artists = Array.isArray(artistsInput) ? artistsInput : [];
  const tokenIdToArtists = new Map();

  const today = getTodaySuffix();
  const csvName = `cards_to_artists__${today}.csv`;
  const csvPath = path.join(dirPath, csvName);
  const reset = String(process.env.RESET_DATA || "").toLowerCase() === "true";
  if (!reset && fs.existsSync(csvPath)) {
    console.log(`Cards-to-artists CSV already exists for today, skipping: ${csvName}`);
    return;
  }

  for (const artist of artists) {
    const rawName = artist?.name ?? artist?.display_name ?? artist?.artist ?? "";
    const artistName = typeof rawName === "string" ? rawName : String(rawName ?? "");
    // Extract meme card IDs from the 'memes' array (objects with { id })
    const memeIds = Array.isArray(artist?.memes)
      ? artist.memes
          .map((m) => (m && typeof m === "object" ? m.id : m))
          .filter((id) => Number.isFinite(id))
      : [];

    for (const cardId of memeIds) {
      const tokenId = String(cardId);
      if (!tokenIdToArtists.has(tokenId)) tokenIdToArtists.set(tokenId, []);
      const list = tokenIdToArtists.get(tokenId);
      if (!list.includes(artistName)) list.push(artistName);
    }
  }

  const rows = [];
  rows.push("tokenId,artist,artist_count");

  const sortedEntries = Array.from(tokenIdToArtists.entries()).sort(
    (a, b) => Number(a[0]) - Number(b[0])
  );

  for (const [tokenId, names] of sortedEntries) {
    const escapedNames = names.map((n) => (n.includes(",") ? `"${n}"` : n));
    const artistsField = `"${escapedNames.join(",")}"`;
    const count = names.length;
    rows.push(`${tokenId},${artistsField},${count}`);
  }

  const csvContent = rows.join("\n");
  fs.writeFileSync(csvPath, csvContent, "utf8");
  console.log(`Saved ${csvName} (${csvContent.length} bytes)`);
}

// ===== Compact Aggregator (collectors_artists_agg.json) =====
function parseCSVLine(line) {
  const result = [];
  let inQuotes = false;
  let cur = "";
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') {
      if (inQuotes && line[i + 1] === '"') {
        cur += '"';
        i++;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (ch === "," && !inQuotes) {
      result.push(cur);
      cur = "";
    } else {
      cur += ch;
    }
  }
  result.push(cur);
  return result;
}

function normalizeMemesJsonLike(memesRaw) {
  if (memesRaw == null || memesRaw === "") return [];
  const fixed = String(memesRaw).replace(/([{,]\s*)([a-zA-Z_][a-zA-Z0-9_]*)\s*:/g, '$1"$2":');
  try {
    const parsed = JSON.parse(fixed);
    return Array.isArray(parsed) ? parsed : [parsed];
  } catch {
    return [];
  }
}

async function loadProfilesMapFromFile(profilesCsvPath) {
  const text = fs.readFileSync(profilesCsvPath, "utf8");
  const lines = text.split("\n");
  if (lines.length < 2) return new Map();
  const headers = parseCSVLine(lines[0]).map((h) => h.replace(/"/g, "").trim());
  const map = new Map();
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i];
    if (!line) continue;
    const cols = parseCSVLine(line);
    const get = (name) => {
      const idx = headers.indexOf(name);
      return idx >= 0 && idx < cols.length ? cols[idx] : "";
    };
    const k = get("consolidation_key");
    if (!k) continue;
    map.set(k, {
      handle: get("handle"),
      consolidation_display: get("consolidation_display"),
      boosted_tdh: get("boosted_tdh"),
      unique_memes: get("unique_memes"),
    });
  }
  return map;
}

async function loadCardsMapFromFile(cardsCsvPath) {
  const text = fs.readFileSync(cardsCsvPath, "utf8");
  const lines = text.split("\n");
  if (lines.length < 2) return { cardsMap: new Map(), artistFirstTokenId: new Map() };
  const headers = parseCSVLine(lines[0]).map((h) => h.replace(/"/g, "").trim());
  const tokenIdIdx = headers.findIndex((h) => h.toLowerCase() === "tokenid");
  const artistIdx = headers.findIndex((h) => h.toLowerCase() === "artist");
  const map = new Map();
  const artistFirstTokenId = new Map();
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i];
    if (!line) continue;
    const cols = parseCSVLine(line);
    if (cols.length <= Math.max(tokenIdIdx, artistIdx)) continue;
    const tokenId = (cols[tokenIdIdx] || "").replace(/"/g, "").trim();
    const artistFieldRaw = cols[artistIdx] || "";
    const artistField = artistFieldRaw.trim();
    if (!tokenId || !artistField) continue;
    let artists = [];
    // Prefer JSON list if present
    if (artistField.startsWith("[")) {
      try {
        const arr = JSON.parse(artistField);
        if (Array.isArray(arr)) {
          artists = arr.map((n) => String(n).trim());
        }
      } catch {}
    }
    if (artists.length === 0) {
      // Fallback: split by comma, then strip surrounding quotes on each
      const stripOuterQuotes = (s) => {
        let t = String(s).trim();
        if ((t.startsWith('"') && t.endsWith('"')) || (t.startsWith("'") && t.endsWith("'"))) {
          t = t.slice(1, -1);
        }
        return t;
      };
      artists = artistField
        .split(",")
        .map((s) => stripOuterQuotes(s))
        .map((s) => s.replace(/^"+|"+$/g, "").trim())
        .filter(Boolean);
    }
    map.set(tokenId, artists.length > 0 ? artists : [artistField.replace(/^"+|"+$/g, "").trim()]);
    const nTid = Number(tokenId);
    if (!isNaN(nTid)) {
      for (const a of (artists.length > 0 ? artists : [artistField.replace(/^"+|"+$/g, "").trim()])) {
        const nameSan = String(a).replace(/^"+|"+$/g, "").trim();
        const prev = artistFirstTokenId.get(nameSan);
        if (prev == null || nTid < prev) artistFirstTokenId.set(a, nTid);
      }
    }
  }
  return { cardsMap: map, artistFirstTokenId };
}

async function buildAggCompact({ collectorsCsvPath, profilesCsvPath, cardsCsvPath, snapshotDate, snapshotBlock }) {
  const profilesMap = await loadProfilesMapFromFile(profilesCsvPath);
  const { cardsMap, artistFirstTokenId } = await loadCardsMapFromFile(cardsCsvPath);

  const text = fs.readFileSync(collectorsCsvPath, "utf8");
  const lines = text.split("\n");
  if (lines.length < 2) {
    return {
      metadata: { generated_at: new Date().toISOString(), totalCards: cardsMap.size, snapshot_date: snapshotDate || null, snapshot_block: snapshotBlock || null },
      artists: [],
      collectors: []
    };
  }
  const headers = parseCSVLine(lines[0]).map((h) => h.replace(/"/g, "").trim());

  const compactArtistsList = [];
  const compactNameToId = new Map();
  const compactCollectors = [];

  function sanitizeArtistName(name) {
    let s = String(name ?? "").trim();
    if ((s.startsWith('"') && s.endsWith('"')) || (s.startsWith("'") && s.endsWith("'"))) {
      s = s.slice(1, -1).trim();
    }
    // Remove any residual leading/trailing quotes
    s = s.replace(/^"+|"+$/g, "").trim();
    return s;
  }

  function getArtistId(name) {
    const clean = sanitizeArtistName(name);
    let id = compactNameToId.get(clean);
    if (id == null) {
      id = compactArtistsList.length;
      compactNameToId.set(clean, id);
      const ftid = artistFirstTokenId.get(clean) ?? null;
      compactArtistsList.push([clean, ftid]);
    }
    return id;
  }

  for (let i = 1; i < lines.length; i++) {
    const line = lines[i];
    if (!line) continue;
    const cols = parseCSVLine(line);
    const get = (name) => {
      const idx = headers.indexOf(name);
      return idx >= 0 && idx < cols.length ? cols[idx] : "";
    };
    const consolidation_key = get("consolidation_key");
    const memesRaw = get("memes");
    if (!consolidation_key || !memesRaw) continue;

    const prof = profilesMap.get(consolidation_key) || {};
    const handle = prof.handle || "";
    const boosted_tdh = Number(prof.boosted_tdh) || 0;
    const unique_memes = Number(prof.unique_memes) || 0;

    const memes = normalizeMemesJsonLike(memesRaw);
    let collectorCardCount = 0;
    let collectorTotalCardCount = 0;
    const perArtist = new Map();
    for (const m of memes) {
      if (!m || m.id == null || m.balance == null || m.balance <= 0) continue;
      const tid = String(m.id);
      const mappedArtists = cardsMap.get(tid);
      if (!mappedArtists || mappedArtists.length === 0) continue;
      collectorCardCount += 1;
      collectorTotalCardCount += Number(m.balance) || 0;
      for (const a of mappedArtists) {
        const key = sanitizeArtistName(a);
        const prev = perArtist.get(key) || { name: a, uniqueCards: 0, totalBalance: 0 };
        prev.uniqueCards += 1;
        prev.totalBalance += Number(m.balance) || 0;
        perArtist.set(key, prev);
      }
    }
    const aTuples = Array.from(perArtist.values()).map((a) => [getArtistId(a.name), a.uniqueCards, a.totalBalance]);
    compactCollectors.push([consolidation_key, handle, collectorCardCount, collectorTotalCardCount, unique_memes, boosted_tdh, aTuples]);
  }

  return {
    metadata: {
      generated_at: new Date().toISOString(),
      totalCards: cardsMap.size,
      snapshot_date: snapshotDate || null,
      snapshot_block: snapshotBlock || null,
    },
    artists: compactArtistsList,
    collectors: compactCollectors
  };
}

// --- Upload helpers (ArDrive Turbo) ---
function loadArweaveJwkFromEnv() {
  // Prefer explicit path; otherwise, derive from ARWEAVE_JWK_B64
  const ensuredPath = ensureJwkPathEnv();
  const jwkPath = ensuredPath || process.env.ARWEAVE_JWK_PATH;
  if (!jwkPath) return null;
  try {
    const raw = fs.readFileSync(jwkPath, "utf8");
    return JSON.parse(raw);
  } catch (e) {
    console.error("Failed to read ARWEAVE_JWK_PATH:", e.message);
    return null;
  }
}

async function initTurbo(jwk) {
  const { TurboFactory } = await import("@ardrive/turbo-sdk");
  return await TurboFactory.authenticated({ privateKey: jwk });
}

async function uploadFileWithTurbo(turbo, filePath, contentType, extraTags = []) {
  if (!fs.existsSync(filePath)) {
    throw new Error(`Upload source file not found: ${filePath}`);
  }
  try {
    const result = await turbo.uploadFile({
      fileStreamFactory: () => fs.createReadStream(filePath),
      fileSizeFactory: () => fs.statSync(filePath).size,
      dataItemOpts: {
        tags: [
          { name: "Content-Type", value: contentType },
          { name: "App-Name", value: "network-art" },
          { name: "App-Version", value: "v2" },
          ...extraTags,
        ],
      },
    });
    if (!result?.id) {
      throw new Error(`Upload did not return a transaction id for ${path.basename(filePath)}`);
    }
    console.log(`Uploaded ${path.basename(filePath)} -> ${result.id}`);
    return result.id;
  } catch (e) {
    throw new Error(`Upload failed for ${path.basename(filePath)}: ${e?.message || e}`);
  }
}

async function uploadManifestWithTurbo(turbo, manifestObj) {
  const json = JSON.stringify(manifestObj, null, 2);
  const buffer = Buffer.from(json, "utf8");
  const result = await turbo.uploadFile({
    fileStreamFactory: () => Readable.from([buffer]),
    fileSizeFactory: () => buffer.length,
    dataItemOpts: {
      tags: [
        { name: "Content-Type", value: "application/x.arweave-manifest+json" },
        { name: "App-Name", value: "network-art" },
        { name: "App-Version", value: "v2" },
        ...(manifestObj?.metadata?.date
          ? [{ name: "Snapshot-Date", value: String(manifestObj.metadata.date) }]
          : []),
      ],
    },
  });
  if (!result?.id) {
    throw new Error("Manifest upload did not return a transaction id");
  }
  return { id: result.id, json };
}

async function uploadJsonDataWithTurbo(turbo, jsonObj, extraTags = []) {
  const json = JSON.stringify(jsonObj);
  const buffer = Buffer.from(json, "utf8");
  const result = await turbo.uploadFile({
    fileStreamFactory: () => Readable.from([buffer]),
    fileSizeFactory: () => buffer.length,
    dataItemOpts: {
      tags: [
        { name: "Content-Type", value: "application/json" },
        { name: "App-Name", value: "network-art" },
        { name: "App-Version", value: "v2" },
        ...extraTags,
      ],
    },
  });
  if (!result?.id) {
    throw new Error("Metadata snapshot upload did not return a transaction id");
  }
  return result.id;
}

async function uploadCsvsAndManifest(dirPath, snapshotMeta) {
  const jwk = loadArweaveJwkFromEnv();
  if (!jwk) {
    throw new Error("ARWEAVE_JWK not provided");
  }
  const turbo = await initTurbo(jwk);
  const today = getTodaySuffix();

  // Idempotency: if we already uploaded today, reuse
  const summaryPath = path.join(dirPath, `upload_summary__${today}.json`);
  if (!CONFIG.buildAgg && fs.existsSync(summaryPath)) {
    try {
      const prev = JSON.parse(fs.readFileSync(summaryPath, "utf8"));
      if (prev?.manifestTxId) {
        console.log(`Found existing upload summary, reusing manifest ${prev.manifestTxId}`);
        return prev;
      }
    } catch {}
  }

  const cardsCsvPath = path.join(dirPath, `cards_to_artists__${today}.csv`);
  const profilesCsvPath = path.join(dirPath, `network_profiles__${today}.csv`);
  const aggJsonPath = path.join(dirPath, "collectors_artists_agg.json");

  // Check required daily artifacts before manifest upload
  const missing = [];
  if (!fs.existsSync(cardsCsvPath)) missing.push(path.basename(cardsCsvPath));
  if (!fs.existsSync(profilesCsvPath)) missing.push(path.basename(profilesCsvPath));

  // Fail early if required artifacts are missing
  if (missing.length > 0) {
    throw new Error(`Missing daily artifacts (${missing.join(", ")}); cannot upload manifest`);
  }

  const cardsTxId = await uploadFileWithTurbo(turbo, cardsCsvPath, "text/csv");
  const profilesTxId = await uploadFileWithTurbo(turbo, profilesCsvPath, "text/csv");
  let aggTxId = null;
  if (CONFIG.buildAgg && fs.existsSync(aggJsonPath)) {
    aggTxId = await uploadFileWithTurbo(turbo, aggJsonPath, "application/json", [
      { name: "Dataset", value: "collectors_artists_agg" },
    ]);
  }

  // Build manifest using available IDs
  const manifest = {
    manifest: "arweave/paths",
    version: "0.1.0",
    index: { path: "collectors_artists_agg.json" },
    paths: {
      "collectors_cards.csv": { id: snapshotMeta.txnId },
    },
    metadata: {
      date: snapshotMeta.date,
      block: snapshotMeta.block,
    },
  };
  if (cardsTxId) manifest.paths["cards_to_artists.csv"] = { id: cardsTxId };
  if (profilesTxId) manifest.paths["network_profiles.csv"] = { id: profilesTxId };
  if (aggTxId) manifest.paths["collectors_artists_agg.json"] = { id: aggTxId };


  const { id: manifestTxId, json: manifestJson } = await uploadManifestWithTurbo(turbo, manifest);
  console.log(`Uploaded manifest -> ${manifestTxId}`);

  // Also write the same manifest locally with our usual filename
  const manifestPath = path.join(dirPath, `manifest__${today}.json`);
  fs.writeFileSync(manifestPath, manifestJson, "utf8");

  const summary = { cardsTxId, profilesTxId, manifestTxId };
  fs.writeFileSync(summaryPath, JSON.stringify(summary, null, 2), "utf8");
  return summary;
}

// AO-only update
async function updateArnsTargetIfConfigured(manifestTxId) {
  const nameToUpdate = ARNS_CONFIG.name;
  if (!nameToUpdate) {
    console.log("ARNS_NAME not set; skipping ARNS update.");
    return null;
  }
  const jwk = loadArweaveJwkFromEnv();
  if (!jwk) {
    console.log("No JWK found; cannot update ARNS.");
    return null;
  }
  try {
    const { ANT, ArweaveSigner } = await import("@ar.io/sdk");
    const Arweave = (await import("arweave")).default;
    const arweave = Arweave.init({ host: "arweave.net", port: 443, protocol: "https" });
    const signerAddress = await arweave.wallets.jwkToAddress(jwk);

    const processId = ARNS_CONFIG.antContractTxId;
    if (!processId) {
      console.log("ANT processId (ARNS_CONFIG.antContractTxId) not set; cannot update ArNS.");
      return null;
    }

    const antReadableAO = ANT.init({ processId });
    try {
      // const ownerAO = await antReadableAO.getOwner();
      // const controllersAO = await antReadableAO.getControllers();
      const rootRecordAO = await antReadableAO.getRecord({ undername: "@" });
      // console.log(`AO ANT owner: ${ownerAO}`);
      // console.log(`AO ANT controllers: ${Array.isArray(controllersAO) ? controllersAO.join(",") : controllersAO}`);
      // console.log(`AO current '@' record: ${rootRecordAO?.transactionId || "<none>"}`);
      if (rootRecordAO?.transactionId === manifestTxId) {
        console.log("ArNS '@' already points to this manifest (AO); skipping setRecord.");
        return null;
      }
    } catch {}

    const antAO = ANT.init({ processId, signer: new ArweaveSigner(jwk) });
    const resAO = await antAO.setRecord({ undername: "@", transactionId: manifestTxId, ttlSeconds: ARNS_CONFIG.ttlSeconds });
    console.log("ARNS AO setRecord result:", resAO?.id || resAO);
    return resAO?.id || null;
  } catch (e) {
    console.error("Failed to update ARNS via ar.io SDK:", e?.message || e);
    return null;
  }
}

// --- Main Execution ---
(async () => {
  console.log("Starting dataset download...");

  try {
    const today = getTodaySuffix();
    const dirPath = path.join(__dirname, "data", today);
    ensureDir(dirPath);

    // Ensure local artifacts
    await downloadProfileData(dirPath);
    const artists = await downloadArtistsRaw(dirPath);
    processArtistsToCsv(artists, dirPath);

    // Fetch latest snapshot meta and download collectors_cards.csv for local processing
    const snapshotMeta = await downloadAndValidateSnapshot();
    await downloadCollectorsCsvFromArweave(dirPath, snapshotMeta.txnId);

    // Optionally build aggregated compact JSON
    if (CONFIG.buildAgg) {
      try {
        const profilesCsv = path.join(dirPath, `network_profiles__${today}.csv`);
        const cardsCsv = path.join(dirPath, `cards_to_artists__${today}.csv`);
        const collectorsCsv = path.join(dirPath, "collectors_cards.csv");
    const aggObj = await buildAggCompact({
          collectorsCsvPath: collectorsCsv,
          profilesCsvPath: profilesCsv,
      cardsCsvPath: cardsCsv,
      snapshotDate: snapshotMeta?.date,
      snapshotBlock: snapshotMeta?.block
        });
        const aggOutPath = path.join(dirPath, "collectors_artists_agg.json");
        writeJsonSafe(aggOutPath, aggObj);
        console.log(`Wrote collectors_artists_agg.json with ${aggObj.collectors.length} collectors, ${aggObj.artists.length} artists`);
      } catch (e) {
        console.error("Failed to build collectors_artists_agg.json:", e?.message || e);
      }
    }

    // Determine if we already uploaded a manifest today
    const summaryPath = path.join(dirPath, `upload_summary__${today}.json`);
    const manifestPath = path.join(dirPath, `manifest__${today}.json`);
    const prevSummary = readJsonSafe(summaryPath);
    const existingManifest = readJsonSafe(manifestPath);

    let manifestTxId = prevSummary?.manifestTxId || null;
    const hadPrevManifest = Boolean(manifestTxId);
    let uploadedNewManifest = false;

    // If no manifestTxId yet, decide whether to build and upload a new manifest
    if (!manifestTxId) {
      // Only upload manifest if both CSVs exist
      const cardsCsv = path.join(dirPath, `cards_to_artists__${today}.csv`);
      const profilesCsv = path.join(dirPath, `network_profiles__${today}.csv`);
      const bothCsvsExist = fs.existsSync(cardsCsv) && fs.existsSync(profilesCsv);

      if (bothCsvsExist) {
        // If we already have a local manifest with collectors id, we can skip re-writing it
        // but we still need snapshot meta for a fresh manifest upload if none was uploaded.
        // Fetch snapshot meta only when needed.
        const uploadSummary = await uploadCsvsAndManifest(dirPath, snapshotMeta);
        manifestTxId = uploadSummary?.manifestTxId || null;
        if (!hadPrevManifest && manifestTxId) uploadedNewManifest = true;
      } else {
        throw new Error("Required CSVs missing; skipping manifest upload");
      }
    }
    if (manifestTxId) {
      const arnsTx = await updateArnsTargetIfConfigured(manifestTxId);
      if (uploadedNewManifest && !arnsTx) {
        throw new Error("ARNS update failed (no txId) after manifest upload");
      }
      const verified = await verifyArnsAndManifestAfterTtl(manifestTxId);
      if (uploadedNewManifest && !verified) {
        if (CONFIG.failIfDateMismatch) {
          throw new Error("ARNS verification failed (snapshot_metadata mismatch) after manifest upload");
        } else {
          console.warn("WARN: ARNS verification did not pass (pointer and/or snapshot date) – continuing because failIfDateMismatch=false");
        }
      }
      if (verified) {
        try {
          await triggerGithubWorkflow({
            owner: "ewharton",
            repo: "tnita-data",
            workflowFile: "static.yml",
            ref: "main",
          });
        } catch (e) {
          console.error("Mirror workflow trigger failed:", e?.message || e);
        }
      }
    }

    console.log("All downloads complete.");
  } catch (err) {
    console.error("Fatal error:", err.message);
    process.exit(1);
  }
})();