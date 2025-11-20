import fs from "fs";
import path from "path";
import https from "https";
import { Readable } from "stream";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const CONFIG = {
  failIfDateMismatch: true, // Set false to skip date validation
  // When true, build and upload collectors_artists_agg.json (compact) and include in manifest
  buildAgg: true,
};

const ARNS_CONFIG = {
  name: "network-art-test2",
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

function writeJsonMin(filePath, obj) {
  fs.writeFileSync(filePath, JSON.stringify(obj), "utf8");
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

// Ensure ARWEAVE_JWK_PATH is available; if only ARWEAVE_JWK_B64 is set, decode to temp file
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
    // sanity check
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
  workflowFile = "mirror.yml",
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


async function verifyArnsAndManifestAfterTtl(manifestTxId, { earlyExitOnMismatchedDate = false } = {}) {
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
    const pointerOk = pointedTx === manifestTxId;
    if (!pointerOk) {
      console.warn("ArNS record does not yet point to the expected manifest.");
      // Continue to try date verification via direct manifest path
    }

    // Verify via collectors_artists_agg.json path (reads meta.snapshot_date)
    const maxAttempts = 5; 
    const retryDelayMs = 5000;
    for (let i = 1; i <= maxAttempts; i++) {
      try {
        const agg = await fetchJsonFollowRedirects(`https://arweave.net/${manifestTxId}/collectors_artists_agg.json`);
        const got = agg?.meta?.snapshot_date;
        const expected = getTodayCompact();
        if (got === expected) {
          console.log(`collectors_artists_agg.meta.snapshot_date verified: ${got}`);
          return { ok: true, pointerOk: pointerOk, date: got };
        } else {
          console.warn(`Attempt ${i}/${maxAttempts}: collectors_artists_agg.meta.snapshot_date=${got || '<none>'} expected=${expected}`);
          if (earlyExitOnMismatchedDate && got && got !== expected) {
            console.warn("Early exit on mismatched snapshot date due to strict setting.");
            return { ok: false, pointerOk: pointerOk, date: got };
          }
        }
      } catch (e) {
        console.warn(`Attempt ${i}/${maxAttempts} collectors_artists_agg.json not ready or missing: ${e?.message || e}`);
        // If CONFIG.buildAgg was false or agg not uploaded, verification cannot proceed from this source.
      }
      await delay(retryDelayMs);
    }
    console.warn("collectors_artists_agg.json verification did not complete within retries.");
    return { ok: false, pointerOk: pointerOk, date: null };
  } catch (e) {
    console.error("Verification error:", e?.message || e);
    return { ok: false, pointerOk: false, date: null };
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


// --- Download network profile data (CSV) ---
async function downloadProfileData(dirPath) {
  // Profiles for 6529 handles, joined on consolidation key 
  // All collectors with memes
  const url =
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
  const headers = parseCSVLine(lines[0]).map((h) => h.trim());
  const tokenIdIdx = headers.findIndex((h) => h.toLowerCase() === "tokenid");
  const artistIdx = headers.findIndex((h) => h.toLowerCase() === "artist");
  const map = new Map();
  const artistFirstTokenId = new Map();
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i];
    if (!line) continue;
    const cols = parseCSVLine(line);
    if (cols.length <= Math.max(tokenIdIdx, artistIdx)) continue;
    const tokenId = String(cols[tokenIdIdx] || "").trim().replace(/^\s*"|"\s*$/g, "");
    const artistField = String(cols[artistIdx] || "").trim();
    if (!tokenId || !artistField) continue;
    // The artist field itself is a CSV string (names may contain commas and are quoted individually).
    const parsedNames = parseCSVLine(artistField)
      .map((s) => s.trim())
      .map((s) => s.replace(/^\s*"|"\s*$/g, "")) // remove any surrounding quotes on each name
      .filter(Boolean);
    const artists = parsedNames.length > 0 ? parsedNames : [artistField.replace(/^\s*"|"\s*$/g, "")];
    map.set(tokenId, artists);
    const nTid = Number(tokenId);
    if (!isNaN(nTid)) {
      for (const a of artists) {
        const prev = artistFirstTokenId.get(a);
        if (prev == null || nTid < prev) artistFirstTokenId.set(a, nTid);
      }
    }
  }
  return { cardsMap: map, artistFirstTokenId };
}

async function buildAggCompact({ collectorsCsvPath, profilesCsvPath, cardsCsvPath, snapshotMeta }) {
  const profilesMap = await loadProfilesMapFromFile(profilesCsvPath);
  const { cardsMap, artistFirstTokenId } = await loadCardsMapFromFile(cardsCsvPath);

  const text = fs.readFileSync(collectorsCsvPath, "utf8");
  const lines = text.split("\n");
  if (lines.length < 2) {
    return {
      meta: {
        generated_at: new Date().toISOString(),
        totalCards: cardsMap.size,
        snapshot_date: snapshotMeta?.date ?? null,
        snapshot_block: snapshotMeta?.block ?? null,
      },
      artists: [],
      collectors: [],
    };
  }
  const headers = parseCSVLine(lines[0]).map((h) => h.replace(/"/g, "").trim());

  const compactArtistsList = [];
  const compactNameToId = new Map();
  const compactCollectors = [];

  function getArtistId(name) {
    let id = compactNameToId.get(name);
    if (id == null) {
      id = compactArtistsList.length;
      compactNameToId.set(name, id);
      const ftid = artistFirstTokenId.get(name) ?? null;
      compactArtistsList.push([name, ftid]);
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
        const key = a;
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
    meta: {
      generated_at: new Date().toISOString(),
      totalCards: cardsMap.size,
      snapshot_date: snapshotMeta?.date ?? null,
      snapshot_block: snapshotMeta?.block ?? null,
    },
    artists: compactArtistsList,
    collectors: compactCollectors,
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
    console.log(`Uploaded (Turbo) ${path.basename(filePath)} -> ${result.id}`);
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
  console.log(`Uploaded (Turbo) JSON blob -> ${result.id}`);
  return result.id;
}

function getUploadProvider() {
  const raw = String(process.env.UPLOAD_PROVIDER || "auto").toLowerCase().trim();
  if (raw === "turbo" || raw === "arweave") return raw;
  return "auto";
}

async function uploadFileWithArweave(jwk, filePath, contentType, extraTags = []) {
  if (!fs.existsSync(filePath)) {
    throw new Error(`Upload source file not found: ${filePath}`);
  }
  const Arweave = (await import("arweave")).default;
  const arweave = Arweave.init({ host: "arweave.net", port: 443, protocol: "https" });
  const data = fs.readFileSync(filePath);
  const tx = await arweave.createTransaction({ data }, jwk);
  tx.addTag("Content-Type", contentType);
  tx.addTag("App-Name", "network-art");
  tx.addTag("App-Version", "v2");
  for (const t of extraTags) {
    if (t?.name && t?.value) tx.addTag(String(t.name), String(t.value));
  }
  await arweave.transactions.sign(tx, jwk);
  const res = await arweave.transactions.post(tx);
  if (![200, 202].includes(res?.status)) {
    throw new Error(`Arweave post failed: ${res?.status}`);
  }
  console.log(`Uploaded (L1) ${path.basename(filePath)} -> ${tx.id}`);
  return tx.id;
}

async function uploadManifestWithArweave(jwk, manifestObj) {
  const Arweave = (await import("arweave")).default;
  const arweave = Arweave.init({ host: "arweave.net", port: 443, protocol: "https" });
  const json = JSON.stringify(manifestObj, null, 2);
  const tx = await arweave.createTransaction({ data: json }, jwk);
  tx.addTag("Content-Type", "application/x.arweave-manifest+json");
  tx.addTag("App-Name", "network-art");
  tx.addTag("App-Version", "v2");
  if (manifestObj?.metadata?.date) {
    tx.addTag("Snapshot-Date", String(manifestObj.metadata.date));
  }
  await arweave.transactions.sign(tx, jwk);
  const res = await arweave.transactions.post(tx);
  if (![200, 202].includes(res?.status)) {
    throw new Error(`Arweave manifest post failed: ${res?.status}`);
  }
  return { id: tx.id, json };
}

async function uploadFileAuto(filePath, contentType, extraTags = []) {
  const provider = getUploadProvider();
  const jwk = loadArweaveJwkFromEnv();
  if (!jwk) throw new Error("ARWEAVE_JWK not provided");
  if (provider === "arweave") {
    return await uploadFileWithArweave(jwk, filePath, contentType, extraTags);
  }
  // provider === "turbo" or "auto"
  try {
    const turbo = await initTurbo(jwk);
    return await uploadFileWithTurbo(turbo, filePath, contentType, extraTags);
  } catch (e) {
    if (provider === "turbo") throw e;
    console.warn(`Turbo upload failed; falling back to L1: ${e?.message || e}`);
    return await uploadFileWithArweave(jwk, filePath, contentType, extraTags);
  }
}

async function uploadManifestAuto(manifestObj) {
  const provider = getUploadProvider();
  const jwk = loadArweaveJwkFromEnv();
  if (!jwk) throw new Error("ARWEAVE_JWK not provided");
  if (provider === "arweave") {
    const res = await uploadManifestWithArweave(jwk, manifestObj);
    console.log(`Uploaded (L1) manifest -> ${res.id}`);
    return res;
  }
  // provider === "turbo" or "auto"
  try {
    const turbo = await initTurbo(jwk);
    const res = await uploadManifestWithTurbo(turbo, manifestObj);
    console.log(`Uploaded (Turbo) manifest -> ${res.id}`);
    return res;
  } catch (e) {
    if (provider === "turbo") throw e;
    console.warn(`Turbo manifest upload failed; falling back to L1: ${e?.message || e}`);
    const res = await uploadManifestWithArweave(jwk, manifestObj);
    console.log(`Uploaded (L1) manifest -> ${res.id}`);
    return res;
  }
}

async function uploadCsvsAndManifest(dirPath, snapshotMeta) {
  const jwk = loadArweaveJwkFromEnv();
  if (!jwk) {
    throw new Error("ARWEAVE_JWK not provided");
  }
  const today = getTodaySuffix();
  console.log(`Using upload provider: ${getUploadProvider()}`);

  // Idempotency: if we already uploaded today, reuse
  const summaryPath = path.join(dirPath, `upload_summary__${today}.json`);
  if (fs.existsSync(summaryPath)) {
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

  const cardsTxId = await uploadFileAuto(cardsCsvPath, "text/csv");
  const profilesTxId = await uploadFileAuto(profilesCsvPath, "text/csv");
  let aggTxId = null;
  if (CONFIG.buildAgg && fs.existsSync(aggJsonPath)) {
    aggTxId = await uploadFileAuto(aggJsonPath, "application/json", [
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

  const { id: manifestTxId, json: manifestJson } = await uploadManifestAuto(manifest);

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
      const rootRecordAO = await antReadableAO.getRecord({ undername: "@" });
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
          snapshotMeta,
        });
        const aggOutPath = path.join(dirPath, "collectors_artists_agg.json");
        writeJsonMin(aggOutPath, aggObj);
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
      const verification = await verifyArnsAndManifestAfterTtl(manifestTxId, { earlyExitOnMismatchedDate: CONFIG.failIfDateMismatch });
      const expectedDate = getTodayCompact();
      const dateMismatch = verification?.date && verification.date !== expectedDate;
      if (uploadedNewManifest && CONFIG.failIfDateMismatch && (dateMismatch || !verification?.ok)) {
        throw new Error("ARNS verification failed (collectors_artists_agg.meta.snapshot_date mismatch) after manifest upload");
      }
      if (!CONFIG.failIfDateMismatch && (!verification?.ok || dateMismatch)) {
        console.warn("WARN: Verification did not pass (pointer and/or snapshot date). Proceeding because failIfDateMismatch=false.");
      }
      // Proceed with mirror regardless if failIfDateMismatch is false; otherwise only on success.
      if (!CONFIG.failIfDateMismatch || verification?.ok) {
        try {
          await triggerGithubWorkflow({
            owner: "ewharton",
            repo: "tnita-data",
            workflowFile: "mirror.yml",
            ref: "main",
          });
        } catch (e) {
          console.error("Mirror workflow trigger failed:", e?.message || e);
        }
      }
    }

    console.log("Update complete.");
  } catch (err) {
    console.error("Fatal error:", err.message);
    process.exit(1);
  }
})();