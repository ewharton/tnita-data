#!/usr/bin/env node
/**
 * Fetches collectors_artists_agg.json from the GH Pages CDN,
 * replaces meta.configUpdates with config_updates.json (so deletions and additions are reflected),
 * and writes the result. Leaves snapshot_date, collectors, artists, etc. unchanged.
 *
 * Run locally: node scripts/update_config_cdn.js
 * Run locally and deploy: node scripts/update_config_cdn.js --push
 *   (commits and pushes public/, which triggers the workflow to deploy)
 * Or as part of the update-config-cdn GitHub workflow.
 */

import fs from "fs";
import path from "path";
import { execSync } from "child_process";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");

const CDN_URL = "https://ewharton.github.io/tnita-data/collectors_artists_agg.json";
const CONFIG_UPDATES_PATH = path.join(ROOT, "config_updates.json");
const OUT_PATH = path.join(ROOT, "public", "collectors_artists_agg.json");

async function fetchUrl(url) {
  const res = await fetch(url, {
    signal: AbortSignal.timeout(120_000),
    headers: { Accept: "application/json" },
  });
  if (!res.ok) throw new Error(`${url} ${res.status}`);
  return res.json();
}

async function fetchFromCdn() {
  const data = await fetchUrl(CDN_URL);
  if (!data?.meta || !Array.isArray(data.artists) || !Array.isArray(data.collectors)) {
    throw new Error("CDN response missing meta/artists/collectors");
  }
  return data;
}

function loadConfigUpdates() {
  const raw = fs.readFileSync(CONFIG_UPDATES_PATH, "utf8");
  const parsed = JSON.parse(raw);
  if (typeof parsed !== "object" || parsed === null) {
    throw new Error("config_updates.json must be a JSON object");
  }
  return parsed;
}

function main() {
  console.log("update_config_cdn: fetching current collectors_artists_agg.json ...");

  const run = async () => {
    const agg = await fetchFromCdn();

    const configUpdates = loadConfigUpdates();
    agg.meta = agg.meta ?? {};
    agg.meta.configUpdates = configUpdates;

    fs.mkdirSync(path.dirname(OUT_PATH), { recursive: true });
    fs.writeFileSync(OUT_PATH, JSON.stringify(agg), "utf8");
    console.log(`Wrote ${OUT_PATH}`);

    const indexPath = path.join(ROOT, "public", "index.html");
    fs.writeFileSync(indexPath, `Config CDN updated at ${new Date().toISOString()}\n`, "utf8");
    console.log(`Wrote ${indexPath}`);

    if (process.argv.includes("--push")) {
      execSync("git add public/collectors_artists_agg.json public/index.html", {
        cwd: ROOT,
        stdio: "inherit",
      });
      try {
        execSync("git diff --staged --quiet", { cwd: ROOT });
        console.log("No changes to push.");
      } catch {
        execSync('git commit -m "chore: update config CDN"', {
          cwd: ROOT,
          stdio: "inherit",
        });
        execSync("git push", { cwd: ROOT, stdio: "inherit" });
        console.log("Pushed to main; workflow will deploy.");
      }
    }
  };

  run().catch((err) => {
    console.error(err?.message || err);
    process.exit(1);
  });
}

main();
