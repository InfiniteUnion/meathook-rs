#!/usr/bin/env bun
/**
 * Sample CPU + RSS of a running nea_weather binary every 2s.
 * Appends JSONL to data/samples.jsonl — safe to stop/restart.
 *
 * Usage:
 *   bun run scripts/sample.mjs --duration 3          # sample for 3 minutes
 *   bun run scripts/sample.mjs --pid 97147 --duration 5
 *   bun run scripts/sample.mjs --until-stopped        # Ctrl-C to stop
 */
import { spawnSync } from "node:child_process";
import { appendFileSync, mkdirSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const OUTFILE = join(__dirname, "..", "data", "samples.jsonl");

const args = process.argv.slice(2);
const getArg = (name) => {
  const idx = args.indexOf(name);
  return idx >= 0 ? args[idx + 1] : null;
};
const hasFlag = (name) => args.includes(name);

const DURATION_MIN = parseFloat(getArg("--duration") || "3");
const UNTIL_STOPPED = hasFlag("--until-stopped");
const PID_ARG = getArg("--pid");
const INTERVAL_MS = 2000;

function discoverPid() {
  const result = spawnSync("pgrep", ["-f", "nea_weather"], {
    encoding: "utf8",
  });
  if (result.status !== 0 || !result.stdout.trim()) {
    console.error("Could not auto-discover nea_weather process. Pass --pid <PID>.");
    process.exit(1);
  }
  const pid = parseInt(result.stdout.trim().split("\n")[0], 10);
  return pid;
}

function sampleOnce(pid) {
  const result = spawnSync("ps", ["-o", "pid,%cpu,rss", "-p", String(pid), "-h"], {
    encoding: "utf8",
  });
  if (result.status !== 0 || !result.stdout.trim()) {
    return null;
  }
  const lines = result.stdout.trim().split("\n");
  const line = lines[lines.length - 1].trim();
  const parts = line.split(/\s+/);
  return {
    ts: new Date().toISOString(),
    cpu: parseFloat(parts[1]),
    rss: parseInt(parts[2], 10),
  };
}

async function main() {
  const pid = PID_ARG ? parseInt(PID_ARG, 10) : discoverPid();
  const totalSamples = UNTIL_STOPPED ? Infinity : Math.round((DURATION_MIN * 60 * 1000) / INTERVAL_MS);

  mkdirSync(dirname(OUTFILE), { recursive: true });

  console.error(`Sampling PID ${pid} every ${INTERVAL_MS}ms → ${OUTFILE}`);
  console.error(UNTIL_STOPPED ? "Until Ctrl-C." : `Duration: ${DURATION_MIN} min (${totalSamples} samples).`);
  console.error("");

  let n = 0;
  const startMs = Date.now();

  const tick = () => {
    const sample = sampleOnce(pid);
    if (!sample) {
      console.error(`sample ${n + 1}/${totalSamples === Infinity ? "∞" : totalSamples} — process gone, stopping.`);
      process.exit(0);
    }
    appendFileSync(OUTFILE, JSON.stringify(sample) + "\n");
    n++;
    const elapsed = Math.round((Date.now() - startMs) / 1000);
    process.stderr.write(
      `\rsample ${n}/${totalSamples === Infinity ? "∞" : totalSamples} · ${elapsed}s · cpu ${sample.cpu}% · rss ${sample.rss}kb   `,
    );

    if (!UNTIL_STOPPED && n >= totalSamples) {
      console.error("\nDone.");
      process.exit(0);
    }
  };

  tick();
  setInterval(tick, INTERVAL_MS);
}

main();
