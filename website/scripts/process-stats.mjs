#!/usr/bin/env bun
/**
 * Process raw JSONL samples into footprint.json for the Footprint component.
 *
 * Reads:  data/samples.jsonl
 * Writes: src/data/footprint.json
 *
 * Usage:
 *   bun run scripts/process-stats.mjs
 */
import { readFileSync, writeFileSync, existsSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const INFILE = join(__dirname, "..", "data", "samples.jsonl");
const OUTFILE = join(__dirname, "..", "src", "data", "footprint.json");

function loadSamples() {
  if (!existsSync(INFILE)) {
    console.error(`No samples file at ${INFILE}. Run scripts/sample.mjs first.`);
    process.exit(1);
  }
  const raw = readFileSync(INFILE, "utf8");
  return raw
    .trim()
    .split("\n")
    .filter(Boolean)
    .map((line) => {
      try {
        return JSON.parse(line);
      } catch {
        return null;
      }
    })
    .filter(Boolean);
}

function downsample(samples, maxPoints = 120) {
  if (samples.length <= maxPoints) {
    return samples.map((s, i) => ({
      t: i * 2,
      cpu: s.cpu,
      rss: Math.round((s.rss / 1024) * 10) / 10,
    }));
  }
  const bucketSize = Math.ceil(samples.length / maxPoints);
  const buckets = [];
  for (let i = 0; i < samples.length; i += bucketSize) {
    const chunk = samples.slice(i, i + bucketSize);
    const cpu = Math.max(...chunk.map((s) => s.cpu));
    const rss = chunk.reduce((sum, s) => sum + s.rss, 0) / chunk.length;
    buckets.push({
      t: i * 2,
      cpu: Math.round(cpu * 100) / 100,
      rss: Math.round((rss / 1024) * 10) / 10,
    });
  }
  return buckets;
}

function main() {
  const raw = loadSamples();
  const samples = raw.filter(
    (s) =>
      s.cpu !== null &&
      s.cpu !== undefined &&
      !Number.isNaN(s.cpu) &&
      s.rss !== null &&
      s.rss !== undefined &&
      !Number.isNaN(s.rss),
  );
  const skipped = raw.length - samples.length;
  if (skipped > 0) {
    console.error(`Filtered ${skipped} invalid samples (null/NaN).`);
  }
  if (samples.length === 0) {
    console.error("No valid samples found.");
    process.exit(1);
  }

  const cpuValues = samples.map((s) => s.cpu);
  const rssValuesMb = samples.map((s) => s.rss / 1024);

  const avg = (arr) => arr.reduce((a, b) => a + b, 0) / arr.length;
  const peak = (arr) => Math.max(...arr);

  const cpuAvg = Math.round(avg(cpuValues) * 100) / 100;
  const cpuPeak = Math.round(peak(cpuValues) * 100) / 100;
  const rssAvg = Math.round(avg(rssValuesMb) * 10) / 10;
  const rssPeak = Math.round(peak(rssValuesMb) * 10) / 10;

  const totalSecs = samples.length * 2;
  const durationLabel =
    totalSecs >= 3600
      ? `${Math.round(totalSecs / 3600)}h`
      : totalSecs >= 60
        ? `${Math.round(totalSecs / 60)} min`
        : `${totalSecs}s`;

  const chart = downsample(samples);

  const out = {
    samples: samples.length,
    duration_label: durationLabel,
    cpu: { avg: cpuAvg, peak: cpuPeak, unit: "%" },
    rss: { avg_mb: rssAvg, peak_mb: rssPeak },
    chart,
  };

  writeFileSync(OUTFILE, JSON.stringify(out, null, 2) + "\n");
  console.error(`Wrote ${OUTFILE}`);
  console.error(`  ${samples.length} samples · ${durationLabel}`);
  console.error(`  cpu: avg ${cpuAvg}% · peak ${cpuPeak}%`);
  console.error(`  rss: avg ${rssAvg}mb · peak ${rssPeak}mb`);
}

main();
