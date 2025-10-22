// Data loader for latest KPIs (last 7 days)
// This file loads data from the DuckDB exports

import {readFile} from "node:fs/promises";
import {fileURLToPath} from "node:url";
import {dirname, join} from "node:path";

const __dirname = dirname(fileURLToPath(import.meta.url));
const dataPath = join(__dirname, "../../data/exports/observable/latest_kpis.json");

try {
  const data = JSON.parse(await readFile(dataPath, "utf8"));

  // Transform timestamps to Date objects for better plotting
  const transformed = data.map(d => ({
    ...d,
    date: new Date(d.date)
  }));

  process.stdout.write(JSON.stringify(transformed));
} catch (error) {
  // If file doesn't exist yet, return empty array
  console.error(`Could not load ${dataPath}:`, error.message);
  process.stdout.write(JSON.stringify([]));
}
