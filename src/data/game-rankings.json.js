// Data loader for game rankings

import {readFile} from "node:fs/promises";
import {fileURLToPath} from "node:url";
import {dirname, join} from "node:path";

const __dirname = dirname(fileURLToPath(import.meta.url));
const dataPath = join(__dirname, "../../data/exports/observable/game_rankings.json");

try {
  const data = JSON.parse(await readFile(dataPath, "utf8"));
  process.stdout.write(JSON.stringify(data));
} catch (error) {
  console.error(`Could not load ${dataPath}:`, error.message);
  process.stdout.write(JSON.stringify([]));
}
