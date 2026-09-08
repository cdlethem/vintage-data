import { readdir, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const ui = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const root = resolve(ui, "../src/airflow/providers/vintage/bot_dashboard/static");
const locate = async (name) => {
  const matches = (await readdir(resolve(root, name))).filter((entry) => /^main\.[A-Za-z0-9_-]+\.umd\.cjs$/.test(entry));
  if (matches.length !== 1) throw new Error(`expected one ${name} bundle, found ${matches.length}`);
  return `${name}/${matches[0]}`;
};
const manifest = { dashboard: await locate("dashboard"), app: await locate("app") };
await writeFile(resolve(root, "asset-manifest.json"), `${JSON.stringify(manifest, null, 2)}\n`, "utf8");
