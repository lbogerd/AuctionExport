#!/usr/bin/env node
/** Run the end-to-end export pipeline.
 *
 * 1) Copy AuctionExport SavedVariables into ./data with a timestamped name.
 * 2) Convert any new .lua/.lua.bak files in ./data to CSV (skipping ones that already have matching CSVs).
 */

import { spawnSync } from "node:child_process";
import * as path from "node:path";
import { fileURLToPath } from "node:url";

type Cli = {
  includeBak: boolean;
  accountRoot?: string;
  account?: string;
  dataDir?: string;
  verbose: boolean;
};

function ts(): string {
  const d = new Date();
  const pad = (n: number) => String(n).padStart(2, "0");
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(
    d.getHours()
  )}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}

function log(name: string, msg: string): void {
  process.stdout.write(`${ts()} [${name}] ${msg}\n`);
}

function warn(name: string, msg: string): void {
  process.stderr.write(`${ts()} [${name}] ${msg}\n`);
}

function parseArgs(argv: string[]): Cli {
  const out: Cli = { includeBak: false, verbose: false };

  const nextValue = (i: number): [string, number] => {
    if (i + 1 >= argv.length) throw new Error(`Missing value after ${argv[i]}`);
    return [argv[i + 1], i + 1];
  };

  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--include-bak") {
      out.includeBak = true;
      continue;
    }
    if (a === "--verbose") {
      out.verbose = true;
      continue;
    }
    if (a === "--account-root") {
      const [v, j] = nextValue(i);
      out.accountRoot = v;
      i = j;
      continue;
    }
    if (a === "--account") {
      const [v, j] = nextValue(i);
      out.account = v;
      i = j;
      continue;
    }
    if (a === "--data-dir") {
      const [v, j] = nextValue(i);
      out.dataDir = v;
      i = j;
      continue;
    }

    if (a.startsWith("-")) throw new Error(`Unknown argument: ${a}`);
    throw new Error(`Unexpected positional argument: ${a}`);
  }

  return out;
}

function runNodeTs(scriptPath: string, scriptArgs: string[]): number {
  const node = process.execPath;
  const args = ["--experimental-strip-types", scriptPath, ...scriptArgs];
  const completed = spawnSync(node, args, { stdio: "inherit" });

  // If Node couldn't start, map to a non-zero code.
  if (completed.error) return 2;
  return completed.status ?? 0;
}

function main(): number {
  let args: Cli;
  try {
    args = parseArgs(process.argv.slice(2));
  } catch (e) {
    warn("process", `ERROR: ${(e as Error).message}`);
    warn(
      "process",
      "Usage: node --experimental-strip-types ./process.ts [--include-bak] [--account-root PATH] [--account NAME] [--data-dir data] [--verbose]"
    );
    return 2;
  }

  const startTotal = performance.now();

  const repoDir = path.dirname(fileURLToPath(import.meta.url));
  const copyScript = path.join(repoDir, "copy_savedvariables_to_data.ts");
  const convertScript = path.join(repoDir, "auctionexport_to_csv.ts");

  const copyArgs: string[] = [];
  if (args.includeBak) copyArgs.push("--include-bak");
  if (args.accountRoot) copyArgs.push("--account-root", args.accountRoot);
  if (args.account) copyArgs.push("--account", args.account);
  if (args.dataDir) copyArgs.push("--data-dir", args.dataDir);

  log("process", "Step 1/2: Copy SavedVariables into ./data");
  let startStep = performance.now();
  let rc = runNodeTs(copyScript, copyArgs);
  log(
    "process",
    `Step 1/2 finished in ${((performance.now() - startStep) / 1000).toFixed(
      2
    )}s`
  );
  if (rc !== 0) {
    warn("process", `Copy step failed with exit code ${rc}`);
    return rc;
  }

  const convertArgs: string[] = [];
  if (args.dataDir) convertArgs.push("--data-dir", args.dataDir);
  if (args.verbose) convertArgs.push("--verbose");

  log("process", "Step 2/2: Convert exports in ./data to CSV");
  startStep = performance.now();
  rc = runNodeTs(convertScript, convertArgs);
  log(
    "process",
    `Step 2/2 finished in ${((performance.now() - startStep) / 1000).toFixed(
      2
    )}s`
  );
  log(
    "process",
    `Total pipeline time: ${((performance.now() - startTotal) / 1000).toFixed(
      2
    )}s`
  );
  return rc;
}

process.exitCode = main();
