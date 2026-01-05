#!/usr/bin/env node
/** Copy AuctionExport SavedVariables into ./data with a timestamped name.
 *
 * Default WoW Retail path checked first:
 *   C:\Program Files (x86)\World of Warcraft\_retail_\WTF\Account
 *
 * The script ignores the shared "SharedVariables" folder under Account.
 */

import * as fs from "node:fs";
import * as path from "node:path";
import { stdin as input, stdout as output } from "node:process";
import * as readline from "node:readline/promises";

const DEFAULT_ACCOUNT_ROOT =
  "C:\\Program Files (x86)\\World of Warcraft\\_retail_\\WTF\\Account";

type AccountChoice = { name: string; fullPath: string };

type Cli = {
  accountRoot: string;
  account?: string;
  includeBak: boolean;
  dataDir: string;
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
  const out: Cli = {
    accountRoot: DEFAULT_ACCOUNT_ROOT,
    includeBak: false,
    dataDir: "data",
  };

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

function listAccountFolders(accountRoot: string): AccountChoice[] {
  if (!fs.existsSync(accountRoot))
    throw new Error(`Account root does not exist: ${accountRoot}`);
  const st = fs.statSync(accountRoot);
  if (!st.isDirectory())
    throw new Error(`Account root is not a directory: ${accountRoot}`);

  const entries = fs.readdirSync(accountRoot, { withFileTypes: true });
  const choices: AccountChoice[] = [];

  const sorted = entries
    .filter((e) => e.isDirectory())
    .map((e) => e.name)
    .sort((a, b) => a.toLowerCase().localeCompare(b.toLowerCase()));

  for (const name of sorted) {
    const lower = name.toLowerCase();
    if (lower === "sharedvariables" || lower === "savedvariables") continue;
    choices.push({ name, fullPath: path.join(accountRoot, name) });
  }

  return choices;
}

async function promptForAccount(
  choices: AccountChoice[]
): Promise<AccountChoice> {
  if (!choices.length) throw new Error("No account folders found");
  if (choices.length === 1) return choices[0];

  log(
    "copy_savedvariables_to_data",
    "Multiple WoW account folders found. Select one:"
  );
  for (let idx = 0; idx < choices.length; idx++) {
    log("copy_savedvariables_to_data", `  ${idx + 1}) ${choices[idx].name}`);
  }

  const rl = readline.createInterface({ input, output });
  try {
    while (true) {
      const raw = (
        await rl.question(
          `${ts()} [copy_savedvariables_to_data] Enter number (or exact folder name): `
        )
      ).trim();
      if (!raw) continue;

      if (/^\d+$/.test(raw)) {
        const idx = Number.parseInt(raw, 10);
        if (idx >= 1 && idx <= choices.length) return choices[idx - 1];
        warn("copy_savedvariables_to_data", `Invalid selection: ${raw}`);
        continue;
      }

      const exact = choices.find((c) => c.name === raw);
      if (exact) return exact;
      warn("copy_savedvariables_to_data", `Unknown folder name: ${raw}`);
    }
  } finally {
    rl.close();
  }
}

async function resolveAccount(
  accountRoot: string,
  requestedAccount?: string
): Promise<AccountChoice> {
  const choices = listAccountFolders(accountRoot);

  if (requestedAccount) {
    const match = choices.find((c) => c.name === requestedAccount);
    if (match) return match;
    const names = choices.map((c) => c.name).join(", ") || "<none>";
    throw new Error(
      `Account '${requestedAccount}' not found under ${accountRoot}. Found: ${names}`
    );
  }

  return promptForAccount(choices);
}

function copyIfExists(src: string, dest: string): boolean {
  if (!fs.existsSync(src)) return false;
  fs.mkdirSync(path.dirname(dest), { recursive: true });
  fs.copyFileSync(src, dest);
  return true;
}

function timestampYmdHms(): string {
  const d = new Date();
  const pad = (n: number) => String(n).padStart(2, "0");
  return `${d.getFullYear()}${pad(d.getMonth() + 1)}${pad(d.getDate())}${pad(
    d.getHours()
  )}${pad(d.getMinutes())}${pad(d.getSeconds())}`;
}

async function main(): Promise<number> {
  let args: Cli;
  try {
    args = parseArgs(process.argv.slice(2));
  } catch (e) {
    warn("copy_savedvariables_to_data", `ERROR: ${(e as Error).message}`);
    warn(
      "copy_savedvariables_to_data",
      "Usage: node --experimental-strip-types ./copy_savedvariables_to_data.ts [--account-root PATH] [--account NAME] [--include-bak] [--data-dir data]"
    );
    return 2;
  }

  let account: AccountChoice;
  try {
    account = await resolveAccount(args.accountRoot, args.account);
  } catch (e) {
    warn("copy_savedvariables_to_data", `ERROR: ${(e as Error).message}`);
    return 2;
  }

  const savedVarsDir = path.join(account.fullPath, "SavedVariables");
  const srcLua = path.join(savedVarsDir, "AuctionExport.lua");
  const srcBak = path.join(savedVarsDir, "AuctionExport.lua.bak");

  const baseName = `AuctionExport-${timestampYmdHms()}`;
  const destDir = path.resolve(args.dataDir);
  const destLua = path.join(destDir, `${baseName}.lua`);
  const destBak = path.join(destDir, `${baseName}.lua.bak`);

  log("copy_savedvariables_to_data", `Using account: ${account.name}`);
  log("copy_savedvariables_to_data", `Source: ${savedVarsDir}`);
  log("copy_savedvariables_to_data", `Destination: ${destDir}`);

  let copiedAny = false;
  const start = performance.now();

  if (copyIfExists(srcLua, destLua)) {
    copiedAny = true;
    log("copy_savedvariables_to_data", `Copied: ${srcLua} -> ${destLua}`);
  } else {
    log("copy_savedvariables_to_data", `Not found: ${srcLua}`);
  }

  if (args.includeBak) {
    if (copyIfExists(srcBak, destBak)) {
      copiedAny = true;
      log("copy_savedvariables_to_data", `Copied: ${srcBak} -> ${destBak}`);
    } else {
      log("copy_savedvariables_to_data", `Not found: ${srcBak}`);
    }
  }

  if (!copiedAny) {
    log("copy_savedvariables_to_data", "Nothing copied.");
    return 1;
  }

  const elapsed = (performance.now() - start) / 1000;
  log("copy_savedvariables_to_data", `Done in ${elapsed.toFixed(2)}s`);
  return 0;
}

main().then((code) => {
  process.exitCode = code;
});
