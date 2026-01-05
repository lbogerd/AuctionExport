#!/usr/bin/env node
/** Convert AuctionExport saved variables (Lua table) to a CSV for Excel.
 *
 * Expects a file that contains something like:
 *   AuctionExportDB = { ... ["lastScan"] = { ["rows"] = { { ... }, ... } } }
 *
 * Extracts the array at AuctionExportDB.lastScan.rows into a CSV.
 */

import * as fs from "node:fs";
import * as path from "node:path";

type Row = Record<string, unknown>;

type Cli = {
  input: string[];
  output?: string;
  dataDir: string;
  verbose: boolean;
};

const PREFERRED_FIELDS = [
  "index",
  "name",
  "itemLink",
  "itemId",
  "count",
  "quality",
  "timeLeft",
  "minBidCopper",
  "buyoutCopper",
  "hasAllInfo",
  "scannedAtUtc",
] as const;

function ts(): string {
  const d = new Date();
  const pad = (n: number) => String(n).padStart(2, "0");
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(
    d.getHours()
  )}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}

function log(name: string, msg: string): void {
  // Match Python logging-ish format.
  process.stdout.write(`${ts()} [${name}] ${msg}\n`);
}

function warn(name: string, msg: string): void {
  process.stderr.write(`${ts()} [${name}] ${msg}\n`);
}

function parseArgs(argv: string[]): Cli {
  const out: Cli = { input: [], dataDir: "data", verbose: false };

  const nextValue = (i: number): [string, number] => {
    if (i + 1 >= argv.length) throw new Error(`Missing value after ${argv[i]}`);
    return [argv[i + 1], i + 1];
  };

  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--verbose") {
      out.verbose = true;
      continue;
    }
    if (a === "--data-dir") {
      const [v, j] = nextValue(i);
      out.dataDir = v;
      i = j;
      continue;
    }
    if (a === "-o" || a === "--output") {
      const [v, j] = nextValue(i);
      out.output = v;
      i = j;
      continue;
    }
    if (a.startsWith("-")) {
      throw new Error(`Unknown argument: ${a}`);
    }
    out.input.push(a);
  }

  return out;
}

function* walkFiles(rootDir: string): Generator<string> {
  const stack: string[] = [rootDir];
  while (stack.length) {
    const dir = stack.pop()!;
    let entries: fs.Dirent[];
    try {
      entries = fs.readdirSync(dir, { withFileTypes: true });
    } catch {
      continue;
    }
    for (const e of entries) {
      const full = path.join(dir, e.name);
      if (e.isDirectory()) {
        stack.push(full);
      } else if (e.isFile()) {
        yield full;
      }
    }
  }
}

function iterLuaInputs(dataDir: string): string[] {
  const paths: string[] = [];
  for (const p of walkFiles(dataDir)) {
    const lower = p.toLowerCase();
    if (lower.endsWith(".lua") || lower.endsWith(".lua.bak")) paths.push(p);
  }
  paths.sort((a, b) => a.toLowerCase().localeCompare(b.toLowerCase()));
  return paths;
}

function hasMatchingCsv(inputPath: string): boolean {
  const directory = path.dirname(inputPath) || ".";
  const base = path.basename(inputPath);
  const prefix = `${base}.rows`;

  let names: string[];
  try {
    names = fs.readdirSync(directory);
  } catch {
    return false;
  }

  for (const name of names) {
    const lower = name.toLowerCase();
    if (lower.endsWith(".csv") && name.startsWith(prefix)) return true;
  }
  return false;
}

function defaultOutputCsvPath(inputPath: string): string {
  return `${inputPath}.rows.csv`;
}

function skipLuaString(text: string, i: number): number {
  // Assumes text[i] === '"'
  i++;
  while (i < text.length) {
    const ch = text[i];
    if (ch === "\\") {
      i += 2;
      continue;
    }
    if (ch === '"') return i + 1;
    i++;
  }
  return i;
}

function extractBalancedBraces(text: string, start: number): string {
  if (start < 0 || start >= text.length || text[start] !== "{") {
    throw new Error("start must point at '{'");
  }

  let depth = 0;
  let i = start;
  while (i < text.length) {
    const ch = text[i];
    if (ch === '"') {
      i = skipLuaString(text, i);
      continue;
    }
    if (ch === "{") {
      depth++;
    } else if (ch === "}") {
      depth--;
      if (depth === 0) return text.slice(start, i + 1);
    }
    i++;
  }

  throw new Error("Unbalanced braces while parsing Lua table");
}

function findRowsBlock(text: string): string {
  const rowsKeyPos = text.indexOf('["rows"]');
  if (rowsKeyPos === -1)
    throw new Error('Could not find ["rows"] in input file');

  const bracePos = text.indexOf("{", rowsKeyPos);
  if (bracePos === -1)
    throw new Error('Could not find opening "{" after ["rows"]');

  return extractBalancedBraces(text, bracePos);
}

function extractRowTables(rowsBlock: string): string[] {
  let inner = rowsBlock.trim();
  if (!(inner.startsWith("{") && inner.endsWith("}"))) {
    throw new Error("rowsBlock is not a Lua table");
  }

  inner = inner.slice(1, -1);

  const rows: string[] = [];
  let i = 0;
  while (i < inner.length) {
    const ch = inner[i];
    if (ch === "," || ch === " " || ch === "\t" || ch === "\n" || ch === "\r") {
      i++;
      continue;
    }
    if (ch === '"') {
      i = skipLuaString(inner, i);
      continue;
    }
    if (ch !== "{") {
      // Unexpected token at top-level; skip forward defensively.
      i++;
      continue;
    }

    const rowText = extractBalancedBraces(inner, i);
    rows.push(rowText);
    i += rowText.length;
  }

  return rows;
}

function parseLuaString(text: string, i: number): [string, number] {
  // Assumes text[i] === '"'
  i++;
  const out: string[] = [];
  while (i < text.length) {
    const ch = text[i];
    if (ch === "\\" && i + 1 < text.length) {
      const nxt = text[i + 1];
      if (
        nxt === "\\" ||
        nxt === '"' ||
        nxt === "n" ||
        nxt === "r" ||
        nxt === "t"
      ) {
        if (nxt === "n") out.push("\n");
        else if (nxt === "r") out.push("\r");
        else if (nxt === "t") out.push("\t");
        else out.push(nxt);
        i += 2;
        continue;
      }
      out.push(nxt);
      i += 2;
      continue;
    }
    if (ch === '"') return [out.join(""), i + 1];
    out.push(ch);
    i++;
  }
  return [out.join(""), i];
}

function parseScalar(token: string): unknown {
  const t = token.trim();
  if (t === "") return "";
  if (t === "true") return true;
  if (t === "false") return false;
  if (t === "nil") return null;

  // integer / float
  if (t.startsWith("0x") || t.startsWith("-0x")) {
    const n = Number.parseInt(t, 16);
    if (!Number.isNaN(n)) return n;
    return t;
  }

  if (/[.eE]/.test(t)) {
    const n = Number.parseFloat(t);
    if (!Number.isNaN(n)) return n;
    return t;
  }

  const n = Number.parseInt(t, 10);
  if (!Number.isNaN(n) && String(n) === t) return n;
  return t;
}

function parseRowTable(rowTable: string): Row {
  let s = rowTable.trim();
  if (!(s.startsWith("{") && s.endsWith("}")))
    throw new Error("rowTable is not a Lua table");

  s = s.slice(1, -1);
  const row: Row = {};

  let i = 0;
  while (i < s.length) {
    const keyStart = s.indexOf('["', i);
    if (keyStart === -1) break;

    const keyEnd = s.indexOf('"]', keyStart + 2);
    if (keyEnd === -1) break;

    const key = s.slice(keyStart + 2, keyEnd);

    const eqPos = s.indexOf("=", keyEnd + 2);
    if (eqPos === -1) break;

    let j = eqPos + 1;
    while (
      j < s.length &&
      (s[j] === " " || s[j] === "\t" || s[j] === "\n" || s[j] === "\r")
    )
      j++;
    if (j >= s.length) break;

    let value: unknown;
    if (s[j] === '"') {
      const [val, j2] = parseLuaString(s, j);
      value = val;
      j = j2;
    } else if (s[j] === "{") {
      const raw = extractBalancedBraces(s, j);
      value = raw;
      j += raw.length;
    } else {
      let k = j;
      while (k < s.length && s[k] !== "," && s[k] !== "\n" && s[k] !== "\r") {
        if (s[k] === "}") break;
        k++;
      }
      value = parseScalar(s.slice(j, k));
      j = k;
    }

    row[key] = value;
    i = j;
  }

  return row;
}

function preferredFieldOrder(fields: string[]): string[] {
  const preferred: string[] = [...PREFERRED_FIELDS];
  const remaining = fields.filter((f) => !preferred.includes(f));
  const ordered = preferred
    .filter((f) => fields.includes(f))
    .concat(remaining.sort());
  return ordered;
}

function csvEscape(value: unknown): string {
  if (value === null || value === undefined) return "";
  const s = String(value);
  const needsQuotes = /[",\r\n]/.test(s) || /^\s|\s$/.test(s);
  if (!needsQuotes) return s;
  return `"${s.replace(/"/g, '""')}"`;
}

function writeCsvUtf8Bom(
  filePath: string,
  headers: string[],
  rows: Row[]
): void {
  const bom = "\uFEFF";
  const lines: string[] = [];
  lines.push(headers.map((h) => csvEscape(h)).join(","));
  for (const r of rows) {
    const line = headers.map((h) => csvEscape((r as any)[h])).join(",");
    lines.push(line);
  }
  const content = bom + lines.join("\r\n") + "\r\n";
  fs.writeFileSync(filePath, content, { encoding: "utf8" });
}

function convert(inputPath: string, outputCsvPath: string): number {
  const text = fs.readFileSync(inputPath, { encoding: "utf8" });

  const rowsBlock = findRowsBlock(text);
  const rowTables = extractRowTables(rowsBlock);

  const rows: Row[] = rowTables.map(parseRowTable);
  if (!rows.length) throw new Error('Parsed 0 rows from ["rows"]');

  const fieldSet = new Set<string>();
  for (const r of rows) {
    for (const k of Object.keys(r)) fieldSet.add(k);
  }

  // Retail AH does not expose seller names; omit the field even if present.
  fieldSet.delete("seller");

  const fieldnames = preferredFieldOrder([...fieldSet]);

  // Ensure seller omitted even if present.
  const sanitized = rows.map((r) => {
    const out: Row = {};
    for (const [k, v] of Object.entries(r)) {
      if (k === "seller") continue;
      out[k] = v === null ? "" : v;
    }
    return out;
  });

  writeCsvUtf8Bom(outputCsvPath, fieldnames, sanitized);
  return rows.length;
}

function rel(p: string, baseDir: string): string {
  const r = path.relative(baseDir, p);
  return r || ".";
}

function logFoundInputs(inputPaths: string[], baseDir: string): void {
  log("auctionexport_to_csv", `Found ${inputPaths.length} Lua export file(s).`);
  const maxShow = 25;
  const shown = inputPaths.slice(0, maxShow);
  for (const p of shown) log("auctionexport_to_csv", `  - ${rel(p, baseDir)}`);
  const remaining = inputPaths.length - shown.length;
  if (remaining > 0) log("auctionexport_to_csv", `  ... and ${remaining} more`);
}

function main(): number {
  let args: Cli;
  try {
    args = parseArgs(process.argv.slice(2));
  } catch (e) {
    warn("auctionexport_to_csv", `ERROR: ${(e as Error).message}`);
    warn(
      "auctionexport_to_csv",
      "Usage: node --experimental-strip-types ./auctionexport_to_csv.ts [--data-dir data] [--verbose] [-o out.csv] [input1.lua ...]"
    );
    return 2;
  }

  const startTotal = performance.now();

  let inputPaths: string[];
  if (args.input.length) {
    inputPaths = args.input.map((p) => path.resolve(p));
  } else {
    const dataDir = path.resolve(args.dataDir);
    log("auctionexport_to_csv", `Scanning for exports under: ${dataDir}`);
    inputPaths = iterLuaInputs(dataDir).map((p) => path.resolve(p));
  }

  const baseDir = process.cwd();

  if (inputPaths.length) logFoundInputs(inputPaths, baseDir);
  if (!inputPaths.length) {
    log("auctionexport_to_csv", "No .lua / .lua.bak files found.");
    return 0;
  }

  if (args.output && inputPaths.length !== 1) {
    warn(
      "auctionexport_to_csv",
      "ERROR: --output can only be used with a single input file."
    );
    return 2;
  }

  let converted = 0;
  let skipped = 0;
  let failed = 0;

  const total = inputPaths.length;
  for (let idx = 0; idx < inputPaths.length; idx++) {
    const inputPath = inputPaths[idx];
    const outputPath = args.output
      ? path.resolve(args.output)
      : defaultOutputCsvPath(inputPath);

    if (hasMatchingCsv(inputPath)) {
      skipped++;
      log(
        "auctionexport_to_csv",
        `[${idx + 1}/${total}] Skip (CSV exists): ${rel(inputPath, baseDir)}`
      );
      continue;
    }

    try {
      log(
        "auctionexport_to_csv",
        `[${idx + 1}/${total}] Converting: ${rel(inputPath, baseDir)}`
      );
      const startFile = performance.now();
      const n = convert(inputPath, outputPath);
      const elapsed = (performance.now() - startFile) / 1000;
      converted++;
      log(
        "auctionexport_to_csv",
        `Wrote ${n} rows to: ${rel(outputPath, baseDir)}`
      );
      log(
        "auctionexport_to_csv",
        `[${idx + 1}/${total}] Done in ${elapsed.toFixed(2)}s`
      );
    } catch (e) {
      failed++;
      warn(
        "auctionexport_to_csv",
        `ERROR converting ${inputPath}: ${(e as Error).message}`
      );
      if (args.verbose) {
        warn("auctionexport_to_csv", String(e));
      }
    }
  }

  const totalElapsed = (performance.now() - startTotal) / 1000;
  log(
    "auctionexport_to_csv",
    `Done. Converted: ${converted}, skipped (already had CSV): ${skipped}, failed: ${failed}.`
  );
  log("auctionexport_to_csv", `Total time: ${totalElapsed.toFixed(2)}s`);

  if (failed) return 2;
  if (converted === 0) log("auctionexport_to_csv", "Nothing to do.");
  return 0;
}

process.exitCode = main();
