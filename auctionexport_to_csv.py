#!/usr/bin/env python3
"""Convert AuctionExport saved variables (Lua table) to a CSV for Excel.

This expects a file that contains something like:
  AuctionExportDB = { ... ["lastScan"] = { ["rows"] = { { ... }, ... } } }

It extracts the array at AuctionExportDB.lastScan.rows into a CSV.
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import sys
import time
from typing import Any, Dict, List, Optional, Tuple


logger = logging.getLogger("auctionexport_to_csv")


def _iter_lua_inputs(data_dir: str) -> List[str]:
    paths: List[str] = []
    for root, _dirs, files in os.walk(data_dir):
        for name in files:
            lower = name.lower()
            if lower.endswith(".lua") or lower.endswith(".lua.bak"):
                paths.append(os.path.join(root, name))
    paths.sort(key=lambda p: p.lower())
    return paths


def _has_matching_csv(input_path: str) -> bool:
    directory = os.path.dirname(input_path) or "."
    base = os.path.basename(input_path)
    prefix = base + ".rows"
    try:
        for name in os.listdir(directory):
            lower = name.lower()
            if lower.endswith(".csv") and name.startswith(prefix):
                return True
    except FileNotFoundError:
        return False
    return False


def _default_output_csv_path(input_path: str) -> str:
    return input_path + ".rows.csv"


def _log_found_inputs(input_paths: List[str], base_dir: str) -> None:
    logger.info(f"Found {len(input_paths)} Lua export file(s).")

    # Avoid dumping huge lists at INFO level.
    max_show = 25
    shown = input_paths[:max_show]
    for p in shown:
        rel = os.path.relpath(p, base_dir)
        logger.info(f"  - {rel}")
    remaining = len(input_paths) - len(shown)
    if remaining > 0:
        logger.info(f"  ... and {remaining} more")


def _skip_lua_string(text: str, i: int) -> int:
    # Assumes text[i] == '"'
    i += 1
    while i < len(text):
        ch = text[i]
        if ch == "\\":
            i += 2
            continue
        if ch == '"':
            return i + 1
        i += 1
    return i


def _extract_balanced_braces(text: str, start: int) -> str:
    if start < 0 or start >= len(text) or text[start] != "{":
        raise ValueError("start must point at '{'")

    depth = 0
    i = start
    while i < len(text):
        ch = text[i]
        if ch == '"':
            i = _skip_lua_string(text, i)
            continue
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return text[start : i + 1]
        i += 1

    raise ValueError("Unbalanced braces while parsing Lua table")


def _find_rows_block(text: str) -> str:
    # Find the rows table: ["rows"] = { ... }
    rows_key_pos = text.find('["rows"]')
    if rows_key_pos == -1:
        raise ValueError('Could not find ["rows"] in input file')

    brace_pos = text.find("{", rows_key_pos)
    if brace_pos == -1:
        raise ValueError('Could not find opening "{" after ["rows"]')

    return _extract_balanced_braces(text, brace_pos)


def _extract_row_tables(rows_block: str) -> List[str]:
    # rows_block is "{ ... }" where inside is "{ ... }, { ... }, ..."
    inner = rows_block.strip()
    if not (inner.startswith("{") and inner.endswith("}")):
        raise ValueError("rows_block is not a Lua table")

    inner = inner[1:-1]

    rows: List[str] = []
    i = 0
    while i < len(inner):
        ch = inner[i]
        if ch.isspace() or ch == ",":
            i += 1
            continue
        if ch == '"':
            i = _skip_lua_string(inner, i)
            continue
        if ch != "{":
            # Unexpected token at top-level; skip forward defensively.
            i += 1
            continue

        row_text = _extract_balanced_braces(inner, i)
        rows.append(row_text)
        i += len(row_text)

    return rows


def _parse_lua_string(text: str, i: int) -> Tuple[str, int]:
    # Assumes text[i] == '"'
    i += 1
    out_chars: List[str] = []
    while i < len(text):
        ch = text[i]
        if ch == "\\" and i + 1 < len(text):
            # Keep common escapes; for anything else, just take the next char.
            nxt = text[i + 1]
            if nxt in ['\\', '"', "n", "r", "t"]:
                if nxt == "n":
                    out_chars.append("\n")
                elif nxt == "r":
                    out_chars.append("\r")
                elif nxt == "t":
                    out_chars.append("\t")
                else:
                    out_chars.append(nxt)
                i += 2
                continue
            out_chars.append(nxt)
            i += 2
            continue
        if ch == '"':
            return "".join(out_chars), i + 1
        out_chars.append(ch)
        i += 1
    return "".join(out_chars), i


def _parse_scalar(token: str) -> Any:
    token = token.strip()
    if token == "":
        return ""
    if token == "true":
        return True
    if token == "false":
        return False
    if token == "nil":
        return None

    # Integer / float
    try:
        if token.startswith("0x") or token.startswith("-0x"):
            return int(token, 16)
        if any(c in token for c in [".", "e", "E"]):
            return float(token)
        return int(token)
    except ValueError:
        return token


def _parse_row_table(row_table: str) -> Dict[str, Any]:
    s = row_table.strip()
    if not (s.startswith("{") and s.endswith("}")):
        raise ValueError("row_table is not a Lua table")

    s = s[1:-1]
    row: Dict[str, Any] = {}

    i = 0
    while i < len(s):
        key_start = s.find('["', i)
        if key_start == -1:
            break
        key_end = s.find('"]', key_start + 2)
        if key_end == -1:
            break

        key = s[key_start + 2 : key_end]
        eq_pos = s.find("=", key_end + 2)
        if eq_pos == -1:
            break

        j = eq_pos + 1
        while j < len(s) and s[j].isspace():
            j += 1

        if j >= len(s):
            break

        # Parse value
        if s[j] == '"':
            val, j2 = _parse_lua_string(s, j)
            value: Any = val
            j = j2
        elif s[j] == "{":
            raw = _extract_balanced_braces(s, j)
            value = raw
            j += len(raw)
        else:
            # Read until comma or end-of-table
            k = j
            while k < len(s) and s[k] not in [",", "\n", "\r"]:
                if s[k] == "}":
                    break
                k += 1
            value = _parse_scalar(s[j:k])
            j = k

        row[key] = value
        i = j

    return row


def _preferred_field_order(fields: List[str]) -> List[str]:
    preferred = [
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
    ]
    remaining = [f for f in fields if f not in preferred]
    ordered = [f for f in preferred if f in fields] + sorted(remaining)
    return ordered


def convert(input_path: str, output_csv_path: str) -> int:
    with open(input_path, "r", encoding="utf-8", errors="replace") as f:
        text = f.read()

    rows_block = _find_rows_block(text)
    row_tables = _extract_row_tables(rows_block)

    rows: List[Dict[str, Any]] = [_parse_row_table(rt) for rt in row_tables]
    if not rows:
        raise ValueError('Parsed 0 rows from ["rows"]')

    fieldnames_set = set()
    for r in rows:
        fieldnames_set.update(r.keys())

    # Retail AH does not expose seller names; omit the field even if present.
    fieldnames_set.discard("seller")

    fieldnames = _preferred_field_order(list(fieldnames_set))

    # Use UTF-8 with BOM so Excel on Windows auto-detects encoding.
    with open(output_csv_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for r in rows:
            r = {k: v for k, v in r.items() if k != "seller"}
            writer.writerow({k: ("" if v is None else v) for k, v in r.items()})

    return len(rows)


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Extract AuctionExportDB.lastScan.rows from a Lua file into CSV for Excel."
    )
    parser.add_argument(
        "input",
        nargs="*",
        help=(
            "Path(s) to AuctionExport.lua / AuctionExport.lua.bak (SavedVariables export). "
            "If omitted, scans ./data for .lua and .lua.bak files."
        ),
    )
    parser.add_argument(
        "-o",
        "--output",
        default=None,
        help="Output CSV path (default: <input>.rows.csv)",
    )
    parser.add_argument(
        "--data-dir",
        default="data",
        help="Directory to scan when no input paths are provided (default: ./data)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable more detailed logging",
    )

    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    start_total = time.perf_counter()

    input_paths: List[str]
    if args.input:
        input_paths = [os.path.abspath(p) for p in args.input]
    else:
        data_dir = os.path.abspath(args.data_dir)
        logger.info(f"Scanning for exports under: {data_dir}")
        input_paths = [os.path.abspath(p) for p in _iter_lua_inputs(data_dir)]

    base_dir = os.getcwd()

    if input_paths:
        _log_found_inputs(input_paths, base_dir)

    if not input_paths:
        logger.info("No .lua / .lua.bak files found.")
        return 0

    if args.output and len(input_paths) != 1:
        logger.error("ERROR: --output can only be used with a single input file.")
        return 2

    converted = 0
    skipped = 0
    failed = 0

    total = len(input_paths)
    for idx, input_path in enumerate(input_paths, start=1):
        if args.output:
            output_path = os.path.abspath(args.output)
        else:
            output_path = _default_output_csv_path(input_path)

        if _has_matching_csv(input_path):
            skipped += 1
            logger.info(f"[{idx}/{total}] Skip (CSV exists): {os.path.relpath(input_path, base_dir)}")
            continue

        try:
            logger.info(f"[{idx}/{total}] Converting: {os.path.relpath(input_path, base_dir)}")
            start_file = time.perf_counter()
            n = convert(input_path, output_path)
            elapsed = time.perf_counter() - start_file
            converted += 1
            logger.info(f"Wrote {n} rows to: {os.path.relpath(output_path, base_dir)}")
            logger.info(f"[{idx}/{total}] Done in {elapsed:.2f}s")
        except Exception as e:
            failed += 1
            logger.error(f"ERROR converting {input_path}: {e}")

    total_elapsed = time.perf_counter() - start_total
    logger.info(
        f"Done. Converted: {converted}, skipped (already had CSV): {skipped}, failed: {failed}."
    )
    logger.info(f"Total time: {total_elapsed:.2f}s")

    if failed:
        return 2
    if converted == 0:
        logger.info("Nothing to do.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
