#!/usr/bin/env python3
"""Convert AuctionExport saved variables (Lua table) to a CSV for Excel.

This expects a file that contains something like:
  AuctionExportDB = { ... ["lastScan"] = { ["rows"] = { { ... }, ... } } }

It extracts the array at AuctionExportDB.lastScan.rows into a CSV.
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from typing import Any, Dict, List, Optional, Tuple


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
        help="Path to AuctionExport.lua / AuctionExport.lua.bak (SavedVariables export)",
    )
    parser.add_argument(
        "-o",
        "--output",
        default=None,
        help="Output CSV path (default: <input>.rows.csv)",
    )

    args = parser.parse_args(argv)

    input_path = os.path.abspath(args.input)
    if args.output:
        output_path = os.path.abspath(args.output)
    else:
        output_path = input_path + ".rows.csv"

    try:
        n = convert(input_path, output_path)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 2

    print(f"Wrote {n} rows to: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
