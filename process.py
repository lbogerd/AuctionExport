#!/usr/bin/env python3
"""Run the end-to-end export pipeline.

1) Copy AuctionExport SavedVariables into ./data with a timestamped name.
2) Convert any new .lua/.lua.bak files in ./data to CSV (skipping ones that already have matching CSVs).

Usage:
  python .\process.py
  python .\process.py --include-bak
"""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys
import time
from pathlib import Path
from typing import List, Optional


logger = logging.getLogger("process")


def _run(cmd: List[str]) -> int:
    completed = subprocess.run(cmd)
    return int(completed.returncode)


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Copy SavedVariables into ./data then convert to CSV."
    )
    parser.add_argument(
        "--include-bak",
        action="store_true",
        help="Also copy AuctionExport.lua.bak when present",
    )
    parser.add_argument(
        "--account-root",
        default=None,
        help="Override WoW WTF Account root (passed through to copy script)",
    )
    parser.add_argument(
        "--account",
        default=None,
        help="Account folder name under Account (passed through to copy script)",
    )
    parser.add_argument(
        "--data-dir",
        default=None,
        help="Destination directory (passed through to both scripts)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable more detailed logging for the CSV converter",
    )

    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    start_total = time.perf_counter()

    repo_dir = Path(__file__).resolve().parent
    copy_script = repo_dir / "copy_savedvariables_to_data.py"
    convert_script = repo_dir / "auctionexport_to_csv.py"

    copy_cmd: List[str] = [sys.executable, str(copy_script)]
    if args.include_bak:
        copy_cmd.append("--include-bak")
    if args.account_root:
        copy_cmd += ["--account-root", args.account_root]
    if args.account:
        copy_cmd += ["--account", args.account]
    if args.data_dir:
        copy_cmd += ["--data-dir", args.data_dir]

    logger.info("Step 1/2: Copy SavedVariables into ./data")
    start_step = time.perf_counter()
    rc = _run(copy_cmd)
    logger.info(f"Step 1/2 finished in {(time.perf_counter() - start_step):.2f}s")
    if rc != 0:
        logger.error(f"Copy step failed with exit code {rc}")
        return rc

    convert_cmd: List[str] = [sys.executable, str(convert_script)]
    if args.data_dir:
        convert_cmd += ["--data-dir", args.data_dir]
    if args.verbose:
        convert_cmd.append("--verbose")

    logger.info("Step 2/2: Convert exports in ./data to CSV")
    start_step = time.perf_counter()
    rc = _run(convert_cmd)
    logger.info(f"Step 2/2 finished in {(time.perf_counter() - start_step):.2f}s")
    logger.info(f"Total pipeline time: {(time.perf_counter() - start_total):.2f}s")
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
