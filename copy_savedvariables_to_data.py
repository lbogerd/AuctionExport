#!/usr/bin/env python3
"""Copy AuctionExport SavedVariables into ./data with a timestamped name.

Default WoW Retail path checked first:
  C:\Program Files (x86)\World of Warcraft\_retail_\WTF\Account

The script ignores the shared "SharedVariables" folder under Account.

Outputs to ./data using base name:
  AuctionExport-YYYYMMDDHHMMSS

Examples:
  python .\copy_savedvariables_to_data.py
  python .\copy_savedvariables_to_data.py --include-bak
  python .\copy_savedvariables_to_data.py --account YOURACCOUNTNAME
"""

from __future__ import annotations

import argparse
import logging
import os
import shutil
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Optional


logger = logging.getLogger("copy_savedvariables_to_data")


DEFAULT_ACCOUNT_ROOT = Path(
    r"C:\Program Files (x86)\World of Warcraft\_retail_\WTF\Account"
)


@dataclass(frozen=True)
class AccountChoice:
    name: str
    path: Path


def _list_account_folders(account_root: Path) -> List[AccountChoice]:
    if not account_root.exists():
        raise FileNotFoundError(f"Account root does not exist: {account_root}")
    if not account_root.is_dir():
        raise NotADirectoryError(f"Account root is not a directory: {account_root}")

    choices: List[AccountChoice] = []
    for entry in sorted(account_root.iterdir(), key=lambda p: p.name.lower()):
        if not entry.is_dir():
            continue
        # WoW keeps some non-account folders under Account.
        if entry.name.lower() in {"sharedvariables", "savedvariables"}:
            continue
        choices.append(AccountChoice(name=entry.name, path=entry))

    return choices


def _prompt_for_account(choices: List[AccountChoice]) -> AccountChoice:
    if not choices:
        raise ValueError("No account folders found")
    if len(choices) == 1:
        return choices[0]

    logger.info("Multiple WoW account folders found. Select one:")
    for idx, c in enumerate(choices, start=1):
        logger.info(f"  {idx}) {c.name}")

    def prompt_input(msg: str) -> str:
        # Keep the input prompt timestamped too.
        prefix = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return input(f"{prefix} [copy_savedvariables_to_data] {msg}")

    while True:
        raw = prompt_input("Enter number (or exact folder name): ").strip()
        if not raw:
            continue

        if raw.isdigit():
            idx = int(raw)
            if 1 <= idx <= len(choices):
                return choices[idx - 1]
            logger.warning(f"Invalid selection: {raw}")
            continue

        for c in choices:
            if c.name == raw:
                return c
        logger.warning(f"Unknown folder name: {raw}")


def _resolve_account(
    account_root: Path, requested_account: Optional[str]
) -> AccountChoice:
    choices = _list_account_folders(account_root)

    if requested_account:
        for c in choices:
            if c.name == requested_account:
                return c
        names = ", ".join(c.name for c in choices) or "<none>"
        raise ValueError(
            f"Account '{requested_account}' not found under {account_root}. Found: {names}"
        )

    return _prompt_for_account(choices)


def _copy_if_exists(src: Path, dest: Path) -> bool:
    if not src.exists():
        return False
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dest)
    return True


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Copy AuctionExport.lua (and optionally .bak) into ./data with a timestamped filename."
    )
    parser.add_argument(
        "--account-root",
        default=str(DEFAULT_ACCOUNT_ROOT),
        help=(
            "Path to WoW Retail WTF Account folder "
            "(default: C:\\Program Files (x86)\\World of Warcraft\\_retail_\\WTF\\Account)"
        ),
    )
    parser.add_argument(
        "--account",
        default=None,
        help="Account folder name under Account (if omitted, prompts when multiple exist)",
    )
    parser.add_argument(
        "--include-bak",
        action="store_true",
        help="Also copy AuctionExport.lua.bak when present",
    )
    parser.add_argument(
        "--data-dir",
        default="data",
        help="Destination directory (default: ./data)",
    )

    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    account_root = Path(args.account_root)
    try:
        account = _resolve_account(account_root, args.account)
    except Exception as e:
        logger.error(f"ERROR: {e}")
        return 2

    saved_vars_dir = account.path / "SavedVariables"
    src_lua = saved_vars_dir / "AuctionExport.lua"
    src_bak = saved_vars_dir / "AuctionExport.lua.bak"

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    base_name = f"AuctionExport-{timestamp}"

    dest_dir = Path(args.data_dir)
    dest_lua = dest_dir / f"{base_name}.lua"
    dest_bak = dest_dir / f"{base_name}.lua.bak"

    logger.info(f"Using account: {account.name}")
    logger.info(f"Source: {saved_vars_dir}")
    logger.info(f"Destination: {dest_dir.resolve()}")

    copied_any = False
    start_total = datetime.now()

    if _copy_if_exists(src_lua, dest_lua):
        copied_any = True
        logger.info(f"Copied: {src_lua} -> {dest_lua}")
    else:
        logger.info(f"Not found: {src_lua}")

    if args.include_bak:
        if _copy_if_exists(src_bak, dest_bak):
            copied_any = True
            logger.info(f"Copied: {src_bak} -> {dest_bak}")
        else:
            logger.info(f"Not found: {src_bak}")

    if not copied_any:
        logger.info("Nothing copied.")
        return 1

    elapsed = (datetime.now() - start_total).total_seconds()
    logger.info(f"Done in {elapsed:.2f}s")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
