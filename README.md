# AuctionExport

Small WoW addon to export Auction House replicate data to SavedVariables.

## Usage

1. Open the Auction House.
2. Run `/ahexport scan` to request a replicate scan (Blizzard throttles this).
3. When you see the "Replicate list ready" message, run `/ahexport read`.
4. Optionally run `/ahexport enrich` to try to fill missing item names/links/quality (rate-limited).

Note: WoW only writes SavedVariables to disk on `/reload`, logout, or exit.
The file is stored under `WTF\\Account\\<account>\\SavedVariables\\AuctionExport.lua` (not directly under the account folder).

## Load SavedVariables into Excel

If you have a SavedVariables backup like `AuctionExport.lua.bak` (or the normal `AuctionExport.lua`) that contains `AuctionExportDB.lastScan.rows`, you can convert it to a CSV that Excel opens cleanly.

Optional helper: copy your latest SavedVariables into `./data` with a timestamped filename:

- `node --experimental-strip-types .\copy_savedvariables_to_data.ts`
- Add `--include-bak` to also copy `AuctionExport.lua.bak` when present.

1. Run the converter (requires a recent Node that supports TypeScript type-stripping):

   - `node --experimental-strip-types .\auctionexport_to_csv.ts "C:\\Path\\To\\AuctionExport.lua.bak"`
   - Output will be written next to the input as `AuctionExport.lua.bak.rows.csv` (UTF-8 with BOM).

   Or, to batch-convert exports under `./data`:

   - `node --experimental-strip-types .\auctionexport_to_csv.ts`
   - The script scans `./data` for `.lua` and `.lua.bak` files and only converts files that don’t already have a matching `*.rows*.csv` next to them.

### End-to-end pipeline

Run copy + convert in one command:

- `node --experimental-strip-types .\process.ts`
- Optional flags: `--include-bak`, `--account-root <path>`, `--account <name>`, `--data-dir <dir>`, `--verbose`

2. Open the CSV in Excel:
   - Excel → Data → From Text/CSV → select the generated `.csv`.

### Commands

- `/ahexport scan` — Request replicate scan (AH must be open; may be throttled).
- `/ahexport scan cancel` — Cancel scan wait/progress timer.
- `/ahexport stopscan` — Cancel scan wait/progress timer.
- `/ahexport read` — Read replicate rows into SavedVariables.
- `/ahexport enrich` — Try to fill missing item names/links/quality from local item cache (rate-limited; may take a few seconds).
- `/ahexport clear` — Clear stored scan data.

## Batching (responsiveness)

Unfiltered replicate exports can be very large. To keep the WoW client responsive:

- `/ahexport read` and `/ahexport enrich` run in small batches across multiple frames.
- Progress prints to chat at most once every 5 seconds while a job is running.

Current defaults:

- `batchSize = 200` items/rows per tick
- `budgetMs = 8` ms of work per tick

Note: batching prevents long frame stalls, but the resulting SavedVariables (and CSV text) can still be large. Use `/ahexport clear` when you’re done exporting.
