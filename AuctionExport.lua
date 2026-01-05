---@diagnostic disable: undefined-global

AuctionExportDB = AuctionExportDB or {}

local ADDON = "AuctionExport"
local f = CreateFrame("Frame")

local function getSettings()
  if type(AuctionExportDB) ~= "table" then
    AuctionExportDB = {}
  end
  if type(AuctionExportDB.settings) ~= "table" then
    AuctionExportDB.settings = {}
  end

  local settings = AuctionExportDB.settings
  if settings.batchSize == nil then settings.batchSize = 200 end
  if settings.budgetMs == nil then settings.budgetMs = 8 end
  -- Item info enrichment (used to fill in blank names/links/quality).
  -- Keep these conservative to avoid spamming item data requests.
  if settings.enrichTickSec == nil then settings.enrichTickSec = 1 end
  -- Enrichment runs for up to 30 minutes by default.
  if settings.enrichMaxRuntimeSec == nil then settings.enrichMaxRuntimeSec = 30 * 60 end
  if settings.itemRequestsPerTick == nil then settings.itemRequestsPerTick = 10 end
  if settings.enrichOverallMaxRequests == nil then settings.enrichOverallMaxRequests = 20000 end
  return settings
end

getSettings() -- ensure defaults exist

local PROGRESS_EVERY_SECONDS = 5
local activeJob

local scanProgress = {
  inFlight = false,
  requestedAt = 0,
  lastPrintAt = 0,
  throttleSeen = false,
}

local function isAHMessageSystemThrottled()
  if C_AuctionHouse and C_AuctionHouse.IsThrottledMessageSystemReady then
    return not C_AuctionHouse.IsThrottledMessageSystemReady()
  end
  return false
end

local function stopScanProgress()
  scanProgress.inFlight = false
  scanProgress.requestedAt = 0
  scanProgress.lastPrintAt = 0
  scanProgress.throttleSeen = false
end

local function tickScanProgress()
  if not scanProgress.inFlight then return end

  local t = GetTime()
  local elapsed = 0
  if scanProgress.requestedAt and scanProgress.requestedAt > 0 then
    elapsed = t - scanProgress.requestedAt
  end

  local throttled = isAHMessageSystemThrottled()
  if throttled then
    scanProgress.throttleSeen = true
  end

  if (t - (scanProgress.lastPrintAt or 0)) >= PROGRESS_EVERY_SECONDS then
    scanProgress.lastPrintAt = t
    local suffix = throttled and " (THROTTLED)" or ""
    print(ADDON .. ": Waiting for replicate scan results... " .. math.floor(elapsed) .. "s elapsed" .. suffix)
  end

  C_Timer.After(PROGRESS_EVERY_SECONDS, tickScanProgress)
end

local function startScanProgress()
  scanProgress.inFlight = true
  scanProgress.requestedAt = GetTime()
  scanProgress.lastPrintAt = 0
  scanProgress.throttleSeen = isAHMessageSystemThrottled()
  C_Timer.After(PROGRESS_EVERY_SECONDS, tickScanProgress)
end

local function now()
  return date("!%Y-%m-%dT%H:%M:%SZ") -- UTC ISO-ish
end

local function itemIdFromLink(itemLink)
  if not itemLink or itemLink == "" then return nil end
  local id = itemLink:match("Hitem:(%d+)")
  if not id then return nil end
  id = tonumber(id)
  if not id or id <= 0 then return nil end
  return id
end

local function extractItemId(info, itemLink)
  local id = itemIdFromLink(itemLink)
  if id then return id end

  -- Fallback to a few commonly-used positions in different client builds.
  -- We intentionally avoid indices that collide with count/quality/minBid/buyout.
  for _, idx in ipairs({ 17, 18, 19, 20, 2, 15, 16 }) do
    local v = info[idx]
    if type(v) == "number" and v > 0 then
      return v
    end
  end

  return nil
end

local function findHasAllInfoFlag(info)
  for i = #info, 1, -1 do
    if type(info[i]) == "boolean" then
      return info[i]
    end
  end
  return nil
end

local function rowNeedsEnrich(r)
  if not r then return false end
  if not r.itemId or r.itemId <= 0 then return false end
  if r.name == nil or r.name == "" then return true end
  if r.itemLink == nil or r.itemLink == "" then return true end
  if r.quality == nil or r.quality == -1 then return true end
  return false
end

local function rowMissingAnyInfo(r)
  if not r then return false end
  if r.name == nil or r.name == "" then return true end
  if r.itemLink == nil or r.itemLink == "" then return true end
  if r.quality == nil or r.quality == -1 then return true end
  return false
end

local function enrichRowFromItemId(r)
  if not r or not r.itemId or r.itemId <= 0 then return false end

  local name, link, quality = GetItemInfo(r.itemId)
  local changed = false

  if (r.name == nil or r.name == "") and name and name ~= "" then
    r.name = name
    changed = true
  end
  if (r.itemLink == nil or r.itemLink == "") and link and link ~= "" then
    r.itemLink = link
    changed = true
  end
  if (r.quality == nil or r.quality == -1) and quality ~= nil then
    r.quality = quality
    changed = true
  end

  return changed
end


local function scanReplicate()
  if not AuctionHouseFrame or not AuctionHouseFrame:IsShown() then
    print(ADDON .. ": Open the Auction House first.")
    return
  end

  if isAHMessageSystemThrottled() then
    print(ADDON .. ": Auction House is currently throttling requests. Replicate scan may be delayed or ignored; try again later.")
  end

  print(ADDON .. ": Requesting replicate scan... (may be throttled)")
  -- Throttled ~15 minutes account-wide when successful.
  C_AuctionHouse.ReplicateItems()

  -- Print periodic status while we wait for REPLICATE_ITEM_LIST_UPDATE.
  startScanProgress()
end

local function cancelActiveJob(reason)
  if not activeJob then return end

  local kind = activeJob.kind
  activeJob = nil
  if reason then
    print(ADDON .. ": Canceled " .. kind .. " job (" .. reason .. ")")
  else
    print(ADDON .. ": Canceled " .. kind .. " job")
  end
end

local function maybePrintProgress(job, force)
  local t = GetTime()
  if not force and job.lastProgressAt and (t - job.lastProgressAt) < PROGRESS_EVERY_SECONDS then
    return
  end

  job.lastProgressAt = t

  local done = job.done or 0
  local total = job.total or 0
  local pct = 0
  if total > 0 then
    pct = math.floor((done / total) * 100)
  end

  local label = job.kind == "read" and "Reading"
    or (job.kind == "enrich" and "Loading item data")
    or "Working"
  print(ADDON .. ": " .. label .. "... " .. done .. "/" .. total .. " (" .. pct .. "%)")
end

local function tickEnrichJob()
  local job = activeJob
  if not job or job.kind ~= "enrich" then return end

  local t0 = debugprofilestop()
  local settings = getSettings()
  local requestedThisTick = 0
  local processed = 0

  -- Consume loaded queue and update rows.
  while job.loadedQueueHead <= #job.loadedQueue do
    if processed >= job.batchSize then break end
    if (debugprofilestop() - t0) >= job.budgetMs then break end

    local itemId = job.loadedQueue[job.loadedQueueHead]
    job.loadedQueueHead = job.loadedQueueHead + 1

    if job.pending[itemId] then
      job.pending[itemId] = nil
      job.pendingCount = job.pendingCount - 1
    end

    local indices = job.itemToRows[itemId]
    if indices then
      for j = 1, #indices do
        local idx = indices[j]
        local r = job.rows[idx]
        if r and rowNeedsEnrich(r) then
          if enrichRowFromItemId(r) then
            job.rowsUpdated = job.rowsUpdated + 1
          end
        end
      end
      job.itemToRows[itemId] = nil
    end

    processed = processed + 1
  end

  -- Request item data for rows that need it (rate-limited).
  while job.i <= job.total do
    if processed >= job.batchSize then break end
    if (debugprofilestop() - t0) >= job.budgetMs then break end

    local r = job.rows[job.i]
    if r and rowMissingAnyInfo(r) then
      if not r.itemId or r.itemId <= 0 then
        job.missingNoItemIdThisPass = job.missingNoItemIdThisPass + 1
      end
    end

    if r and rowNeedsEnrich(r) then
      -- If the item is already cached locally, fill what we can without requesting.
      if enrichRowFromItemId(r) then
        job.rowsUpdated = job.rowsUpdated + 1
      end

      if not rowNeedsEnrich(r) then
        job.i = job.i + 1
        processed = processed + 1
      else
        local itemId = r.itemId
        local list = job.itemToRows[itemId]
        if not list then
          list = {}
          job.itemToRows[itemId] = list
        end
        list[#list + 1] = job.i

        if not job.requestedEver[itemId]
          and requestedThisTick < job.itemRequestsPerTick
          and job.overallRequested < job.overallMaxRequests
          and C_Item
          and C_Item.RequestLoadItemDataByID
        then
          job.requestedEver[itemId] = true
          job.pending[itemId] = true
          job.pendingCount = job.pendingCount + 1
          job.overallRequested = job.overallRequested + 1
          requestedThisTick = requestedThisTick + 1
          C_Item.RequestLoadItemDataByID(itemId)
          job.requestedThisPass = job.requestedThisPass + 1
        end

        job.rowsNeeding = job.rowsNeeding + 1
        job.i = job.i + 1
        processed = processed + 1
      end
    else
      job.i = job.i + 1
      processed = processed + 1
    end
  end

  job.done = job.i - 1
  maybePrintProgress(job, false)

  local runtime = GetTime() - job.startedAt
  local timedOut = runtime >= job.maxRuntimeSec
  local fullyScanned = job.i > job.total

  if fullyScanned then
    -- One full pass completed.
    if job.pendingCount <= 0 and job.requestedThisPass == 0 then
      -- Nothing in-flight and we didn't request anything new in a full pass.
      local scan = AuctionExportDB.lastScan
      if scan and scan.rows == job.rows then
        scan.enrichedAtUtc = now()
        scan.enrichStats = {
          rowsUpdated = job.rowsUpdated,
          overallItemRequests = job.overallRequested,
          runtimeSec = math.floor(runtime),
          missingNoItemIdThisPass = job.missingNoItemIdThisPass,
        }
      end

      activeJob = nil
      local msg = ": Item data enrichment complete (updated " .. job.rowsUpdated .. " rows; requested " .. job.overallRequested .. " items"
      if job.overallRequested >= job.overallMaxRequests then msg = msg .. "; hit overall request cap" end
      if job.missingNoItemIdThisPass > 0 then msg = msg .. "; missing itemId rows seen" end
      msg = msg .. ")"
      print(ADDON .. msg)
      return
    end

    if timedOut then
      local scan = AuctionExportDB.lastScan
      if scan and scan.rows == job.rows then
        scan.enrichedAtUtc = now()
        scan.enrichStats = {
          rowsUpdated = job.rowsUpdated,
          overallItemRequests = job.overallRequested,
          runtimeSec = math.floor(runtime),
          timedOut = true,
          missingNoItemIdThisPass = job.missingNoItemIdThisPass,
        }
      end

      activeJob = nil
      print(ADDON .. ": Item data enrichment stopped (hit max runtime; updated " .. job.rowsUpdated .. " rows; requested " .. job.overallRequested .. " items)")
      return
    end

    -- Reset pass counters and wrap.
    job.i = 1
    job.requestedThisPass = 0
    job.missingNoItemIdThisPass = 0
    job.itemToRows = {}
  end

  -- Keep trying once per second.
  local tickSec = settings.enrichTickSec or 1
  if tickSec < 0.1 then tickSec = 0.1 end
  C_Timer.After(tickSec, tickEnrichJob)
end

local function startEnrichJob(rows, nextKind, nextFn)
  if not rows or #rows == 0 then
    if nextFn then nextFn() end
    return
  end

  if activeJob then
    cancelActiveJob("starting enrich")
  end

  local settings = getSettings()
  activeJob = {
    kind = "enrich",
    rows = rows,
    i = 1,
    total = #rows,
    done = 0,
    batchSize = settings.batchSize,
    budgetMs = settings.budgetMs,
    lastProgressAt = 0,

    itemRequestsPerTick = settings.itemRequestsPerTick,
    overallRequested = 0,
    overallMaxRequests = settings.enrichOverallMaxRequests,
    maxRuntimeSec = settings.enrichMaxRuntimeSec,

    requestedEver = {},
    pending = {},
    pendingCount = 0,
    loadedQueue = {},
    loadedQueueHead = 1,
    itemToRows = {},

    startedAt = GetTime(),
    rowsNeeding = 0,
    rowsUpdated = 0,

    requestedThisPass = 0,
    missingNoItemIdThisPass = 0,

    nextKind = nextKind,
    nextFn = nextFn,
  }

  print(ADDON .. ": Enriching item info (" .. activeJob.itemRequestsPerTick .. "/sec, " .. activeJob.overallMaxRequests .. " overall; " .. math.floor(activeJob.maxRuntimeSec/60) .. "m max)")
  maybePrintProgress(activeJob, true)
  C_Timer.After(0, tickEnrichJob)
end

local function tickReadJob()
  local job = activeJob
  if not job or job.kind ~= "read" then return end

  local t0 = debugprofilestop()
  local processed = 0

  while job.i < job.total do
    if processed >= job.batchSize then break end
    if (debugprofilestop() - t0) >= job.budgetMs then break end

    local i = job.i
    local info = { C_AuctionHouse.GetReplicateItemInfo(i) }
    local name        = info[1]
    local count       = info[3]
    local quality     = info[4]
    local minBid      = info[8]
    local buyout      = info[10]

    local itemLink = C_AuctionHouse.GetReplicateItemLink(i)
    local timeLeft = C_AuctionHouse.GetReplicateItemTimeLeft(i)

    local itemId = extractItemId(info, itemLink)
    local hasAllInfo = findHasAllInfoFlag(info)

    job.rows[#job.rows+1] = {
      scannedAtUtc = job.scannedAtUtc,
      index = i,
      itemId = itemId,
      itemLink = itemLink,
      name = name,
      count = count,
      quality = quality,
      minBidCopper = minBid,
      buyoutCopper = buyout,
      timeLeft = timeLeft,
      hasAllInfo = hasAllInfo,
    }

    job.i = i + 1
    processed = processed + 1
  end

  job.done = job.i
  maybePrintProgress(job, false)

  if job.i >= job.total then
    AuctionExportDB.lastScan = {
      scannedAtUtc = job.scannedAtUtc,
      numItems = #job.rows,
      rows = job.rows,
    }
    activeJob = nil
    maybePrintProgress({ kind = "read", done = job.total, total = job.total, lastProgressAt = 0 }, true)
    print(ADDON .. ": Stored " .. #job.rows .. " rows in SavedVariables. Use /ahexport enrich (optional)")
    return
  end

  C_Timer.After(0, tickReadJob)
end

local function startReadJob()
  local n = C_AuctionHouse.GetNumReplicateItems()
  if not n or n == 0 then
    print(ADDON .. ": No replicate items available yet.")
    return
  end

  if activeJob then
    cancelActiveJob("starting read")
  end

  local settings = getSettings()
  activeJob = {
    kind = "read",
    i = 0,
    total = n,
    done = 0,
    scannedAtUtc = now(),
    rows = {},
    batchSize = settings.batchSize,
    budgetMs = settings.budgetMs,
    lastProgressAt = 0,
  }

  print(ADDON .. ": Starting read job (" .. activeJob.total .. " items)")
  maybePrintProgress(activeJob, true)
  C_Timer.After(0, tickReadJob)
end

local function readReplicateToDB()
  startReadJob()
end

SLASH_AUCTIONEXPORT1 = "/ahexport"
SlashCmdList["AUCTIONEXPORT"] = function(msg)
  msg = (msg or ""):lower()
  local cmd, rest = msg:match("^(%S+)%s*(.-)$")
  cmd = cmd or ""
  rest = rest or ""

  if cmd == "scan" then
    if rest == "cancel" or rest == "stop" then
      if scanProgress.inFlight then
        stopScanProgress()
        print(ADDON .. ": Scan canceled.")
      else
        print(ADDON .. ": No scan in progress.")
      end
      return
    end
    scanReplicate()
  elseif cmd == "stopscan" then
    if scanProgress.inFlight then
      stopScanProgress()
      print(ADDON .. ": Scan canceled.")
    else
      print(ADDON .. ": No scan in progress.")
    end
  elseif cmd == "read" then
    readReplicateToDB()
  elseif cmd == "enrich" then
    local scan = AuctionExportDB.lastScan
    if not scan or not scan.rows then
      print(ADDON .. ": No scan data. Use /ahexport read first.")
      return
    end
    if not (C_Item and C_Item.RequestLoadItemDataByID) then
      print(ADDON .. ": Item data loading API not available on this client.")
      return
    end

    startEnrichJob(scan.rows, nil, nil)
  elseif cmd == "clear" then
    if activeJob then
      cancelActiveJob("clear")
    end
    AuctionExportDB.lastScan = nil
    print(ADDON .. ": Cleared.")
  else
    print(ADDON .. " commands:")
    print("  /ahexport scan         - request replicate scan (AH must be open; may be throttled)")
    print("  /ahexport scan cancel  - cancel scan wait/progress timer")
    print("  /ahexport stopscan     - cancel scan wait/progress timer")
    print("  /ahexport read   - read replicate rows into SavedVariables")
    print("  /ahexport enrich       - try to fill missing item names/links locally (rate-limited)")
    print("  /ahexport clear  - clear stored data")
  end
end

-- Event wiring: replicate list update fires when the scan result is ready.
f:RegisterEvent("REPLICATE_ITEM_LIST_UPDATE")
f:RegisterEvent("ITEM_DATA_LOAD_RESULT")
f:SetScript("OnEvent", function(_, event, ...)
  if event == "REPLICATE_ITEM_LIST_UPDATE" then
    -- This event can fire for reasons other than our /ahexport scan.
    -- Only announce readiness when we are actively waiting for a scan result.
    if not scanProgress.inFlight then
      return
    end

    local elapsedMsg = ""
    if scanProgress.inFlight and scanProgress.requestedAt and scanProgress.requestedAt > 0 then
      local elapsed = math.floor(GetTime() - scanProgress.requestedAt)
      elapsedMsg = " after " .. elapsed .. "s"
    end

    local throttleMsg = ""
    if scanProgress.inFlight and scanProgress.throttleSeen then
      throttleMsg = " (throttling was active)"
    end

    stopScanProgress()
    print(ADDON .. ": Replicate list ready" .. elapsedMsg .. ". Now run /ahexport read" .. throttleMsg)
  elseif event == "ITEM_DATA_LOAD_RESULT" then
    local job = activeJob
    if not job or job.kind ~= "enrich" then
      return
    end

    local itemId, success = ...
    itemId = tonumber(itemId)
    if not itemId or itemId <= 0 then
      return
    end

    -- Only queue items we are still tracking as pending.
    if job.pending[itemId] then
      -- We update rows on the next enrich tick; keep the handler cheap.
      job.loadedQueue[#job.loadedQueue + 1] = itemId
      job.loadedSuccess = job.loadedSuccess or {}
      job.loadedSuccess[itemId] = (success == true)
    end
  end
end)

print(ADDON .. " loaded. Open AH and run /ahexport scan")
