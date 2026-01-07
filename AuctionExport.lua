---@diagnostic disable: undefined-global

AuctionExportDB = AuctionExportDB or {}

local ADDON = "AuctionExport"
local f = CreateFrame("Frame")

-- This file implements a single-command export pipeline:
-- replicate scan -> read replicate rows -> enrich missing item info (optional/automatic).

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
  -- How many distinct item IDs to request per enrich tick (default: 1000/sec).
  if settings.itemRequestsPerTick == nil then settings.itemRequestsPerTick = 1000 end
  if settings.enrichOverallMaxRequests == nil then settings.enrichOverallMaxRequests = 20000 end
  -- Whether to automatically enrich after a read.
  if settings.autoEnrich == nil then settings.autoEnrich = true end
  -- Delay before the final cache-only enrich pass (helps catch late item cache fills).
  if settings.finalEnrichDelaySec == nil then settings.finalEnrichDelaySec = 1 end
  return settings
end

getSettings() -- ensure defaults exist

local PROGRESS_EVERY_SECONDS = 5

-- Replicate scan throttle window (Blizzard-side). We can't know for sure whether a request will be accepted,
-- but we can warn when the last successful scan was recent.
local REPLICATE_THROTTLE_WINDOW_SEC = 15 * 60
local RECENT_SCAN_POPUP_KEY = "AUCTIONEXPORT_RECENT_SCAN_CONFIRM"

-- Active job state machine.
-- Kinds:
--   pipeline: scan(wait)->read->enrich
--   read: read replicate rows
--   enrich: enrich missing rows
local activeJob

local function isActiveJob(kind)
  return activeJob and activeJob.kind == kind
end

local scanProgress = {
  inFlight = false,
  requestedAt = 0,
  lastPrintAt = 0,
  throttleSeen = false,
}

local SCAN_READY_POLL_SECONDS = 2
local function tickScanReadyPoll()
  -- Some clients/throttle states may not deliver REPLICATE_ITEM_LIST_UPDATE promptly.
  -- If replicate data is already present, allow the pipeline to continue.
  if not isActiveJob("pipeline") then return end
  if not scanProgress.inFlight then return end

  local n = 0
  if C_AuctionHouse and C_AuctionHouse.GetNumReplicateItems then
    n = C_AuctionHouse.GetNumReplicateItems() or 0
  end

  if n and n > 0 then
    -- We'll let the normal event handler handle this too; but if it never comes,
    -- this ensures the user isn't stuck.
    local job = activeJob
    if job and job.stage == "waiting_scan" then
      job.scanReadyViaPoll = true
      -- Simulate readiness; the handler will start reading.
      f:GetScript("OnEvent")(f, "REPLICATE_ITEM_LIST_UPDATE")
      return
    end
  end

  C_Timer.After(SCAN_READY_POLL_SECONDS, tickScanReadyPoll)
end

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
  C_Timer.After(SCAN_READY_POLL_SECONDS, tickScanReadyPoll)
end

local function now()
  return date("!%Y-%m-%dT%H:%M:%SZ") -- UTC ISO-ish
end

local function getLastSuccessfulReplicateAt()
  local t = AuctionExportDB and AuctionExportDB.lastSuccessfulReplicateAt
  if type(t) ~= "number" then return 0 end
  return t
end

local function secondsUntilReplicateWindowExpires()
  local last = getLastSuccessfulReplicateAt()
  if not last or last <= 0 then return 0 end
  local elapsed = time() - last
  local remaining = REPLICATE_THROTTLE_WINDOW_SEC - elapsed
  if remaining > 0 then return remaining end
  return 0
end

local function formatRemainingMmSs(sec)
  if not sec or sec <= 0 then return "0:00" end
  local m = math.floor(sec / 60)
  local s = math.floor(sec % 60)
  return string.format("%d:%02d", m, s)
end

local function ensureRecentScanPopupDefined()
  if StaticPopupDialogs and StaticPopupDialogs[RECENT_SCAN_POPUP_KEY] then
    return
  end
  if not StaticPopupDialogs then
    return
  end

  StaticPopupDialogs[RECENT_SCAN_POPUP_KEY] = {
    text = ADDON .. ": Last successful scan was recent. Blizzard throttles replicate scans (~15 minutes).\n\nStill try anyway?",
    button1 = "Still try",
    button2 = "Ok I'll wait",
    timeout = 0,
    whileDead = true,
    hideOnEscape = true,
    preferredIndex = 3,
    OnShow = function(self)
      -- Live countdown: update the popup text every second while it is visible.
      local function setText()
        local remaining = secondsUntilReplicateWindowExpires()
        local msg
        if remaining > 0 then
          msg = ADDON .. ": Last successful scan was recent. Blizzard throttles replicate scans (~15 minutes).\n"
            .. "Wait time remaining: " .. formatRemainingMmSs(remaining) .. "\n\nStill try anyway?"
        else
          msg = ADDON .. ": Last successful scan is older than ~15 minutes.\n\nTry a scan now?"
        end

        local textRegion = self.text
        if not textRegion and self.GetName then
          local name = self:GetName()
          if name and _G[name .. "Text"] then
            textRegion = _G[name .. "Text"]
          end
        end
        if textRegion and textRegion.SetText then
          textRegion:SetText(msg)
        end
      end

      setText()
      if self._auctionExportTicker and self._auctionExportTicker.Cancel then
        self._auctionExportTicker:Cancel()
      end
      if C_Timer and C_Timer.NewTicker then
        self._auctionExportTicker = C_Timer.NewTicker(1, function()
          if self and self.IsShown and self:IsShown() then
            setText()
          end
        end)
      end
    end,
    OnHide = function(self)
      if self._auctionExportTicker and self._auctionExportTicker.Cancel then
        self._auctionExportTicker:Cancel()
      end
      self._auctionExportTicker = nil
    end,
    OnAccept = function(_self, data)
      if data and type(data.onProceed) == "function" then
        data.onProceed(true)
      end
    end,
    OnCancel = function(_self, data)
      if data and type(data.onProceed) == "function" then
        data.onProceed(false)
      end
    end,
  }
end

local function confirmRecentScanAndMaybeProceed(onProceed)
  if type(onProceed) ~= "function" then return end
  local remaining = secondsUntilReplicateWindowExpires()
  if remaining > 0 then
    ensureRecentScanPopupDefined()
    if StaticPopup_Show then
      StaticPopup_Show(RECENT_SCAN_POPUP_KEY, nil, nil, { onProceed = function(accepted)
        if accepted then
          onProceed()
        end
      end })
      return
    end
  end
  onProceed()
end

local function getAHParentFrame()
  -- Retail uses AuctionHouseFrame; Classic-era UI uses AuctionFrame.
  if AuctionHouseFrame then return AuctionHouseFrame end
  if AuctionFrame then return AuctionFrame end
  return nil
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

local function cancelActiveJob(reason)
  if not activeJob then return end

  local kind = activeJob.kind
  activeJob = nil
  stopScanProgress()
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

  if job.kind == "enrich" then
    local queued = 0
    if job.loadedQueue and job.loadedQueueHead then
      queued = #job.loadedQueue - job.loadedQueueHead + 1
      if queued < 0 then queued = 0 end
    end
    local runtimeSec = 0
    if job.startedAt then
      runtimeSec = math.floor(GetTime() - job.startedAt)
    end

    local pending = job.pendingCount or 0
    local overallRequested = job.overallRequested or 0
    local overallMax = job.overallMaxRequests or 0
    local rowsUpdated = job.rowsUpdated or 0
    local itemsTotal = job.total or 0

    local msg = ": Enriching item info... items " .. done .. "/" .. itemsTotal .. " (" .. pct .. "%)"
      .. "; pending " .. pending
      .. "; loadQ " .. queued
      .. "; requested " .. overallRequested .. "/" .. overallMax
      .. "; updatedRows " .. rowsUpdated
      .. "; runtime " .. runtimeSec .. "s/" .. math.floor((job.maxRuntimeSec or 0)) .. "s"
    print(ADDON .. msg)
    return
  end

  if job.kind == "pipeline" then
    local stage = job.stage or "?"
    local runtimeSec = 0
    if job.startedAt then
      runtimeSec = math.floor(GetTime() - job.startedAt)
    end
    print(ADDON .. ": Export pipeline [" .. stage .. "] runtime " .. runtimeSec .. "s")
    return
  end

  local label = job.kind == "read" and "Reading" or "Working"
  print(ADDON .. ": " .. label .. "... " .. done .. "/" .. total .. " (" .. pct .. "%)")
end

local function tickEnrichJob()
  local job = activeJob
  if not job or job.kind ~= "enrich" then return end

  local runtime = GetTime() - job.startedAt
  if runtime >= job.maxRuntimeSec then
    local scan = AuctionExportDB.lastScan
    if scan and scan.rows == job.rows then
      scan.enrichedAtUtc = now()
      scan.enrichStats = {
        rowsUpdated = job.rowsUpdated,
        overallItemRequests = job.overallRequested,
        runtimeSec = math.floor(runtime),
        timedOut = true,
        missingRowsNoItemId = job.missingRowsNoItemId,
        remainingDistinctItems = job.pendingCount + ((job.total or 0) - (job.done or 0)),
      }
    end

    local onDone = job.onDone
    activeJob = nil
    print(ADDON .. ": Item data enrichment stopped (hit max runtime; updated " .. job.rowsUpdated .. " rows; requested " .. job.overallRequested .. " items)")
    if onDone then onDone(false) end
    return
  end

  local t0 = debugprofilestop()
  local settings = getSettings()
  local requestedThisTick = 0
  local processedLoaded = 0

  -- Consume loaded queue and update rows.
  while job.loadedQueueHead <= #job.loadedQueue do
    if processedLoaded >= job.batchSize then break end
    if (debugprofilestop() - t0) >= job.budgetMs then break end

    local itemId = job.loadedQueue[job.loadedQueueHead]
    job.loadedQueueHead = job.loadedQueueHead + 1

    -- Stop tracking pending first to avoid re-queuing.
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
      -- Keep mapping; a later load event might arrive even if the first was not enough.
      -- But if rows no longer need enrichment, we can drop it.
      local stillNeeded = false
      for j = 1, #indices do
        local idx = indices[j]
        local r = job.rows[idx]
        if r and rowNeedsEnrich(r) then
          stillNeeded = true
          break
        end
      end
      if not stillNeeded then
        job.itemToRows[itemId] = nil
      end
    end

    processedLoaded = processedLoaded + 1
  end

  -- Request item data for distinct itemIds (rate-limited).
  while job.queueHead <= #job.queue do
    if requestedThisTick >= job.itemRequestsPerTick then break end
    if job.overallRequested >= job.overallMaxRequests then break end
    if (debugprofilestop() - t0) >= job.budgetMs then break end

    local itemId = job.queue[job.queueHead]
    job.queueHead = job.queueHead + 1
    job.done = job.queueHead - 1

    -- If everything for this itemId is already filled, skip requesting.
    local indices = job.itemToRows[itemId]
    if indices then
      -- Try local cache first (cheap).
      local anyChanged = false
      for j = 1, #indices do
        local idx = indices[j]
        local r = job.rows[idx]
        if r and rowNeedsEnrich(r) then
          if enrichRowFromItemId(r) then
            anyChanged = true
            job.rowsUpdated = job.rowsUpdated + 1
          end
        end
      end

      -- If still missing, request load.
      local stillMissing = false
      for j = 1, #indices do
        local idx = indices[j]
        local r = job.rows[idx]
        if r and rowNeedsEnrich(r) then
          stillMissing = true
          break
        end
      end

      if not stillMissing then
        job.itemToRows[itemId] = nil
      else
        if not job.requestedEver[itemId]
          and C_Item
          and C_Item.RequestLoadItemDataByID
        then
          job.requestedEver[itemId] = true
          job.pending[itemId] = true
          job.pendingCount = job.pendingCount + 1
          job.overallRequested = job.overallRequested + 1
          requestedThisTick = requestedThisTick + 1
          C_Item.RequestLoadItemDataByID(itemId)
        end
      end
    end
  end

  maybePrintProgress(job, false)

  local allQueuedProcessed = job.queueHead > #job.queue
  local nothingPending = job.pendingCount <= 0

  if allQueuedProcessed and nothingPending then
    -- One final best-effort pass for any items that got cached without events.
    local remaining = 0
    for itemId, indices in pairs(job.itemToRows) do
      local stillMissing = false
      for j = 1, #indices do
        local idx = indices[j]
        local r = job.rows[idx]
        if r and rowNeedsEnrich(r) then
          if enrichRowFromItemId(r) then
            job.rowsUpdated = job.rowsUpdated + 1
          end
        end
      end
      for j = 1, #indices do
        local idx = indices[j]
        local r = job.rows[idx]
        if r and rowNeedsEnrich(r) then
          stillMissing = true
          break
        end
      end
      if stillMissing then
        remaining = remaining + 1
      end
    end

    local scan = AuctionExportDB.lastScan
    if scan and scan.rows == job.rows then
      scan.enrichedAtUtc = now()
      scan.enrichStats = {
        rowsUpdated = job.rowsUpdated,
        overallItemRequests = job.overallRequested,
        runtimeSec = math.floor(runtime),
        missingRowsNoItemId = job.missingRowsNoItemId,
        remainingDistinctItems = remaining,
        hitOverallRequestCap = (job.overallRequested >= job.overallMaxRequests) or nil,
      }
    end

    local onDone = job.onDone
    activeJob = nil
    local msg = ": Item data enrichment complete (updated " .. job.rowsUpdated .. " rows; requested " .. job.overallRequested .. " items"
    if job.overallRequested >= job.overallMaxRequests then msg = msg .. "; hit overall request cap" end
    if job.missingRowsNoItemId and job.missingRowsNoItemId > 0 then msg = msg .. "; rows missing itemId seen" end
    if remaining > 0 then msg = msg .. "; still-missing items " .. remaining end
    msg = msg .. ")"
    print(ADDON .. msg)
    if onDone then onDone(true) end
    return
  end

  local tickSec = settings.enrichTickSec or 1
  if tickSec < 0.1 then tickSec = 0.1 end
  C_Timer.After(tickSec, tickEnrichJob)
end

local function startEnrichJob(rows, onDone)
  if not rows or #rows == 0 then
    if onDone then onDone(true) end
    return
  end

  if activeJob then
    cancelActiveJob("starting enrich")
  end

  local settings = getSettings()

  -- Precompute distinct itemIds needing enrichment and map itemId -> row indices.
  local itemToRows = {}
  local queue = {}
  local queued = {}
  local missingRowsNoItemId = 0

  for i = 1, #rows do
    local r = rows[i]
    if r and rowMissingAnyInfo(r) then
      if not r.itemId or r.itemId <= 0 then
        missingRowsNoItemId = missingRowsNoItemId + 1
      end
    end
    if r and rowNeedsEnrich(r) then
      -- Try cache first so we don't request if already available.
      if enrichRowFromItemId(r) then
        -- We'll count updated rows during job tick processing too, but this is a cheap early win.
      end
      if rowNeedsEnrich(r) and r.itemId and r.itemId > 0 then
        local list = itemToRows[r.itemId]
        if not list then
          list = {}
          itemToRows[r.itemId] = list
        end
        list[#list + 1] = i
        if not queued[r.itemId] then
          queued[r.itemId] = true
          queue[#queue + 1] = r.itemId
        end
      end
    end
  end

  if #queue == 0 then
    -- Nothing enrichable (either everything already filled or missing itemIds).
    local scan = AuctionExportDB.lastScan
    if scan and scan.rows == rows then
      scan.enrichedAtUtc = now()
      scan.enrichStats = {
        rowsUpdated = 0,
        overallItemRequests = 0,
        runtimeSec = 0,
        missingRowsNoItemId = missingRowsNoItemId,
        remainingDistinctItems = 0,
      }
    end
    print(ADDON .. ": No enrichable rows (missing itemId rows: " .. missingRowsNoItemId .. ")")
    if onDone then onDone(true) end
    return
  end

  activeJob = {
    kind = "enrich",
    rows = rows,
    queue = queue,
    queueHead = 1,
    total = #queue,
    done = 0,
    lastProgressAt = 0,
    startedAt = GetTime(),

    -- Keep processing cheap.
    batchSize = settings.batchSize,
    budgetMs = settings.budgetMs,

    -- Request tuning.
    itemRequestsPerTick = settings.itemRequestsPerTick,
    overallRequested = 0,
    overallMaxRequests = settings.enrichOverallMaxRequests,
    maxRuntimeSec = settings.enrichMaxRuntimeSec,

    requestedEver = {},
    pending = {},
    pendingCount = 0,
    loadedQueue = {},
    loadedQueueHead = 1,
    itemToRows = itemToRows,

    rowsUpdated = 0,
    missingRowsNoItemId = missingRowsNoItemId,

    onDone = onDone,
  }

  print(ADDON .. ": Enriching item info (distinct items " .. activeJob.total .. "; " .. activeJob.itemRequestsPerTick .. "/sec; " .. activeJob.overallMaxRequests .. " overall; " .. math.floor(activeJob.maxRuntimeSec/60) .. "m max)")
  maybePrintProgress(activeJob, true)
  C_Timer.After(0, tickEnrichJob)
end

local function finalCacheOnlyEnrichPass(rows)
  if not rows or #rows == 0 then
    return { rowsUpdated = 0, remainingRowsMissing = 0, distinctItemsMissing = 0 }
  end

  local itemCache = {}
  local rowsUpdated = 0
  local remainingRowsMissing = 0
  local distinctItemsMissing = 0
  local missingItemsSeen = {}

  for i = 1, #rows do
    local r = rows[i]
    if r and rowNeedsEnrich(r) then
      local itemId = r.itemId
      if itemId and itemId > 0 then
        local cached = itemCache[itemId]
        if not cached then
          local name, link, quality = GetItemInfo(itemId)
          cached = { name = name, link = link, quality = quality }
          itemCache[itemId] = cached
        end

        local changed = false
        if (r.name == nil or r.name == "") and cached.name and cached.name ~= "" then
          r.name = cached.name
          changed = true
        end
        if (r.itemLink == nil or r.itemLink == "") and cached.link and cached.link ~= "" then
          r.itemLink = cached.link
          changed = true
        end
        if (r.quality == nil or r.quality == -1) and cached.quality ~= nil then
          r.quality = cached.quality
          changed = true
        end

        if changed then
          rowsUpdated = rowsUpdated + 1
        end
      end

      if rowNeedsEnrich(r) then
        remainingRowsMissing = remainingRowsMissing + 1
        if r.itemId and r.itemId > 0 and not missingItemsSeen[r.itemId] then
          missingItemsSeen[r.itemId] = true
          distinctItemsMissing = distinctItemsMissing + 1
        end
      end
    end
  end

  return {
    rowsUpdated = rowsUpdated,
    remainingRowsMissing = remainingRowsMissing,
    distinctItemsMissing = distinctItemsMissing,
  }
end

local function tickReadJob()
  local job = activeJob
  if not job or job.kind ~= "read" then return end

  local t0 = debugprofilestop()
  local processed = 0

  while job.i <= job.total do
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

  job.done = job.i - 1
  maybePrintProgress(job, false)

  if job.i > job.total then
    AuctionExportDB.lastScan = {
      scannedAtUtc = job.scannedAtUtc,
      numItems = #job.rows,
      rows = job.rows,
    }

    local onDone = job.onDone
    activeJob = nil
    maybePrintProgress({ kind = "read", done = job.total, total = job.total, lastProgressAt = 0 }, true)
    print(ADDON .. ": Stored " .. #job.rows .. " rows in SavedVariables")
    if onDone then onDone(job.rows) end
    return
  end

  C_Timer.After(0, tickReadJob)
end

local function startReadJob(onDone)
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
    i = 1,
    total = n,
    done = 0,
    scannedAtUtc = now(),
    rows = {},
    batchSize = settings.batchSize,
    budgetMs = settings.budgetMs,
    lastProgressAt = 0,
    onDone = onDone,
  }

  print(ADDON .. ": Starting read job (" .. activeJob.total .. " items)")
  maybePrintProgress(activeJob, true)
  C_Timer.After(0, tickReadJob)
end

local function scanReplicate()
  if not AuctionHouseFrame or not AuctionHouseFrame:IsShown() then
    print(ADDON .. ": Open the Auction House first.")
    return false
  end

  if isAHMessageSystemThrottled() then
    print(ADDON .. ": Auction House is currently throttling requests. Replicate scan may be delayed or ignored; continuing anyway.")
  end

  print(ADDON .. ": Requesting replicate scan... (may be throttled)")
  -- Throttled ~15 minutes account-wide when successful.
  C_AuctionHouse.ReplicateItems()

  -- Print periodic status while we wait for REPLICATE_ITEM_LIST_UPDATE.
  startScanProgress()
  return true
end

local function readReplicateToDB(onDone)
  startReadJob(onDone)
end

local function startPipelineInternal()
  if activeJob then
    cancelActiveJob("starting export")
  end

  local job = {
    kind = "pipeline",
    stage = "scan",
    startedAt = GetTime(),
    scannedAtUtc = now(),
  }
  activeJob = job

  if not scanReplicate() then
    activeJob = nil
    return
  end

  job.stage = "waiting_scan"
  maybePrintProgress(job, true)
end

local function startPipeline()
  confirmRecentScanAndMaybeProceed(startPipelineInternal)
end

local function startScanOnly()
  confirmRecentScanAndMaybeProceed(function()
    scanReplicate()
  end)
end

local function stopAll(reason)
  local did = false
  if scanProgress.inFlight then
    stopScanProgress()
    did = true
  end
  if activeJob then
    cancelActiveJob(reason or "user")
    did = true
  end
  return did
end

local function ensureAHButtons()
  local parent = getAHParentFrame()
  if not parent then return end
  if parent._auctionExportButtonsCreated then return end
  parent._auctionExportButtonsCreated = true

  -- Use a tiny anchor frame so we can reliably position and layer the buttons.
  local holder = CreateFrame("Frame", nil, parent)
  holder:SetSize(1, 1)
  holder:SetPoint("BOTTOM", parent, "BOTTOM", 0, 60)
  holder:SetFrameStrata("HIGH")
  holder:SetFrameLevel((parent:GetFrameLevel() or 0) + 50)

  local stopBtn = CreateFrame("Button", nil, holder, "UIPanelButtonTemplate")
  stopBtn:SetSize(70, 22)
  stopBtn:SetText("Stop")
  stopBtn:SetPoint("CENTER", holder, "CENTER", 40, 0)
  stopBtn:SetScript("OnClick", function()
    if stopAll("user") then
      print(ADDON .. ": Canceled.")
    else
      print(ADDON .. ": Nothing to cancel.")
    end
  end)

  local startBtn = CreateFrame("Button", nil, holder, "UIPanelButtonTemplate")
  startBtn:SetSize(70, 22)
  startBtn:SetText("Start")
  startBtn:SetPoint("RIGHT", stopBtn, "LEFT", -6, 0)
  startBtn:SetScript("OnClick", function()
    startPipeline()
  end)

  -- Keep references for debugging/adjustments.
  parent._auctionExportStartBtn = startBtn
  parent._auctionExportStopBtn = stopBtn
  parent._auctionExportHolder = holder
end

local function continuePipelineAfterRead(rows)
  local job = activeJob
  if not job or job.kind ~= "pipeline" then
    return
  end

  local settings = getSettings()
  if not settings.autoEnrich then
    job.stage = "done"
    activeJob = nil
    print(ADDON .. ": Export complete (read " .. (#rows or 0) .. " rows; autoEnrich disabled)")
    return
  end

  job.stage = "enrich"
  maybePrintProgress(job, true)

  startEnrichJob(rows, function(_ok)
    -- Final phase: one last cache-only pass after a small delay.
    local settings = getSettings()
    local delay = settings.finalEnrichDelaySec or 1
    if delay < 0 then delay = 0 end

    C_Timer.After(delay, function()
      local scan = AuctionExportDB.lastScan
      local scanRows = (scan and scan.rows) or rows

      local stats = finalCacheOnlyEnrichPass(scanRows)

      if scan and scan.rows == scanRows then
        scan.finalEnrichAtUtc = now()
        scan.finalEnrichStats = {
          rowsUpdated = stats.rowsUpdated,
          remainingRowsMissing = stats.remainingRowsMissing,
          distinctItemsMissing = stats.distinctItemsMissing,
          delaySec = delay,
        }
      end

      local count = 0
      if scanRows then count = #scanRows end
      local suffix = ""
      if stats.rowsUpdated and stats.rowsUpdated > 0 then
        suffix = " (final pass updated " .. stats.rowsUpdated .. " rows)"
      end
      if stats.remainingRowsMissing and stats.remainingRowsMissing > 0 then
        suffix = suffix .. " (still missing " .. stats.remainingRowsMissing .. " rows)"
      end
      print(ADDON .. ": Export complete (" .. count .. " rows)" .. suffix)
    end)
  end)
end

local function continuePipelineAfterScanReady()
  local job = activeJob
  if not job or job.kind ~= "pipeline" then
    return
  end
  job.stage = "read"
  maybePrintProgress(job, true)

  -- Temporarily replace pipeline job with read job; pipeline continues via callback.
  readReplicateToDB(function(rows)
    -- Recreate pipeline job to keep stage/progress cohesive.
    local pj = {
      kind = "pipeline",
      stage = "post_read",
      startedAt = job.startedAt,
      scannedAtUtc = job.scannedAtUtc,
      scanReadyViaPoll = job.scanReadyViaPoll,
    }
    activeJob = pj
    continuePipelineAfterRead(rows)
  end)
end

SLASH_AUCTIONEXPORT1 = "/ahexport"
SlashCmdList["AUCTIONEXPORT"] = function(msg)
  msg = (msg or ""):lower()
  local cmd, rest = msg:match("^(%S+)%s*(.-)$")
  cmd = cmd or ""
  rest = rest or ""

  if cmd == "" or cmd == "run" or cmd == "export" then
    startPipeline()
  elseif cmd == "scan" then
    if rest == "cancel" or rest == "stop" then
      if scanProgress.inFlight then
        stopScanProgress()
        print(ADDON .. ": Scan canceled.")
      else
        print(ADDON .. ": No scan in progress.")
      end
      return
    end
    startScanOnly()
  elseif cmd == "stopscan" then
    if scanProgress.inFlight then
      stopScanProgress()
      print(ADDON .. ": Scan canceled.")
    else
      print(ADDON .. ": No scan in progress.")
    end
  elseif cmd == "read" then
    readReplicateToDB(nil)
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
    startEnrichJob(scan.rows, nil)
  elseif cmd == "cancel" or cmd == "stop" then
    if stopAll("user") then
      print(ADDON .. ": Canceled.")
    else
      print(ADDON .. ": Nothing to cancel.")
    end
  elseif cmd == "clear" then
    if activeJob then
      cancelActiveJob("clear")
    end
    AuctionExportDB.lastScan = nil
    print(ADDON .. ": Cleared.")
  else
    print(ADDON .. " commands:")
    print("  /ahexport              - scan + read + enrich missing item info")
    print("  /ahexport run          - same as /ahexport")
    print("  /ahexport scan         - request replicate scan only (AH must be open; may be throttled)")
    print("  /ahexport scan cancel  - cancel scan wait/progress timer")
    print("  /ahexport read         - read replicate rows into SavedVariables")
    print("  /ahexport enrich       - enrich missing item names/links/quality (rate-limited)")
    print("  /ahexport cancel       - cancel any active job")
    print("  /ahexport clear        - clear stored data")
  end
end

-- Event wiring: replicate list update fires when the scan result is ready.
f:RegisterEvent("REPLICATE_ITEM_LIST_UPDATE")
f:RegisterEvent("ITEM_DATA_LOAD_RESULT")
f:RegisterEvent("GET_ITEM_INFO_RECEIVED")
f:RegisterEvent("AUCTION_HOUSE_SHOW")
f:RegisterEvent("PLAYER_LOGIN")
f:SetScript("OnEvent", function(_, event, ...)
  if event == "PLAYER_LOGIN" then
    -- If the AH is already open after /reload, AUCTION_HOUSE_SHOW may not fire.
    local parent = getAHParentFrame()
    if parent and parent.IsShown and parent:IsShown() then
      ensureAHButtons()
    end
    return
  end

  if event == "AUCTION_HOUSE_SHOW" then
    ensureAHButtons()
    return
  end

  if event == "REPLICATE_ITEM_LIST_UPDATE" then
    -- This event can fire for reasons other than our scan request.
    -- Only act if we're waiting for a scan result (scanProgress) or pipeline is waiting.
    if not scanProgress.inFlight and not (isActiveJob("pipeline") and activeJob.stage == "waiting_scan") then
      return
    end

    local elapsedMsg = ""
    if scanProgress.requestedAt and scanProgress.requestedAt > 0 then
      local elapsed = math.floor(GetTime() - scanProgress.requestedAt)
      elapsedMsg = " after " .. elapsed .. "s"
    end

    local throttleMsg = ""
    if scanProgress.throttleSeen then
      throttleMsg = " (throttling was active)"
    end

    stopScanProgress()

    -- Record the last *successful* scan readiness time so we can warn about the ~15 minute throttle window.
    AuctionExportDB.lastSuccessfulReplicateAt = time()

    if isActiveJob("pipeline") and activeJob.stage == "waiting_scan" then
      local via = activeJob.scanReadyViaPoll and " (detected via poll)" or ""
      print(ADDON .. ": Replicate list ready" .. elapsedMsg .. via .. throttleMsg .. ". Reading...")
      continuePipelineAfterScanReady()
      return
    end

    print(ADDON .. ": Replicate list ready" .. elapsedMsg .. throttleMsg)
  elseif event == "ITEM_DATA_LOAD_RESULT" or event == "GET_ITEM_INFO_RECEIVED" then
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

print(ADDON .. " loaded. Open AH and run /ahexport")
