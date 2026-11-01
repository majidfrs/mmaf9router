import fs from "node:fs";
import os from "node:os";
import path from "node:path";

const DEFAULT_DEBOUNCE_MS = 1200;
const TELEMETRY_SYNC_INTERVAL_MS = 15_000;
const MAX_RETRY_MS = 60_000;
const MAX_SNAPSHOT_BYTES = 100 * 1024 * 1024;

const CRITICAL_TABLES = /\b(settings|providerConnections|providerNodes|proxyPools|apiKeys|combos|kv)\b/i;
const WRITE_SQL = /^\s*(?:INSERT|UPDATE|DELETE|REPLACE|CREATE|ALTER|DROP|REINDEX|VACUUM|ATTACH|DETACH)\b/i;

let activeCloudSync = null;

export async function flushCloudSync() {
  if (!activeCloudSync) return;
  await activeCloudSync.flush();
}

function isSqlite(bytes) {
  return bytes.length >= 100 &&
    bytes.subarray(0, 15).toString("utf8") === "SQLite format 3";
}

function createSnapshot(adapter) {
  if (adapter.driver === "sql.js") {
    const bytes = Buffer.from(adapter.raw.export());
    if (!isSqlite(bytes)) throw new Error("SQL.js produced an invalid SQLite snapshot");
    return bytes;
  }

  const suffix = Math.random().toString(36).slice(2, 10);
  const snapshotPath = path.join(os.tmpdir(), "9router-db-" + process.pid + "-" + Date.now() + "-" + suffix + ".sqlite");
  const escapedPath = snapshotPath.replace(/'/g, "''");

  try {
    // VACUUM INTO creates a transactionally consistent snapshot of a live SQLite DB,
    // including committed WAL content, without copying an active WAL file.
    adapter.raw.exec("VACUUM INTO '" + escapedPath + "'");
    const bytes = fs.readFileSync(snapshotPath);
    if (!isSqlite(bytes)) throw new Error("SQLite produced an invalid cloud snapshot");
    return bytes;
  } finally {
    try { fs.rmSync(snapshotPath, { force: true }); } catch {}
  }
}

export function startCloudSync(adapter) {
  const url = process.env.NINEROUTER_DB_SYNC_URL || process.env.NINEROUTER_DB_DOWNLOAD_URL;
  const token = process.env.NINEROUTER_DB_DOWNLOAD_TOKEN;
  if (!url || !token) {
    console.warn("[DB sync] Disabled: protected Supabase sync URL/token is not configured");
    return;
  }

  if (globalThis._ninerouterCloudSyncStarted) return;
  globalThis._ninerouterCloudSyncStarted = true;

  let dirtyVersion = 0;
  let uploadedVersion = 0;
  let criticalVersion = 0;
  let uploadedCriticalVersion = 0;
  let timer = null;
  let inFlight = null;
  let retryCount = 0;
  let lastSuccessfulUploadAt = 0;

  function schedule(delayMs) {
    if (timer) {
      if (delayMs > DEFAULT_DEBOUNCE_MS) return;
      clearTimeout(timer);
    }

    timer = setTimeout(() => {
      timer = null;
      syncOnce().catch(() => {});
    }, delayMs);
    timer.unref?.();
  }

  function markDirty(critical = false) {
    dirtyVersion += 1;
    if (critical) criticalVersion += 1;

    const urgent = criticalVersion > uploadedCriticalVersion;
    if (urgent) {
      schedule(DEFAULT_DEBOUNCE_MS);
      return;
    }

    const sinceLastUpload = Date.now() - lastSuccessfulUploadAt;
    const delay = lastSuccessfulUploadAt === 0
      ? TELEMETRY_SYNC_INTERVAL_MS
      : Math.max(1000, TELEMETRY_SYNC_INTERVAL_MS - sinceLastUpload);
    schedule(delay);
  }

  async function syncOnce() {
    if (inFlight) return inFlight;
    if (dirtyVersion <= uploadedVersion) return;

    inFlight = (async () => {
      const snapshotVersion = dirtyVersion;
      const snapshotCriticalVersion = criticalVersion;
      const bytes = createSnapshot(adapter);

      if (bytes.length > MAX_SNAPSHOT_BYTES) {
        throw new Error("SQLite snapshot exceeds the 100 MB safety limit");
      }

      const response = await fetch(url, {
        method: "POST",
        headers: {
          "Content-Type": "application/octet-stream",
          "x-9router-download-token": token,
        },
        body: bytes,
        signal: AbortSignal.timeout(60_000),
        cache: "no-store",
      });

      if (!response.ok) {
        throw new Error("Supabase Storage upload returned HTTP " + response.status);
      }

      uploadedVersion = snapshotVersion;
      uploadedCriticalVersion = snapshotCriticalVersion;
      lastSuccessfulUploadAt = Date.now();
      retryCount = 0;
      console.log("[DB sync] Supabase snapshot saved (" + bytes.length + " bytes)");

      if (dirtyVersion > uploadedVersion) {
        const urgent = criticalVersion > uploadedCriticalVersion;
        schedule(urgent ? DEFAULT_DEBOUNCE_MS : TELEMETRY_SYNC_INTERVAL_MS);
      }
    })().catch((error) => {
      retryCount += 1;
      const delay = Math.min(MAX_RETRY_MS, 2000 * (2 ** Math.min(retryCount - 1, 5)));
      console.error("[DB sync] Snapshot upload failed; retrying in " + delay + "ms:", error.message);
      schedule(delay);
    }).finally(() => {
      inFlight = null;
    });

    return inFlight;
  }

  const originalRun = adapter.run.bind(adapter);
  adapter.run = (sql, params = []) => {
    const result = originalRun(sql, params);
    markDirty(CRITICAL_TABLES.test(String(sql)));
    return result;
  };

  const originalExec = adapter.exec.bind(adapter);
  adapter.exec = (sql) => {
    const result = originalExec(sql);
    if (WRITE_SQL.test(String(sql))) {
      markDirty(CRITICAL_TABLES.test(String(sql)));
    }
    return result;
  };

  // The repos perform their writes through adapter.run/exec inside transactions.
  // Their wrapper callbacks are synchronous, so the debounce timer runs only
  // after the transaction has committed.

  activeCloudSync = {
    async flush() {
      // Try up to three immediate snapshots so writes that completed during an
      // in-flight upload are not skipped during a graceful Render shutdown.
      for (let attempt = 0; attempt < 3 && dirtyVersion > uploadedVersion; attempt += 1) {
        if (timer) {
          clearTimeout(timer);
          timer = null;
        }
        await syncOnce();
      }

      if (dirtyVersion > uploadedVersion) {
        console.error("[DB sync] Final shutdown snapshot remains pending after 3 attempts");
      }
    },
  };

  console.log("[DB sync] Supabase persistence enabled; configuration writes sync promptly and telemetry is batched");
  markDirty(true); // Persist any migrations/schema updates completed during startup.
}
