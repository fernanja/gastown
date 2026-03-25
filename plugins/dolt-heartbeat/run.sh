#!/usr/bin/env bash
# dolt-heartbeat/run.sh — Verifies Dolt persistence is working.
#
# Checks:
#   1. Dolt server is reachable (SELECT 1)
#   2. Can write a canary row (INSERT + DOLT_COMMIT)
#   3. Can read back the canary (SELECT verification)
#   4. Commit gap is < 1 hour (no silent persistence failure)
#   5. Cleans up canary after verification
#
# On ANY failure: escalates immediately via gt escalate.
# This is the ONLY defense against silent Dolt outages.
#
# 2026-03-24: Created after 43-hour Dolt commit gap lost all March 23 data.

set -euo pipefail

DOLT_HOST="${GT_DOLT_HOST:-${DOLT_HOST:-127.0.0.1}}"
DOLT_PORT="${BEADS_DOLT_PORT:-${DOLT_PORT:-3307}}"
DOLT_USER="${DOLT_USER:-root}"
TOWN_ROOT="${HOME}/gt"
HEARTBEAT_DB="hq"
MAX_GAP_SECONDS=3600  # 1 hour — escalate if last commit older than this

# State file for tracking consecutive failures
STATE_FILE="${TOWN_ROOT}/daemon/dolt-heartbeat-state.json"

log() { echo "[dolt-heartbeat] $(date '+%Y-%m-%d %H:%M:%S') $*"; }
err() { echo "[dolt-heartbeat] $(date '+%Y-%m-%d %H:%M:%S') ERROR: $*" >&2; }

escalate() {
  local reason="$1"
  err "$reason"
  # Try to escalate — but gt escalate itself may need Dolt, so also write to town.log directly
  gt escalate "DOLT PERSISTENCE FAILURE: $reason" \
    -s CRITICAL \
    --reason "$reason" 2>/dev/null || true

  # Also append directly to town.log as a fallback (Dolt may be down)
  echo "$(date '+%Y-%m-%d %H:%M:%S') [DOLT-HEARTBEAT-CRITICAL] $reason" \
    >> "${TOWN_ROOT}/logs/town.log" 2>/dev/null || true

  # Write failure state
  echo "{\"last_failure\": \"$(date -u '+%Y-%m-%dT%H:%M:%SZ')\", \"reason\": \"$reason\"}" \
    > "$STATE_FILE" 2>/dev/null || true
}

dolt_query() {
  local db="$1"
  local query="$2"
  mysql -h "$DOLT_HOST" -P "$DOLT_PORT" -u "$DOLT_USER" -N -B "$db" -e "$query" 2>&1
}

# -------------------------------------------------------------------
# Check 1: Dolt server reachable
# -------------------------------------------------------------------
log "Check 1: Dolt server reachable..."
if ! RESULT=$(dolt_query "$HEARTBEAT_DB" "SELECT 1" 2>&1); then
  escalate "Dolt server UNREACHABLE on ${DOLT_HOST}:${DOLT_PORT}. All persistence is down. Result: ${RESULT}"
  exit 1
fi
log "  OK: Server responded"

# -------------------------------------------------------------------
# Check 2: Commit gap — how old is the last commit?
# -------------------------------------------------------------------
log "Check 2: Commit gap freshness..."
LAST_COMMIT_DATE=$(dolt_query "$HEARTBEAT_DB" \
  "SELECT date FROM dolt_log ORDER BY date DESC LIMIT 1" 2>&1)

if [ -z "$LAST_COMMIT_DATE" ]; then
  escalate "Cannot read dolt_log — Dolt metadata may be corrupt"
  exit 1
fi

# Parse the date and compute gap
LAST_EPOCH=$(date -d "$LAST_COMMIT_DATE" +%s 2>/dev/null || echo "0")
NOW_EPOCH=$(date +%s)

if [ "$LAST_EPOCH" -eq 0 ]; then
  log "  WARN: Could not parse last commit date: $LAST_COMMIT_DATE"
else
  GAP_SECONDS=$((NOW_EPOCH - LAST_EPOCH))
  GAP_MINUTES=$((GAP_SECONDS / 60))
  log "  Last commit: $LAST_COMMIT_DATE ($GAP_MINUTES min ago)"

  if [ "$GAP_SECONDS" -gt "$MAX_GAP_SECONDS" ]; then
    escalate "Dolt commit gap: ${GAP_MINUTES} minutes since last commit (threshold: $((MAX_GAP_SECONDS/60))m). Last commit: ${LAST_COMMIT_DATE}. Data written since then may be in volatile working set only — WSL restart will lose it."
    # Don't exit — continue with write test to see if we can still write
  fi
fi

# -------------------------------------------------------------------
# Check 3: Write test — canary INSERT + DOLT_COMMIT
# -------------------------------------------------------------------
log "Check 3: Write test (canary commit)..."
CANARY_ID="hq-heartbeat-canary"
CANARY_TITLE="dolt-heartbeat canary $(date -u '+%Y-%m-%dT%H:%M:%SZ')"

# Try to write a canary issue
WRITE_RESULT=$(dolt_query "$HEARTBEAT_DB" "
  INSERT INTO issues (id, title, description, status, priority, issue_type, ephemeral)
  VALUES ('${CANARY_ID}', '${CANARY_TITLE}', 'Heartbeat canary — auto-deleted', 'closed', 4, 'chore', 1)
  ON DUPLICATE KEY UPDATE title='${CANARY_TITLE}', updated_at=NOW();
" 2>&1)

if echo "$WRITE_RESULT" | grep -qi "error\|denied\|refused\|cannot"; then
  escalate "Dolt WRITE FAILED: Cannot insert canary row. Result: ${WRITE_RESULT}. All bd create/update/mail operations are silently failing."
  exit 1
fi
log "  OK: Canary written to working set"

# -------------------------------------------------------------------
# Check 4: DOLT_COMMIT — verify commit actually works
# -------------------------------------------------------------------
log "Check 4: DOLT_COMMIT test..."
COMMIT_RESULT=$(dolt_query "$HEARTBEAT_DB" \
  "CALL DOLT_COMMIT('-Am', 'dolt-heartbeat: canary commit')" 2>&1)

if echo "$COMMIT_RESULT" | grep -qi "error\|nothing to commit"; then
  # "nothing to commit" means the canary was already there with same data — that's actually OK
  if echo "$COMMIT_RESULT" | grep -qi "nothing to commit"; then
    log "  OK: Nothing to commit (canary unchanged)"
  else
    escalate "DOLT_COMMIT FAILED: ${COMMIT_RESULT}. Data is accumulating in working set but NOT being committed. WSL restart will lose ALL uncommitted data."
    exit 1
  fi
else
  log "  OK: Canary committed"
fi

# -------------------------------------------------------------------
# Check 5: Read-back verification
# -------------------------------------------------------------------
log "Check 5: Read-back verification..."
READBACK=$(dolt_query "$HEARTBEAT_DB" \
  "SELECT title FROM issues WHERE id='${CANARY_ID}'" 2>&1)

if [ -z "$READBACK" ]; then
  escalate "Dolt READ-BACK FAILED: Canary row not found after commit. Data integrity issue."
  exit 1
fi
log "  OK: Canary verified: ${READBACK}"

# -------------------------------------------------------------------
# Cleanup: Remove canary
# -------------------------------------------------------------------
log "Cleanup: Removing canary..."
dolt_query "$HEARTBEAT_DB" "DELETE FROM issues WHERE id='${CANARY_ID}'" >/dev/null 2>&1 || true
dolt_query "$HEARTBEAT_DB" "CALL DOLT_COMMIT('-Am', 'dolt-heartbeat: cleanup canary')" >/dev/null 2>&1 || true

# -------------------------------------------------------------------
# Success
# -------------------------------------------------------------------
log "ALL CHECKS PASSED: Dolt persistence verified"

# Clear failure state on success
rm -f "$STATE_FILE" 2>/dev/null || true

# Record success
bd create "dolt-heartbeat: OK (gap=${GAP_MINUTES:-?}m)" -t chore --ephemeral \
  -l type:plugin-run,plugin:dolt-heartbeat,result:success \
  -d "All 5 checks passed. Last commit gap: ${GAP_MINUTES:-unknown} minutes." \
  --silent 2>/dev/null || true

echo "=== dolt-heartbeat: PASS ==="
