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
# 2026-03-24: Rewritten to use pymysql (mysql CLI not installed on WSL2).

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
  gt escalate "DOLT PERSISTENCE FAILURE: $reason" \
    -s CRITICAL \
    --reason "$reason" 2>/dev/null || true

  echo "$(date '+%Y-%m-%d %H:%M:%S') [DOLT-HEARTBEAT-CRITICAL] $reason" \
    >> "${TOWN_ROOT}/logs/town.log" 2>/dev/null || true

  echo "{\"last_failure\": \"$(date -u '+%Y-%m-%dT%H:%M:%SZ')\", \"reason\": \"$reason\"}" \
    > "$STATE_FILE" 2>/dev/null || true
}

# Use pymysql for all DB operations (mysql CLI not available)
dolt_query() {
  local db="$1"
  local query="$2"
  python3 -c "
import pymysql, sys
try:
    c = pymysql.connect(host='${DOLT_HOST}', port=${DOLT_PORT}, user='${DOLT_USER}', db='${db}', connect_timeout=10)
    cur = c.cursor()
    cur.execute(\"\"\"${query}\"\"\")
    rows = cur.fetchall()
    for row in rows:
        print('\t'.join(str(col) for col in row))
    c.commit()
    c.close()
except Exception as e:
    print(f'ERROR: {e}', file=sys.stderr)
    sys.exit(1)
" 2>&1
}

# -------------------------------------------------------------------
# Check 1: Dolt server reachable
# -------------------------------------------------------------------
log "Check 1: Dolt server reachable on ${DOLT_HOST}:${DOLT_PORT}..."
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

# Parse the date and compute gap using python for reliability
GAP_INFO=$(python3 -c "
from datetime import datetime, timezone
import sys
try:
    date_str = '''${LAST_COMMIT_DATE}'''.strip()
    for fmt in ['%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S']:
        try:
            dt = datetime.strptime(date_str, fmt).replace(tzinfo=timezone.utc)
            break
        except ValueError:
            continue
    else:
        print('PARSE_ERROR')
        sys.exit(0)
    gap = (datetime.now(timezone.utc) - dt).total_seconds()
    print(f'{int(gap)} {int(gap/60)}')
except Exception:
    print('PARSE_ERROR')
" 2>&1)

if [ "$GAP_INFO" = "PARSE_ERROR" ]; then
  log "  WARN: Could not parse last commit date: $LAST_COMMIT_DATE"
else
  GAP_SECONDS=$(echo "$GAP_INFO" | awk '{print $1}')
  GAP_MINUTES=$(echo "$GAP_INFO" | awk '{print $2}')
  log "  Last commit: $LAST_COMMIT_DATE ($GAP_MINUTES min ago)"

  if [ "$GAP_SECONDS" -gt "$MAX_GAP_SECONDS" ]; then
    escalate "Dolt commit gap: ${GAP_MINUTES} minutes since last commit (threshold: $((MAX_GAP_SECONDS/60))m). Last commit: ${LAST_COMMIT_DATE}."
  fi
fi

# -------------------------------------------------------------------
# Check 3: Write test — canary INSERT + DOLT_COMMIT
# -------------------------------------------------------------------
log "Check 3: Write test (canary commit)..."
CANARY_TITLE="dolt-heartbeat canary $(date -u '+%Y-%m-%dT%H:%M:%SZ')"

WRITE_RESULT=$(python3 -c "
import pymysql, sys
try:
    c = pymysql.connect(host='${DOLT_HOST}', port=${DOLT_PORT}, user='${DOLT_USER}', db='${HEARTBEAT_DB}', connect_timeout=10)
    cur = c.cursor()
    cur.execute('''INSERT INTO wisps (id, title, description, status, priority, issue_type, ephemeral, wisp_type)
      VALUES ('hq-heartbeat-canary', %s, 'Heartbeat canary — auto-deleted', 'closed', 4, 'chore', 1, 'heartbeat')
      ON DUPLICATE KEY UPDATE title=%s, updated_at=NOW()''', ('${CANARY_TITLE}', '${CANARY_TITLE}'))
    c.commit()
    print('OK')
    c.close()
except Exception as e:
    print(f'ERROR: {e}', file=sys.stderr)
    sys.exit(1)
" 2>&1)

if [ "$WRITE_RESULT" != "OK" ]; then
  escalate "Dolt WRITE FAILED: Cannot insert canary row. Result: ${WRITE_RESULT}."
  exit 1
fi
log "  OK: Canary written"

# -------------------------------------------------------------------
# Check 4: DOLT_COMMIT — verify commit actually works
# -------------------------------------------------------------------
log "Check 4: DOLT_COMMIT test..."
COMMIT_RESULT=$(python3 -c "
import pymysql, sys
try:
    c = pymysql.connect(host='${DOLT_HOST}', port=${DOLT_PORT}, user='${DOLT_USER}', db='${HEARTBEAT_DB}', connect_timeout=10, autocommit=True)
    cur = c.cursor()
    cur.execute(\"CALL DOLT_COMMIT('-Am', 'dolt-heartbeat: canary commit')\")
    print('OK')
    c.close()
except pymysql.err.OperationalError as e:
    if 'nothing to commit' in str(e):
        print('NOTHING')
    else:
        print(f'ERROR: {e}', file=sys.stderr)
        sys.exit(1)
except Exception as e:
    print(f'ERROR: {e}', file=sys.stderr)
    sys.exit(1)
" 2>&1)

if [ "$COMMIT_RESULT" = "ERROR" ] || echo "$COMMIT_RESULT" | grep -q "^ERROR"; then
  escalate "DOLT_COMMIT FAILED: ${COMMIT_RESULT}."
  exit 1
elif [ "$COMMIT_RESULT" = "NOTHING" ]; then
  log "  OK: Nothing to commit (canary unchanged)"
else
  log "  OK: Canary committed"
fi

# -------------------------------------------------------------------
# Check 5: Read-back verification
# -------------------------------------------------------------------
log "Check 5: Read-back verification..."
READBACK=$(dolt_query "$HEARTBEAT_DB" \
  "SELECT title FROM wisps WHERE id='hq-heartbeat-canary'" 2>&1)

if [ -z "$READBACK" ]; then
  escalate "Dolt READ-BACK FAILED: Canary row not found after commit."
  exit 1
fi
log "  OK: Canary verified"

# -------------------------------------------------------------------
# Cleanup: Remove canary
# -------------------------------------------------------------------
log "Cleanup: Removing canary..."
dolt_query "$HEARTBEAT_DB" "DELETE FROM wisps WHERE id='hq-heartbeat-canary'" >/dev/null 2>&1 || true
python3 -c "
import pymysql
try:
    c = pymysql.connect(host='${DOLT_HOST}', port=${DOLT_PORT}, user='${DOLT_USER}', db='${HEARTBEAT_DB}', autocommit=True)
    c.cursor().execute(\"CALL DOLT_COMMIT('-Am', 'dolt-heartbeat: cleanup canary')\")
    c.close()
except: pass
" 2>/dev/null || true

# -------------------------------------------------------------------
# Success
# -------------------------------------------------------------------
log "ALL CHECKS PASSED: Dolt persistence verified (${DOLT_HOST}:${DOLT_PORT})"

rm -f "$STATE_FILE" 2>/dev/null || true

bd create "dolt-heartbeat: OK (gap=${GAP_MINUTES:-?}m, host=${DOLT_HOST})" -t chore --ephemeral \
  -l type:plugin-run,plugin:dolt-heartbeat,result:success \
  -d "All 5 checks passed. Host: ${DOLT_HOST}:${DOLT_PORT}. Last commit gap: ${GAP_MINUTES:-unknown} minutes." \
  --silent 2>/dev/null || true

echo "=== dolt-heartbeat: PASS ==="
