#!/usr/bin/env bash
set -u

log() {
  echo "[sonarr-runner] $(date '+%Y-%m-%d %H:%M:%S') $*"
}

run_one() {
  local name="$1"
  shift
  log "START $name"
  "$@"
  local rc=$?
  if [[ $rc -eq 0 ]]; then
    log "OK    $name"
  else
    log "ERROR $name (exit=$rc)"
  fi
  return 0
}

RUN_SLEEP_SECONDS="${RUN_SLEEP_SECONDS:-3600}"
log "Starting loop. RUN_SLEEP_SECONDS=${RUN_SLEEP_SECONDS}"

while true; do
  log "=== Cycle begin ==="
  run_one "sonarr_missing_done.py" python -u /app/sonarr_missing_done.py
  run_one "sonarr_search.py"       python -u /app/sonarr_search.py
  log "=== Cycle end; sleeping ${RUN_SLEEP_SECONDS}s ==="
  sleep "${RUN_SLEEP_SECONDS}"
done
