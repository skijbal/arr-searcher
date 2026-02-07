#!/usr/bin/env bash
set -u

log() {
  echo "[lidarr-runner] $(date '+%Y-%m-%d %H:%M:%S') $*"
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
  run_one "lidarr_tag_arr_extended_to_search.py" python -u /app/lidarr_tag_arr_extended_to_search.py
  run_one "lidarr_missing_done.py"              python -u /app/lidarr_missing_done.py
  run_one "lidarr_search.py"                    python -u /app/lidarr_search.py
  log "=== Cycle end; sleeping ${RUN_SLEEP_SECONDS}s ==="
  sleep "${RUN_SLEEP_SECONDS}"
done
