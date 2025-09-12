#!/usr/bin/env bash
set -Eeuo pipefail

# Space-separated list of host:port pairs to wait for
WAIT_FOR="${WAIT_FOR:-postgres:5432 redis:6379}"
WAIT_TIMEOUT="${WAIT_TIMEOUT:-120}"      # seconds per host
SLEEP_BETWEEN="${SLEEP_BETWEEN:-2}"     # seconds between probes

log(){ echo "[$(date -u +'%Y-%m-%dT%H:%M:%SZ')] $*"; }

# Check if Celery is available
check_celery() {
    if ! command -v celery &> /dev/null; then
        log "ERROR: Celery executable not found in PATH"
        exit 1
    fi
    log "OK: Celery is available"
}


wait_for() {
  local host="${1%:*}" port="${1#*:}" elapsed=0
  log "Waiting for ${host}:${port} (timeout ${WAIT_TIMEOUT}s)…"
  while ! (exec 3<>"/dev/tcp/${host}/${port}") 2>/dev/null; do
    sleep "${SLEEP_BETWEEN}"
    elapsed=$((elapsed + SLEEP_BETWEEN))
    if (( elapsed >= WAIT_TIMEOUT )); then
      log "ERROR: timeout waiting for ${host}:${port}"
      exit 1
    fi
  done
  exec 3>&- 3<&-
  log "OK: ${host}:${port} reachable."
}

# 1) Wait for dependencies
for hp in ${WAIT_FOR}; do
  wait_for "${hp}"
done

# 2) Hand off to the command we were given (Airflow's /entrypoint and args)
log "Starting: $*"
exec "$@"
