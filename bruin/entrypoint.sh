#!/bin/sh
set -eu

PAUSE="${KAFKA_POLL_PAUSE:-5}"
MAX_RETRIES="${KAFKA_MAX_RETRIES:-5}"
RETRY_DELAY="${KAFKA_RETRY_DELAY:-10}"

log() { echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") | $*"; }

sleep "${SLEEP_BEFORE_FIRST_RUN:-10}"

log "Starting continuous Kafka ingest loop (pause=${PAUSE}s, retries=${MAX_RETRIES})"

consecutive_failures=0

while true; do
  if bruin run bruin-pipeline/assets/ingest/kafka_to_pg.asset.yml >> /proc/1/fd/1 2>&1; then
    consecutive_failures=0
  else
    consecutive_failures=$((consecutive_failures + 1))
    log "WARN: kafka_to_pg failed (${consecutive_failures}/${MAX_RETRIES})"
    if [ "$consecutive_failures" -ge "$MAX_RETRIES" ]; then
      log "ERROR: kafka_to_pg failed ${MAX_RETRIES} times in a row, backing off 60s"
      sleep 60
      consecutive_failures=0
    else
      sleep "$RETRY_DELAY"
      continue
    fi
  fi

  sleep "$PAUSE"
done
