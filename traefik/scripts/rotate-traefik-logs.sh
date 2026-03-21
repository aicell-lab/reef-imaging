#!/usr/bin/env bash
set -euo pipefail

LOG_DIR="/home/tao/workspace/reef-imaging/traefik/logs"
MAX_BYTES=$((500 * 1024 * 1024))   # 500 MB rotate threshold
RETENTION_DAYS=14

rotate_one() {
  local file="$1"
  [ -f "$file" ] || return 0

  local size
  size=$(stat -c %s "$file" 2>/dev/null || echo 0)
  if [ "$size" -lt "$MAX_BYTES" ]; then
    return 0
  fi

  local ts out
  ts=$(date +"%Y-%m-%dT%H-%M-%S")
  out="${file%.log}-${ts}.log"

  # Preserve same inode for running writers (traefik) while archiving contents.
  cp -p "$file" "$out"
  : > "$file"
  gzip -f "$out"
}

rotate_one "$LOG_DIR/traefik.log"
rotate_one "$LOG_DIR/access.log"

# Delete rotated logs older than retention window.
find "$LOG_DIR" -maxdepth 1 -type f -name "traefik-*.log.gz" -mtime +"$RETENTION_DAYS" -delete
find "$LOG_DIR" -maxdepth 1 -type f -name "access-*.log.gz" -mtime +"$RETENTION_DAYS" -delete
