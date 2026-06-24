#!/usr/bin/env bash
set -euo pipefail

log() {
  printf '[start-all] %s\n' "$*"
}

require_dir() {
  local dir="$1"
  if [ ! -d "$dir" ]; then
    printf '[start-all] missing directory: %s\n' "$dir" >&2
    exit 1
  fi
}

run_compose() {
  local dir="$1"
  shift
  require_dir "$dir"
  log "docker compose in $dir"
  (
    cd "$dir"
    docker compose "$@"
  )
}

run_make() {
  local dir="$1"
  shift
  require_dir "$dir"
  log "make $* in $dir"
  (
    cd "$dir"
    make "$@"
  )
}

ensure_network() {
  local name="$1"
  if ! docker network inspect "$name" >/dev/null 2>&1; then
    log "creating docker network $name"
    docker network create "$name" >/dev/null
  fi
}

main() {
  ensure_network media-net
  ensure_network airflow-net

  run_make /mnt/sata/airflow up
  run_make /mnt/sata/jober-son start
  run_compose /mnt/sata/chat-media up -d --build
  run_compose /mnt/sata/jellyfin up -d
  run_compose /mnt/sata/navidrome up -d
  run_compose /mnt/sata/reverse-proxy up -d

  log "all requested services started"
  docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'
}

main "$@"
