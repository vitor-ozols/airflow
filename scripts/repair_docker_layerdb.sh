#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  scripts/repair_docker_layerdb.sh [--prune-build-cache] [--docker-root PATH] LAYER_SHA

Example from a Docker error:
  scripts/repair_docker_layerdb.sh bd390c4004551b864690350fce7fa25e484463c0fdcf227917cc5ee042c739b0

What it does:
  - stops this compose stack without removing volumes
  - stops docker/docker.socket
  - moves the conflicting layerdb entry and its descendant chain to quarantine
  - moves associated overlay2 cache directories to quarantine
  - starts Docker again
  - optionally prunes BuildKit cache with --prune-build-cache

Nothing is deleted directly. Quarantine is written next to Docker's data root.
USAGE
}

log() {
  printf '[repair-layerdb] %s\n' "$*"
}

die() {
  printf '[repair-layerdb] ERROR: %s\n' "$*" >&2
  exit 1
}

strip_sha() {
  local value="$1"
  value="${value#sha256:}"
  value="${value##*/}"
  printf '%s' "$value"
}

wait_for_docker() {
  local i
  for i in $(seq 1 30); do
    if docker info >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  return 1
}

prune_build_cache=0
docker_root=""
layer=""

while [ "$#" -gt 0 ]; do
  case "$1" in
    --prune-build-cache)
      prune_build_cache=1
      shift
      ;;
    --docker-root)
      [ "$#" -ge 2 ] || die "--docker-root requires a path"
      docker_root="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    --*)
      die "unknown option: $1"
      ;;
    *)
      [ -z "$layer" ] || die "only one layer SHA can be provided"
      layer="$(strip_sha "$1")"
      shift
      ;;
  esac
done

[ -n "$layer" ] || { usage; exit 2; }
case "$layer" in
  *[!0-9a-f]*|'')
    die "invalid layer SHA: $layer"
    ;;
esac
[ "${#layer}" -eq 64 ] || die "layer SHA must have 64 hex chars: $layer"

command -v docker >/dev/null 2>&1 || die "docker not found"
command -v sudo >/dev/null 2>&1 || die "sudo not found"

if [ -z "$docker_root" ]; then
  docker_root="$(docker info --format '{{.DockerRootDir}}' 2>/dev/null || true)"
fi
if [ -z "$docker_root" ]; then
  if [ -d /mnt/sata/docker ]; then
    docker_root=/mnt/sata/docker
  else
    docker_root=/var/lib/docker
  fi
fi

layerdb="$docker_root/image/overlay2/layerdb/sha256"
overlay="$docker_root/overlay2"
quarantine_parent="${docker_root%/}-quarantine"
quarantine="$quarantine_parent/$(date +%Y%m%d-%H%M%S)-layer-$layer"

[ -d "$layerdb" ] || die "layerdb not found: $layerdb"

log "docker root: $docker_root"
log "target layer: $layer"
log "quarantine: $quarantine"

log "stopping compose stack"
docker compose down --remove-orphans || true

log "stopping Docker service and socket"
sudo systemctl stop docker docker.socket

restart_docker() {
  log "starting Docker"
  sudo systemctl start docker.socket
  sudo systemctl start docker
  wait_for_docker || die "Docker did not become ready"
}
trap restart_docker EXIT

sudo mkdir -p "$quarantine"

seen=" $layer "
layers="$layer"
changed=1
while [ "$changed" -eq 1 ]; do
  changed=0
  for parent_file in "$layerdb"/*/parent; do
    [ -f "$parent_file" ] || continue
    parent="$(sudo cat "$parent_file" 2>/dev/null || true)"
    parent="${parent#sha256:}"
    case "$seen" in
      *" $parent "*)
        child="${parent_file%/parent}"
        child="${child##*/}"
        case "$seen" in
          *" $child "*) ;;
          *)
            seen="$seen$child "
            layers="$layers $child"
            changed=1
            ;;
        esac
        ;;
    esac
  done
done

moved=0
for item in $layers; do
  layer_dir="$layerdb/$item"
  [ -d "$layer_dir" ] || continue

  cache_id="$(sudo cat "$layer_dir/cache-id" 2>/dev/null || true)"
  log "quarantining layer $item cache-id=${cache_id:-none}"
  sudo mv "$layer_dir" "$quarantine/layerdb-$item"
  moved=$((moved + 1))

  if [ -n "$cache_id" ] && [ -d "$overlay/$cache_id" ]; then
    sudo mv "$overlay/$cache_id" "$quarantine/overlay2-$cache_id"
  fi
done

if [ "$moved" -eq 0 ]; then
  log "no matching layerdb directories found; nothing moved"
else
  log "moved $moved layerdb entries to quarantine"
fi

trap - EXIT
restart_docker

if [ "$prune_build_cache" -eq 1 ]; then
  log "pruning BuildKit cache"
  docker builder prune -af
fi

log "done. Next step: make up"
