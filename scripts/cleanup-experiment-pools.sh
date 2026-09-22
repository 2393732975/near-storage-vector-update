#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/cleanup-experiment-pools.sh [options]

Safely delete this project's Ceph experiment pools. Without
--confirm-cleanup, the script only prints the pools it would delete.

Options:
  --confirm-cleanup       Delete the selected pools.
  --include-legacy-ghnsw  Also delete legacy ghnsw_meta/ghnsw_owner_* pools.
  --wait-seconds N        Wait at most N seconds for clean PGs (default: 600).
  --poll-seconds N        Poll every N seconds (default: 5).
  -h, --help              Show this help.

Environment:
  CEPH_CMD           Ceph executable (default: ceph).
  CEPH_USE_SUDO      Set to true to invoke Ceph through sudo -n.
  CEPH_KEYRING       Optional local keyring path.
  EXPECTED_CEPH_FSID Abort unless the connected cluster has this FSID.
  OWNERS             Owner pool count (default: 5).

Exit status 3 means pool cleanup succeeded but overall Ceph health is not OK.
EOF
}

confirm_cleanup=false
include_legacy=false
wait_seconds=600
poll_seconds=5

while (( $# > 0 )); do
  case "$1" in
    --confirm-cleanup)
      confirm_cleanup=true
      shift
      ;;
    --include-legacy-ghnsw)
      include_legacy=true
      shift
      ;;
    --wait-seconds)
      [[ $# -ge 2 ]] || { echo "--wait-seconds requires a value" >&2; exit 2; }
      wait_seconds=$2
      shift 2
      ;;
    --poll-seconds)
      [[ $# -ge 2 ]] || { echo "--poll-seconds requires a value" >&2; exit 2; }
      poll_seconds=$2
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

for value_name in wait_seconds poll_seconds; do
  value=${!value_name}
  [[ "$value" =~ ^[1-9][0-9]*$ ]] || {
    echo "$value_name must be a positive integer, got: $value" >&2
    exit 2
  }
done

ceph_binary=${CEPH_CMD:-ceph}
ceph_cmd=("$ceph_binary")
if [[ "${CEPH_USE_SUDO:-false}" == true ]]; then
  ceph_cmd=(sudo -n "$ceph_binary")
fi
keyring=${CEPH_KEYRING:-}
owners=${OWNERS:-5}
[[ "$owners" =~ ^[1-9][0-9]*$ ]] || {
  echo "OWNERS must be a positive integer, got: $owners" >&2
  exit 2
}

ceph_args=()
if [[ -n "$keyring" ]]; then
  ceph_args+=(--keyring "$keyring")
fi

ceph_run() {
  "${ceph_cmd[@]}" "${ceph_args[@]}" "$@"
}

current_fsid=$(ceph_run fsid)
if [[ -n "${EXPECTED_CEPH_FSID:-}" && "$current_fsid" != "$EXPECTED_CEPH_FSID" ]]; then
  echo "Refusing cleanup: connected FSID $current_fsid does not match EXPECTED_CEPH_FSID $EXPECTED_CEPH_FSID" >&2
  exit 1
fi

targets=(nsvu_meta)
for (( owner = 0; owner < owners; owner++ )); do
  targets+=("nsvu_owner_$owner")
done
if [[ "$include_legacy" == true ]]; then
  targets+=(ghnsw_meta)
  for (( owner = 0; owner < owners; owner++ )); do
    targets+=("ghnsw_owner_$owner")
  done
fi

mapfile -t existing_pools < <(ceph_run osd pool ls)
selected=()
for target in "${targets[@]}"; do
  for pool in "${existing_pools[@]}"; do
    if [[ "$pool" == "$target" ]]; then
      selected+=("$target")
      break
    fi
  done
done

echo "Connected Ceph cluster: $current_fsid"
echo "Selected experiment pools:"
if (( ${#selected[@]} == 0 )); then
  echo "  (none present)"
else
  printf '  %s\n' "${selected[@]}"
fi

if [[ "$confirm_cleanup" != true ]]; then
  echo "Dry run only. Re-run with --confirm-cleanup to delete these exact pools."
  exit 0
fi

osd_stat=$(ceph_run osd stat)
if [[ ! "$osd_stat" =~ ([0-9]+)[[:space:]]osds:[[:space:]]([0-9]+)[[:space:]]up.*,[[:space:]]([0-9]+)[[:space:]]in ]]; then
  echo "Unable to parse OSD state; refusing cleanup: $osd_stat" >&2
  exit 1
fi
total_osds=${BASH_REMATCH[1]}
up_osds=${BASH_REMATCH[2]}
in_osds=${BASH_REMATCH[3]}
if (( total_osds != up_osds || total_osds != in_osds )); then
  echo "Refusing cleanup while OSDs are not all up/in: $osd_stat" >&2
  exit 1
fi

pg_stat=$(ceph_run pg stat)
if [[ ! "$pg_stat" =~ ([0-9]+)[[:space:]]pgs:[[:space:]]([0-9]+)[[:space:]]active\+clean ]] ||
   (( BASH_REMATCH[1] != BASH_REMATCH[2] )); then
  echo "Refusing cleanup while PGs are not all active+clean: $pg_stat" >&2
  exit 1
fi

for pool in "${selected[@]}"; do
  echo "Deleting experiment pool: $pool"
  ceph_run osd pool rm "$pool" "$pool" --yes-i-really-really-mean-it
done

deadline=$(( SECONDS + wait_seconds ))
while true; do
  mapfile -t remaining_pools < <(ceph_run osd pool ls)
  remaining_targets=0
  for target in "${selected[@]}"; do
    for pool in "${remaining_pools[@]}"; do
      if [[ "$pool" == "$target" ]]; then
        (( remaining_targets += 1 ))
        break
      fi
    done
  done

  pg_stat=$(ceph_run pg stat)
  pgs_clean=false
  if [[ "$pg_stat" =~ ([0-9]+)[[:space:]]pgs:[[:space:]]([0-9]+)[[:space:]]active\+clean ]] &&
     (( BASH_REMATCH[1] == BASH_REMATCH[2] )); then
    pgs_clean=true
  fi

  if (( remaining_targets == 0 )) && [[ "$pgs_clean" == true ]]; then
    break
  fi
  if (( SECONDS >= deadline )); then
    echo "Timed out waiting for pool removal and clean PGs." >&2
    echo "Last PG state: $pg_stat" >&2
    exit 1
  fi
  sleep "$poll_seconds"
done

echo "Experiment pools removed and all remaining PGs are active+clean."
ceph_run -s

health=$(ceph_run health)
if [[ "$health" != HEALTH_OK* ]]; then
  echo "Cleanup completed, but the cluster still has health findings unrelated to pool removal:" >&2
  ceph_run health detail >&2
  echo "BlueStore physical reclamation is asynchronous; do not force compaction or restart OSDs solely to clear a retained alert." >&2
  exit 3
fi

echo "Ceph health verification passed: $health"
