#!/usr/bin/env bash
set -euo pipefail

# This script is intentionally safety-gated because it deletes and recreates
# experiment pools. It uses the caller's Ceph authentication; no passwords or
# keyrings belong in this repository.
if [[ "${1:-}" != "--confirm-reset" ]]; then
  echo "Refusing to modify Ceph pools. Re-run with --confirm-reset after verifying the target cluster." >&2
  exit 2
fi

ceph_cmd=${CEPH_CMD:-ceph}
rados_cmd=${RADOS_CMD:-rados}
keyring=${CEPH_KEYRING:-}
owners=${OWNERS:-5}
prefix=${OWNER_POOL_PREFIX:-nsvu_owner_}
meta_pool=${META_POOL:-nsvu_meta}

# OWNER_OSDS may use commas or whitespace, e.g. OWNERS=4 OWNER_OSDS=0,2,3,4.
# Owner IDs are logical shard IDs; OSD IDs need not be contiguous. Validate the
# entire destructive scope and placement before deleting even the first pool.
if [[ ! "$owners" =~ ^[1-5]$ ]]; then
  echo "OWNERS must be an integer in [1, 5] for the approved experiment pools." >&2
  exit 2
fi
if [[ "$prefix" != nsvu_owner_ || "$meta_pool" != nsvu_meta ]]; then
  echo "Refusing reset outside nsvu_meta and nsvu_owner_0 through nsvu_owner_4." >&2
  exit 2
fi
owner_osds=()
if [[ "${OWNER_OSDS+x}" == x ]]; then
  if [[ -z "${OWNER_OSDS//[[:space:]]/}" ||
        "$OWNER_OSDS" =~ (^|,)[[:space:]]*(,|$) ]]; then
    echo "OWNER_OSDS must contain nonempty OSD IDs separated by commas or spaces." >&2
    exit 2
  fi
  mapping_text=${OWNER_OSDS//,/ }
  mapping_text=${mapping_text//$'\n'/ }
  mapping_text=${mapping_text//$'\r'/ }
  read -r -a owner_osds <<< "$mapping_text"
else
  for (( owner = 0; owner < owners; owner++ )); do
    owner_osds+=("$owner")
  done
fi
if (( ${#owner_osds[@]} != owners )); then
  echo "OWNER_OSDS count must match OWNERS=$owners." >&2
  exit 2
fi
declare -A seen_osds=()
for (( owner = 0; owner < owners; owner++ )); do
  osd=${owner_osds[$owner]}
  if [[ ! "$osd" =~ ^[0-9]+$ || ${#osd} -gt 10 ]]; then
    echo "OWNER_OSDS entries must be nonnegative OSD IDs." >&2
    exit 2
  fi
  osd=$((10#$osd))
  if (( osd > 2147483647 )) || [[ -n "${seen_osds[$osd]+present}" ]]; then
    echo "OWNER_OSDS entries must be unique valid OSD IDs." >&2
    exit 2
  fi
  seen_osds[$osd]=1
  owner_osds[$owner]=$osd
done

ceph_args=()
rados_args=()
if [[ -n "$keyring" ]]; then
  ceph_args+=(--keyring "$keyring")
  rados_args+=(--keyring "$keyring")
fi

if [[ -n "${EXPECTED_CEPH_FSID:-}" ]]; then
  actual_fsid=$("$ceph_cmd" "${ceph_args[@]}" fsid)
  if [[ "$actual_fsid" != "$EXPECTED_CEPH_FSID" ]]; then
    echo "Refusing reset: connected FSID does not match EXPECTED_CEPH_FSID" >&2
    exit 1
  fi
fi

osd_state=$("$ceph_cmd" "${ceph_args[@]}" osd dump --format json)
printf '%s\n' "$osd_state" | python3 -c '
import json, sys
document = json.load(sys.stdin)
rows = document.get("osds", [])
states = {row["osd"]: row for row in rows}
if len(states) != len(rows):
    raise SystemExit("Refusing reset: duplicate OSD state records")
targets = [int(value) for value in sys.argv[1:]]
for target in targets:
    row = states.get(target)
    if row is None or row.get("up") != 1 or row.get("in") != 1:
        raise SystemExit("Refusing reset: target osd.%d must be present and up/in" % target)
' "${owner_osds[@]}"

ensure_pool_placement() {
  local pool=$1 target_osd=$2 pool_id pgid acting
  pool_id=$("$ceph_cmd" "${ceph_args[@]}" osd lspools | awk -v name="$pool" '$2 == name {print $1}')
  pgid="${pool_id}.0"
  acting=$("$ceph_cmd" "${ceph_args[@]}" pg map "$pgid" | sed -n 's/.*acting \[\([0-9]\+\)\].*/\1/p')
  if [[ -z "$pool_id" || -z "$acting" ]]; then
    echo "Unable to resolve placement for $pool" >&2
    exit 1
  fi
  if [[ "$acting" != "$target_osd" ]]; then
    "$ceph_cmd" "${ceph_args[@]}" osd pg-upmap-items "$pgid" "$acting" "$target_osd"
  fi
  "$ceph_cmd" "${ceph_args[@]}" pg map "$pgid"
}

ensure_single_pg_pool() {
  local pool=$1 pg_num autoscale_mode
  pg_num=$(
    "$ceph_cmd" "${ceph_args[@]}" osd pool get "$pool" pg_num |
      awk '$1 == "pg_num:" {print $2}'
  )
  autoscale_mode=$(
    "$ceph_cmd" "${ceph_args[@]}" osd pool get "$pool" pg_autoscale_mode |
      awk '$1 == "pg_autoscale_mode:" {print $2}'
  )
  if [[ "$pg_num" != "1" || "$autoscale_mode" != "off" ]]; then
    echo "Invalid experiment pool geometry: $pool pg_num=$pg_num autoscale=$autoscale_mode" >&2
    exit 1
  fi
}

pools=("$meta_pool")
for (( owner = 0; owner < owners; owner++ )); do
  pools+=("${prefix}${owner}")
done
for pool in "${pools[@]}"; do
  if "$ceph_cmd" "${ceph_args[@]}" osd pool ls | grep -Fxq "$pool"; then
    "$ceph_cmd" "${ceph_args[@]}" osd pool rm "$pool" "$pool" --yes-i-really-really-mean-it
  fi
  # Disable autoscaling atomically with pool creation. Setting it afterward
  # leaves a race in which the mgr can expand a freshly created one-PG pool.
  "$ceph_cmd" "${ceph_args[@]}" osd pool create "$pool" 1 1 replicated \
    --pg-num-min 1 --pg-num-max 1 --autoscale-mode off
  "$ceph_cmd" "${ceph_args[@]}" osd pool set "$pool" size 1 --yes-i-really-mean-it
  "$ceph_cmd" "${ceph_args[@]}" osd pool set "$pool" min_size 1
  "$ceph_cmd" "${ceph_args[@]}" osd pool set "$pool" pg_autoscale_mode off
  "$ceph_cmd" "${ceph_args[@]}" osd pool application enable "$pool" rados
  ensure_single_pg_pool "$pool"
done

ensure_pool_placement "$meta_pool" "${owner_osds[0]}"
"$rados_cmd" "${rados_args[@]}" -p "$meta_pool" put hnsw.global.meta /dev/null
for owner in $(seq 0 $((owners - 1))); do
  ensure_pool_placement "${prefix}${owner}" "${owner_osds[$owner]}"
  "$rados_cmd" "${rados_args[@]}" -p "${prefix}${owner}" put hnsw.owner.meta /dev/null
done
