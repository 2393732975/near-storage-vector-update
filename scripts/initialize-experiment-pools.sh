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

ceph_args=()
rados_args=()
if [[ -n "$keyring" ]]; then
  ceph_args+=(--keyring "$keyring")
  rados_args+=(--keyring "$keyring")
fi

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

for pool in "$meta_pool" $(seq 0 $((owners - 1)) | sed "s#^#${prefix}#"); do
  if "$ceph_cmd" "${ceph_args[@]}" osd pool ls | grep -Fxq "$pool"; then
    "$ceph_cmd" "${ceph_args[@]}" osd pool rm "$pool" "$pool" --yes-i-really-really-mean-it
  fi
  "$ceph_cmd" "${ceph_args[@]}" osd pool create "$pool" 1 1 replicated
  "$ceph_cmd" "${ceph_args[@]}" osd pool set "$pool" size 1 --yes-i-really-mean-it
  "$ceph_cmd" "${ceph_args[@]}" osd pool set "$pool" min_size 1
  "$ceph_cmd" "${ceph_args[@]}" osd pool set "$pool" pg_autoscale_mode off
  "$ceph_cmd" "${ceph_args[@]}" osd pool application enable "$pool" rados
done

ensure_pool_placement "$meta_pool" 0
"$rados_cmd" "${rados_args[@]}" -p "$meta_pool" put hnsw.global.meta /dev/null
for owner in $(seq 0 $((owners - 1))); do
  ensure_pool_placement "${prefix}${owner}" "$owner"
  "$rados_cmd" "${rados_args[@]}" -p "${prefix}${owner}" put hnsw.owner.meta /dev/null
done
