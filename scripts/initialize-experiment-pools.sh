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
owners=${OWNERS:-5}
prefix=${OWNER_POOL_PREFIX:-nsvu_owner_}
meta_pool=${META_POOL:-nsvu_meta}

for pool in "$meta_pool" $(seq 0 $((owners - 1)) | sed "s#^#${prefix}#"); do
  if "$ceph_cmd" osd pool ls | grep -Fxq "$pool"; then
    "$ceph_cmd" osd pool rm "$pool" "$pool" --yes-i-really-really-mean-it
  fi
  "$ceph_cmd" osd pool create "$pool" 1 1 replicated
  "$ceph_cmd" osd pool set "$pool" size 1 --yes-i-really-mean-it
  "$ceph_cmd" osd pool set "$pool" min_size 1
  "$ceph_cmd" osd pool set "$pool" pg_autoscale_mode off
  "$ceph_cmd" osd pool application enable "$pool" rados
done

"$rados_cmd" -p "$meta_pool" put hnsw.global.meta /dev/null
for owner in $(seq 0 $((owners - 1))); do
  "$rados_cmd" -p "${prefix}${owner}" put hnsw.owner.meta /dev/null
done

echo "Verify placement before benchmarking: $ceph_cmd pg map <pool-id>.0"
