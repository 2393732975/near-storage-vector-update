#!/usr/bin/env bash
set -euo pipefail

: "${CEPH_SRC:?Set CEPH_SRC to the Ceph src directory.}"
: "${HNSWLIB_INCLUDE:?Set HNSWLIB_INCLUDE to hnswlib/include.}"

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
exec make -C "$repo_root" CEPH_SRC="$CEPH_SRC" HNSWLIB_INCLUDE="$HNSWLIB_INCLUDE"
