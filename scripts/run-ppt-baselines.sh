#!/usr/bin/env bash
set -euo pipefail

# Reproduces the PPT baselines. Each mode/dataset pair receives a fresh base
# import so OSD-side and compute-node results remain comparable.
: "${CEPH_KEYRING:?Set CEPH_KEYRING to a local, untracked Ceph keyring.}"
: "${DATASET_ROOT:?Set DATASET_ROOT to the directory holding the benchmark datasets.}"

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
window_seconds=${WINDOW_SECONDS:-300}
run_root=${RUN_ROOT:-"$repo_root/results/ppt-baseline-$(date -u +%Y%m%dT%H%M%SZ)"}
update_parallelism=${UPDATE_PARALLELISM:-1}
importer="$repo_root/build/nsvu-base-importer"
coordinator="$repo_root/build/nsvu-update-coordinator"

[[ -x "$importer" && -x "$coordinator" ]] || {
  echo "Build first with scripts/build.sh." >&2
  exit 2
}

run_dataset() {
  local mode=$1 dataset=$2 input update format dim kind metric count updates threads ppo
  case "$dataset" in
    gist1m)
      input="$DATASET_ROOT/gist1m/base.1M.fbin"; update="$DATASET_ROOT/gist1m/query.public.1K.fbin"; format=fbin; dim=960; kind=f32; metric=l2; count=1000000; updates=1000; threads=32; ppo=16000 ;;
    text2image10m)
      input="$DATASET_ROOT/text2image10m/base.10M.fbin"; update="$DATASET_ROOT/text2image10m/query.public.100K.fbin"; format=fbin; dim=200; kind=f32; metric=ip; count=10000000; updates=10000; threads=48; ppo=60000 ;;
    deep100m)
      input="$DATASET_ROOT/deep100m/base.100M.fbin"; update="$DATASET_ROOT/deep100m/query.public.10K.fbin"; format=fbin; dim=96; kind=f32; metric=l2; count=100000000; updates=10000; threads=64; ppo=160000 ;;
    sift100m)
      input="$DATASET_ROOT/sift100m/base.u8bin"; update="$DATASET_ROOT/sift100m/queries.u8bin"; format=u8bin; dim=128; kind=u8; metric=l2; count=100000000; updates=10000; threads=64; ppo=250000 ;;
    *) echo "Unknown dataset: $dataset" >&2; exit 2 ;;
  esac

  local output="$run_root/$mode/$dataset"
  mkdir -p "$output"
  CEPH_KEYRING="$CEPH_KEYRING" "$repo_root/scripts/initialize-experiment-pools.sh" --confirm-reset
  "$importer" --keyring "$CEPH_KEYRING" --input "$input" --input-format "$format" \
    --dim "$dim" --vector-kind "$kind" --metric "$metric" --num-vectors "$count" \
    --points-per-object "$ppo" --threads "$threads" --metrics-out "$output/import.json" \
    --progress-out "$output/import.progress.json"
  "$coordinator" --mode update --keyring "$CEPH_KEYRING" --input "$input" \
    --input-format "$format" --update-input "$update" --update-input-format "$format" \
    --dim "$dim" --vector-kind "$kind" --metric "$metric" --num-vectors "$count" \
    --num-updates "$updates" --update-offset 0 --target-start 0 --points-per-object "$ppo" --distance-mode "$mode" \
    --distance-split-probe --time-limit-seconds "$window_seconds" \
    --update-parallelism "$update_parallelism" --metrics-out "$output/update.json" \
    --progress-out "$output/update.progress.json"
}

for mode in osd compute; do
  for dataset in gist1m text2image10m deep100m sift100m; do
    run_dataset "$mode" "$dataset"
  done
done

echo "PPT baseline outputs: $run_root"
