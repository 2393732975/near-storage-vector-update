#!/usr/bin/env bash
set -euo pipefail

if [[ "${1:-}" != "--confirm-reset" ]]; then
  echo "Refusing to reset experiment pools. Re-run with --confirm-reset." >&2
  exit 2
fi

: "${CEPH_KEYRING:?Set CEPH_KEYRING to a local, untracked Ceph keyring.}"
: "${DATASET_ROOT:?Set DATASET_ROOT to the benchmark dataset directory.}"

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
run_root=${RUN_ROOT:-"$repo_root/results/phase1-ab-$(date -u +%Y%m%dT%H%M%SZ)"}
repetitions=${REPETITIONS:-3}
window_seconds=${WINDOW_SECONDS:-300}
parallelism=${UPDATE_PARALLELISM:-4}
checker_batch_size=${CHECKER_BATCH_SIZE:-8192}
semantic_samples=${SEMANTIC_SAMPLES:-256}
semantic_min_edge_win_rate=${SEMANTIC_MIN_EDGE_WIN_RATE:-0.60}
osd_op_timeout_seconds=${OSD_OP_TIMEOUT_SECONDS:-45}
osd_op_retry_limit=${OSD_OP_RETRY_LIMIT:-3}
recall_k=${RECALL_K:-10}
read -r -a datasets <<< "${DATASETS:-gist1m text2image10m deep100m sift100m}"

importer="$repo_root/build/nsvu-base-importer"
coordinator="$repo_root/build/nsvu-update-coordinator"
checker="$repo_root/build/nsvu-index-checker"
ceph_cli=(ceph --keyring "$CEPH_KEYRING")

[[ -x "$importer" && -x "$coordinator" && -x "$checker" ]] || {
  echo "Build importer, coordinator and checker before running phase 1." >&2
  exit 2
}
[[ "$repetitions" =~ ^[1-9][0-9]*$ ]] || { echo "REPETITIONS must be positive." >&2; exit 2; }
[[ "$window_seconds" =~ ^[1-9][0-9]*$ ]] || { echo "WINDOW_SECONDS must be positive." >&2; exit 2; }
[[ "$parallelism" =~ ^[1-9][0-9]*$ ]] || { echo "UPDATE_PARALLELISM must be positive." >&2; exit 2; }
[[ "$checker_batch_size" =~ ^[1-9][0-9]*$ ]] || { echo "CHECKER_BATCH_SIZE must be positive." >&2; exit 2; }
[[ "$semantic_samples" =~ ^[1-9][0-9]*$ ]] || { echo "SEMANTIC_SAMPLES must be positive." >&2; exit 2; }
python3 - "$semantic_min_edge_win_rate" <<'PY'
import sys

value = float(sys.argv[1])
if not 0.0 < value <= 1.0:
    raise SystemExit("SEMANTIC_MIN_EDGE_WIN_RATE must be in (0, 1].")
PY
[[ "$osd_op_timeout_seconds" =~ ^[1-9][0-9]*$ ]] || { echo "OSD_OP_TIMEOUT_SECONDS must be positive." >&2; exit 2; }
[[ "$osd_op_retry_limit" =~ ^[0-9]+$ ]] || { echo "OSD_OP_RETRY_LIMIT must be non-negative." >&2; exit 2; }
[[ "$recall_k" =~ ^[1-9][0-9]*$ ]] || { echo "RECALL_K must be positive." >&2; exit 2; }
(( ${#datasets[@]} > 0 )) || { echo "DATASETS must not be empty." >&2; exit 2; }

wait_for_clean_cluster() {
  local deadline=$((SECONDS + 300)) osd_stat pg_stat
  while true; do
    osd_stat=$("${ceph_cli[@]}" osd stat 2>/dev/null || true)
    pg_stat=$("${ceph_cli[@]}" pg stat 2>/dev/null || true)
    if [[ "$osd_stat" == *"5 up"* && "$osd_stat" == *"5 in"* ]] &&
       ! grep -Eq ' (inactive|peering|down|undersized|degraded|remapped|backfill|recovering|stale)' \
         <<<"$pg_stat"; then
      return
    fi
    if (( SECONDS >= deadline )); then
      echo "Cluster did not return to 5 up/5 in and active+clean: $osd_stat; $pg_stat" >&2
      return 1
    fi
    sleep 5
  done
}

load_dataset_config() {
  local dataset=$1
  case "$dataset" in
    gist1m)
      input="$DATASET_ROOT/gist1m/base.1M.fbin"
      update="$DATASET_ROOT/gist1m/query.public.1K.fbin"
      ground_truth="$DATASET_ROOT/gist1m/gt.public.1K.top1000.ibin"
      format=fbin dim=960 kind=f32 metric=l2 count=1000000 updates=1000 threads=32 ppo=16000
      ;;
    text2image10m)
      input="$DATASET_ROOT/text2image10m/base.10M.fbin"
      update="$DATASET_ROOT/text2image10m/query.public.100K.fbin"
      ground_truth="$DATASET_ROOT/text2image10m/text2image-10M.gt.bin"
      format=fbin dim=200 kind=f32 metric=ip count=10000000 updates=10000 threads=48 ppo=60000
      ;;
    deep100m)
      input="$DATASET_ROOT/deep100m/base.100M.fbin"
      update="$DATASET_ROOT/deep100m/query.public.10K.fbin"
      ground_truth="$DATASET_ROOT/deep100m/deep-100M.gt.bin"
      format=fbin dim=96 kind=f32 metric=l2 count=100000000 updates=10000 threads=64 ppo=160000
      ;;
    sift100m)
      input="$DATASET_ROOT/sift100m/base.u8bin"
      update="$DATASET_ROOT/sift100m/queries.u8bin"
      ground_truth="$DATASET_ROOT/sift100m/gt_100.bin"
      format=u8bin dim=128 kind=u8 metric=l2 count=100000000 updates=10000 threads=64 ppo=250000
      ;;
    *) echo "Unknown dataset: $dataset" >&2; return 2 ;;
  esac
  for path in "$input" "$update" "$ground_truth"; do
    [[ -r "$path" ]] || { echo "Required dataset file is not readable: $path" >&2; return 2; }
  done
}

record_placement() {
  local output=$1 pool pool_id
  for pool in nsvu_meta nsvu_owner_0 nsvu_owner_1 nsvu_owner_2 nsvu_owner_3 nsvu_owner_4; do
    "${ceph_cli[@]}" osd pool get "$pool" size
    pool_id=$("${ceph_cli[@]}" osd pool ls detail --format json | python3 -c \
      'import json,sys; name=sys.argv[1]; print(next(p["pool_id"] for p in json.load(sys.stdin) if p["pool_name"] == name))' "$pool")
    "${ceph_cli[@]}" pg map "${pool_id}.0"
  done >"$output/placement.txt"
}

validate_run() {
  local update_json=$1 check_json=$2
  python3 - "$update_json" "$check_json" <<'PY'
import json
import sys

update = json.load(open(sys.argv[1], encoding="utf-8"))
check = json.load(open(sys.argv[2], encoding="utf-8"))
quality = update.get("quality", {})
summary = {
    "vectors_processed": update["vectors_processed"],
    "failed_updates": update["failed_updates"],
    "throughput_updates_per_sec": update["throughput_updates_per_sec"],
    "avg_update_latency_ms": update["avg_update_latency_ms"],
    "p99_update_latency_ms": update["p99_update_latency_ms"],
    "precommit_static_recall_at_k": quality.get("precommit_static_recall_at_k"),
    "checker_status": check["status"],
    "checker_errors": check["errors"],
    "checker_warnings": check["warnings"],
    "semantic_edge_win_rate": check["semantic_locality"]["edge_win_rate"],
}
print(summary)
if update["failed_updates"] != 0 or check["status"] != "pass":
    raise SystemExit(1)
if not quality.get("ground_truth_enabled"):
    raise SystemExit("ground truth was not enabled")
if quality.get("evaluated_updates") != update["vectors_processed"]:
    raise SystemExit("Recall evaluation count does not match successful updates")
if not check["semantic_locality"].get("enabled"):
    raise SystemExit("Semantic locality check was not enabled")
if check["semantic_locality"].get("edges_compared", 0) == 0:
    raise SystemExit("Semantic locality check made no comparisons")
PY
}

if [[ -e "$run_root" ]]; then
  [[ -d "$run_root" ]] || { echo "RUN_ROOT is not a directory: $run_root" >&2; exit 2; }
  unexpected=$(find "$run_root" -mindepth 1 -maxdepth 1 \
    ! -name runner.log ! -name runner.pid -print -quit)
  [[ -z "$unexpected" ]] || { echo "RUN_ROOT is not empty: $run_root" >&2; exit 2; }
else
  mkdir -p "$run_root"
fi

# Pin client binaries for the whole multi-hour run. Rebuilding the repository
# while this process is active must not silently mix executable versions.
mkdir "$run_root/bin"
cp "$importer" "$coordinator" "$checker" "$repo_root/build/libcls_hnsw_global.so" \
  "$run_root/bin/"
importer="$run_root/bin/nsvu-base-importer"
coordinator="$run_root/bin/nsvu-update-coordinator"
checker="$run_root/bin/nsvu-index-checker"
cls_binary="$run_root/bin/libcls_hnsw_global.so"
exec > >(tee -a "$run_root/suite.log") 2>&1

{
  echo "phase1_strict_ab"
  echo "started_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "git_commit=$(git -C "$repo_root" rev-parse HEAD)"
  echo "coordinator_sha256=$(sha256sum "$coordinator" | awk '{print $1}')"
  echo "importer_sha256=$(sha256sum "$importer" | awk '{print $1}')"
  echo "checker_sha256=$(sha256sum "$checker" | awk '{print $1}')"
  echo "cls_sha256=$(sha256sum "$cls_binary" | awk '{print $1}')"
  echo "ceph_version=$(ceph --version)"
  echo "ceph_fsid=$("${ceph_cli[@]}" fsid)"
  echo "repetitions=$repetitions"
  echo "window_seconds=$window_seconds"
  echo "update_parallelism=$parallelism"
  echo "checker_batch_size=$checker_batch_size"
  echo "semantic_samples=$semantic_samples"
  echo "semantic_min_edge_win_rate=$semantic_min_edge_win_rate"
  echo "osd_op_timeout_seconds=$osd_op_timeout_seconds"
  echo "osd_op_retry_limit=$osd_op_retry_limit"
  echo "recall_k=$recall_k"
  echo "cache_condition=fresh-pool-after-import (not a controlled cold-cache run)"
  echo "datasets=${datasets[*]}"
  for dataset in "${datasets[@]}"; do
    load_dataset_config "$dataset"
    stat -c "dataset_file=%n size=%s mtime=%y" "$input" "$update" "$ground_truth"
  done
} >"$run_root/manifest.txt"

for repetition in $(seq 1 "$repetitions"); do
  if (( repetition % 2 == 1 )); then
    modes=(compute osd)
  else
    modes=(osd compute)
  fi
  echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] repetition $repetition/$repetitions order=${modes[*]}"
  for dataset in "${datasets[@]}"; do
    load_dataset_config "$dataset"
    for mode in "${modes[@]}"; do
      output="$run_root/rep-$repetition/$mode/$dataset"
      mkdir -p "$output"
      echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] reset/import rep=$repetition mode=$mode dataset=$dataset"
      wait_for_clean_cluster
      CEPH_KEYRING="$CEPH_KEYRING" \
        "$repo_root/scripts/initialize-experiment-pools.sh" --confirm-reset
      wait_for_clean_cluster
      record_placement "$output"

      "$importer" --keyring "$CEPH_KEYRING" --input "$input" --input-format "$format" \
        --dim "$dim" --vector-kind "$kind" --metric "$metric" --num-vectors "$count" \
        --points-per-object "$ppo" --threads "$threads" --metrics-out "$output/import.json" \
        --progress-out "$output/import.progress.json"

      echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] update rep=$repetition mode=$mode dataset=$dataset"
      "$coordinator" --mode update --keyring "$CEPH_KEYRING" --input "$input" \
        --input-format "$format" --update-input "$update" --update-input-format "$format" \
        --ground-truth "$ground_truth" --recall-k "$recall_k" \
        --dim "$dim" --vector-kind "$kind" --metric "$metric" --num-vectors "$count" \
        --num-updates "$updates" --update-offset 0 --target-start 0 \
        --points-per-object "$ppo" --distance-mode "$mode" --distance-split-probe \
        --distance-probe-interval-ms 1000 --time-limit-seconds "$window_seconds" \
        --update-parallelism "$parallelism" \
        --osd-op-timeout-seconds "$osd_op_timeout_seconds" \
        --osd-op-retry-limit "$osd_op_retry_limit" \
        --metrics-out "$output/update.json" --progress-out "$output/update.progress.json"

      echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] check rep=$repetition mode=$mode dataset=$dataset"
      "$checker" --keyring "$CEPH_KEYRING" --owners 5 --points-per-object "$ppo" \
        --batch-size "$checker_batch_size" --reference-input "$input" \
        --reference-input-format "$format" --reference-count "$count" \
        --semantic-samples "$semantic_samples" \
        --semantic-min-edge-win-rate "$semantic_min_edge_win_rate" \
        --output "$output/index-check.json"
      validate_run "$output/update.json" "$output/index-check.json"
      wait_for_clean_cluster
    done
  done
done

python3 "$repo_root/scripts/summarize-phase1-ab.py" "$run_root" \
  --output "$run_root/summary.md" --json-output "$run_root/summary.json"
echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] phase-1 strict A/B passed: $run_root"
