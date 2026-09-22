#!/usr/bin/env bash
set -euo pipefail

if [[ "${1:-}" != "--confirm-reset" ]]; then
  echo "Refusing to reset experiment pools. Re-run with --confirm-reset." >&2
  exit 2
fi

: "${CEPH_KEYRING:?Set CEPH_KEYRING to a local, untracked Ceph keyring.}"
: "${DATASET_ROOT:?Set DATASET_ROOT to the benchmark dataset directory.}"

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
rounds=${ROUNDS:-3}
window_seconds=${WINDOW_SECONDS:-300}
parallelism=${UPDATE_PARALLELISM:-4}
checker_batch_size=${CHECKER_BATCH_SIZE:-8192}
run_root=${RUN_ROOT:-"$repo_root/results/phase0-validation-$(date -u +%Y%m%dT%H%M%SZ)"}
read -r -a datasets <<< "${DATASETS:-gist1m text2image10m deep100m sift100m}"

importer="$repo_root/build/nsvu-base-importer"
coordinator="$repo_root/build/nsvu-update-coordinator"
checker="$repo_root/build/nsvu-index-checker"
ceph_cli=(ceph --keyring "$CEPH_KEYRING")

[[ -x "$importer" && -x "$coordinator" && -x "$checker" ]] || {
  echo "Build importer, coordinator and checker before running validation." >&2
  exit 2
}
[[ "$rounds" =~ ^[1-9][0-9]*$ ]] || { echo "ROUNDS must be positive." >&2; exit 2; }
[[ "$window_seconds" =~ ^[1-9][0-9]*$ ]] || {
  echo "WINDOW_SECONDS must be positive." >&2
  exit 2
}
[[ "$parallelism" =~ ^[1-9][0-9]*$ ]] || {
  echo "UPDATE_PARALLELISM must be positive." >&2
  exit 2
}

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
      format=fbin dim=960 kind=f32 metric=l2 count=1000000 updates=1000 threads=32 ppo=16000
      ;;
    text2image10m)
      input="$DATASET_ROOT/text2image10m/base.10M.fbin"
      update="$DATASET_ROOT/text2image10m/query.public.100K.fbin"
      format=fbin dim=200 kind=f32 metric=ip count=10000000 updates=10000 threads=48 ppo=60000
      ;;
    deep100m)
      input="$DATASET_ROOT/deep100m/base.100M.fbin"
      update="$DATASET_ROOT/deep100m/query.public.10K.fbin"
      format=fbin dim=96 kind=f32 metric=l2 count=100000000 updates=10000 threads=64 ppo=160000
      ;;
    sift100m)
      input="$DATASET_ROOT/sift100m/base.u8bin"
      update="$DATASET_ROOT/sift100m/queries.u8bin"
      format=u8bin dim=128 kind=u8 metric=l2 count=100000000 updates=10000 threads=64 ppo=250000
      ;;
    *) echo "Unknown dataset: $dataset" >&2; return 2 ;;
  esac
  [[ -r "$input" && -r "$update" ]] || {
    echo "Dataset files are not readable for $dataset." >&2
    return 2
  }
}

mkdir -p "$run_root"
exec > >(tee -a "$run_root/suite.log") 2>&1

{
  echo "phase0_validation"
  echo "started_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "git_commit=$(git -C "$repo_root" rev-parse HEAD)"
  echo "cls_sha256=$(sha256sum "$repo_root/build/libcls_hnsw_global.so" | awk '{print $1}')"
  echo "ceph_version=$(ceph --version)"
  echo "ceph_fsid=$("${ceph_cli[@]}" fsid)"
  echo "rounds=$rounds"
  echo "window_seconds=$window_seconds"
  echo "update_parallelism=$parallelism"
  echo "checker_batch_size=$checker_batch_size"
  echo "datasets=${datasets[*]}"
} >"$run_root/manifest.txt"

for dataset in "${datasets[@]}"; do
  load_dataset_config "$dataset"
  output="$run_root/$dataset"
  mkdir -p "$output"
  echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] reset/import $dataset"
  wait_for_clean_cluster
  CEPH_KEYRING="$CEPH_KEYRING" "$repo_root/scripts/initialize-experiment-pools.sh" --confirm-reset
  wait_for_clean_cluster
  for pool in nsvu_meta nsvu_owner_0 nsvu_owner_1 nsvu_owner_2 nsvu_owner_3 nsvu_owner_4; do
    "${ceph_cli[@]}" osd pool get "$pool" size
    pool_id=$("${ceph_cli[@]}" osd pool ls detail --format json | python3 -c \
      'import json,sys; name=sys.argv[1]; print(next(p["pool_id"] for p in json.load(sys.stdin) if p["pool_name"] == name))' "$pool")
    "${ceph_cli[@]}" pg map "${pool_id}.0"
  done >"$output/placement.txt"

  "$importer" --keyring "$CEPH_KEYRING" --input "$input" --input-format "$format" \
    --dim "$dim" --vector-kind "$kind" --metric "$metric" --num-vectors "$count" \
    --points-per-object "$ppo" --threads "$threads" --metrics-out "$output/import.json" \
    --progress-out "$output/import.progress.json"

  for round in $(seq 1 "$rounds"); do
    round_dir="$output/round-$round"
    mkdir -p "$round_dir"
    echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] update $dataset round $round/$rounds"
    "$coordinator" --mode update --keyring "$CEPH_KEYRING" --input "$input" \
      --input-format "$format" --update-input "$update" --update-input-format "$format" \
      --dim "$dim" --vector-kind "$kind" --metric "$metric" --num-vectors "$count" \
      --num-updates "$updates" --update-offset 0 --target-start 0 \
      --points-per-object "$ppo" --distance-mode osd --distance-split-probe \
      --distance-probe-interval-ms 1000 --time-limit-seconds "$window_seconds" \
      --update-parallelism "$parallelism" --metrics-out "$round_dir/update.json" \
      --progress-out "$round_dir/update.progress.json"

    echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] check $dataset round $round/$rounds"
    "$checker" --keyring "$CEPH_KEYRING" --owners 5 --points-per-object "$ppo" \
      --batch-size "$checker_batch_size" --output "$round_dir/index-check.json"
    python3 - "$round_dir/update.json" "$round_dir/index-check.json" <<'PY'
import json
import sys

update = json.load(open(sys.argv[1], encoding="utf-8"))
check = json.load(open(sys.argv[2], encoding="utf-8"))
print({
    "vectors_processed": update["vectors_processed"],
    "failed_updates": update["failed_updates"],
    "throughput_updates_per_sec": update["throughput_updates_per_sec"],
    "p99_update_latency_ms": update["p99_update_latency_ms"],
    "checker_status": check["status"],
    "checker_errors": check["errors"],
    "checker_warnings": check["warnings"],
})
if update["failed_updates"] != 0 or check["status"] != "pass":
    raise SystemExit(1)
PY
    wait_for_clean_cluster
  done
done

echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] phase-0 validation passed: $run_root"
