#!/usr/bin/env bash
set -euo pipefail

if [[ "${1:-}" != "--confirm-reset" ]]; then
  echo "Refusing to reset experiment pools. Re-run with --confirm-reset." >&2
  exit 2
fi

: "${CEPH_KEYRING:?Set CEPH_KEYRING to a local, untracked Ceph keyring.}"
: "${DATASET_ROOT:?Set DATASET_ROOT to the benchmark dataset directory.}"

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
run_root=${RUN_ROOT:-"$repo_root/results/stage5a-full-ab-$(date -u +%Y%m%dT%H%M%SZ)"}
repetitions=${REPETITIONS:-3}
window_seconds=${WINDOW_SECONDS:-300}
parallelism=${UPDATE_PARALLELISM:-4}
max_regression=${PAIRED_RECALL_MAX_REGRESSION:-0.005}
read -r -a datasets <<< "${DATASETS:-gist1m text2image10m deep100m sift100m}"
runner="$repo_root/scripts/run-phase1-ab.sh"
compare="$repo_root/scripts/compare-paired-recall.py"

[[ "$repetitions" =~ ^[1-9][0-9]*$ ]] || {
  echo "REPETITIONS must be positive." >&2
  exit 2
}
[[ "$window_seconds" =~ ^[1-9][0-9]*$ ]] || {
  echo "WINDOW_SECONDS must be positive." >&2
  exit 2
}
[[ "$parallelism" =~ ^[1-9][0-9]*$ ]] || {
  echo "UPDATE_PARALLELISM must be positive." >&2
  exit 2
}
(( ${#datasets[@]} > 0 )) || { echo "DATASETS must not be empty." >&2; exit 2; }

if [[ -e "$run_root" ]]; then
  [[ -d "$run_root" ]] || { echo "RUN_ROOT is not a directory: $run_root" >&2; exit 2; }
  unexpected=$(find "$run_root" -mindepth 1 -maxdepth 1 \
    ! -name runner.log ! -name runner.pid -print -quit)
  [[ -z "$unexpected" ]] || { echo "RUN_ROOT is not empty: $run_root" >&2; exit 2; }
else
  mkdir -p "$run_root"
fi

exec > >(tee -a "$run_root/full-ab.log") 2>&1
mkdir -p "$run_root/cache"

log() {
  echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $*"
}

sha() {
  sha256sum "$1" | awk '{print $1}'
}

coordinator="$repo_root/build/nsvu-update-coordinator"
importer="$repo_root/build/nsvu-base-importer"
checker="$repo_root/build/nsvu-index-checker"
cls="$repo_root/build/libcls_hnsw_global.so"
for binary in "$coordinator" "$importer" "$checker" "$cls"; do
  [[ -s "$binary" ]] || { echo "Required build artifact is missing: $binary" >&2; exit 2; }
done

coordinator_sha=$(sha "$coordinator")
importer_sha=$(sha "$importer")
checker_sha=$(sha "$checker")
cls_sha=$(sha "$cls")

verify_binaries() {
  [[ "$(sha "$coordinator")" == "$coordinator_sha" &&
     "$(sha "$importer")" == "$importer_sha" &&
     "$(sha "$checker")" == "$checker_sha" &&
     "$(sha "$cls")" == "$cls_sha" ]] || {
    echo "Build artifacts changed during the formal suite; refusing to mix binaries." >&2
    return 1
  }
}

case_update() {
  local case_root=$1 dataset=$2
  echo "$case_root/rep-1/osd/$dataset/update.json"
}

run_case() {
  local repetition=$1 dataset=$2 variant=$3 microbatch=$4 cache_mode=$5 cache=$6
  local output="$run_root/rep-$repetition/$variant/$dataset"
  verify_binaries
  mkdir -p "$output"
  log "START rep=$repetition dataset=$dataset variant=$variant cache_mode=$cache_mode"
  env \
    CEPH_KEYRING="$CEPH_KEYRING" \
    DATASET_ROOT="$DATASET_ROOT" \
    DATASETS="$dataset" \
    MODES=osd \
    REPETITIONS=1 \
    WINDOW_SECONDS="$window_seconds" \
    HNSW_INDEX_CACHE="$cache" \
    HNSW_INDEX_CACHE_MODE="$cache_mode" \
    UPDATE_PARALLELISM="$parallelism" \
    FRONTIER_WINDOW=1 \
    FRONTIER_ADAPTIVE=false \
    DISTANCE_MICROBATCH="$microbatch" \
    DISTANCE_MICROBATCH_WAIT_US=250 \
    DISTANCE_MICROBATCH_MAX_BYTES=262144 \
    DISTANCE_MICROBATCH_MAX_CANDIDATES=256 \
    DISTANCE_MICROBATCH_MAX_SUBREQUESTS=32 \
    DISTANCE_EXECUTOR_THREADS=5 \
    CHECKER_BATCH_SIZE=8192 \
    SEMANTIC_SAMPLES=256 \
    SEMANTIC_MIN_EDGE_WIN_RATE=0.60 \
    OSD_OP_TIMEOUT_SECONDS=45 \
    OSD_OP_RETRY_LIMIT=3 \
    RECALL_K=10 \
    RUN_ROOT="$output" \
    "$runner" --confirm-reset >"$output/runner.log" 2>&1
  verify_binaries
  log "PASS rep=$repetition dataset=$dataset variant=$variant"
}

write_summary() {
  python3 - "$run_root" "$repetitions" "${datasets[*]}" <<'PY'
import json
import statistics
import sys
from pathlib import Path

root = Path(sys.argv[1])
repetitions = int(sys.argv[2])
datasets = sys.argv[3].split()

def load(path):
    with path.open(encoding="utf-8") as stream:
        return json.load(stream)

def update(rep, variant, dataset):
    return load(root / f"rep-{rep}" / variant / dataset / "rep-1/osd" / dataset / "update.json")

rows = []
for dataset in datasets:
    for rep in range(1, repetitions + 1):
        control = update(rep, "control", dataset)
        optimized = update(rep, "optimized", dataset)
        quality = load(root / f"rep-{rep}/paired-quality/{dataset}.json")
        rows.append({
            "dataset": dataset,
            "repetition": rep,
            "control": control,
            "optimized": optimized,
            "paired_recall": quality,
            "latency_change": optimized["avg_update_latency_ms"] / control["avg_update_latency_ms"] - 1,
            "p99_change": optimized["p99_update_latency_ms"] / control["p99_update_latency_ms"] - 1,
            "throughput_change": optimized["throughput_updates_per_sec"] / control["throughput_updates_per_sec"] - 1,
        })

dataset_means = []
for dataset in datasets:
    selected = [row for row in rows if row["dataset"] == dataset]
    def mean(variant, key):
        return statistics.mean(row[variant][key] for row in selected)
    control_latency = mean("control", "avg_update_latency_ms")
    optimized_latency = mean("optimized", "avg_update_latency_ms")
    control_p99 = mean("control", "p99_update_latency_ms")
    optimized_p99 = mean("optimized", "p99_update_latency_ms")
    control_throughput = mean("control", "throughput_updates_per_sec")
    optimized_throughput = mean("optimized", "throughput_updates_per_sec")
    quality_pass = all(row["paired_recall"]["status"] == "pass" for row in selected)
    zero_failures = all(
        row[variant]["failed_updates"] == 0
        for row in selected for variant in ("control", "optimized")
    )
    result = {
        "dataset": dataset,
        "control_avg_update_latency_ms": control_latency,
        "optimized_avg_update_latency_ms": optimized_latency,
        "latency_change": optimized_latency / control_latency - 1,
        "control_p99_update_latency_ms": control_p99,
        "optimized_p99_update_latency_ms": optimized_p99,
        "p99_change": optimized_p99 / control_p99 - 1,
        "control_throughput_updates_per_sec": control_throughput,
        "optimized_throughput_updates_per_sec": optimized_throughput,
        "throughput_change": optimized_throughput / control_throughput - 1,
        "quality_pass": quality_pass,
        "zero_failures": zero_failures,
    }
    result["performance_pass"] = result["latency_change"] <= -0.05 and result["p99_change"] <= 0.05
    result["status"] = "pass" if quality_pass and zero_failures and result["performance_pass"] else "fail"
    dataset_means.append(result)

status = "pass" if all(row["status"] == "pass" for row in dataset_means) else "fail"
document = {"status": status, "rows": rows, "dataset_means": dataset_means}
(root / "summary.json").write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")

lines = [
    "# Stage 5A Four-Dataset Formal A/B",
    "",
    f"Status: {status}",
    "",
    "| Dataset | Control latency | Optimized latency | Change | P99 change | Throughput change | Quality | Gate |",
    "| --- | ---: | ---: | ---: | ---: | ---: | --- | --- |",
]
for row in dataset_means:
    lines.append(
        f"| {row['dataset']} | {row['control_avg_update_latency_ms']:.3f} ms | "
        f"{row['optimized_avg_update_latency_ms']:.3f} ms | {row['latency_change'] * 100:+.2f}% | "
        f"{row['p99_change'] * 100:+.2f}% | {row['throughput_change'] * 100:+.2f}% | "
        f"{'pass' if row['quality_pass'] else 'fail'} | {row['status']} |"
    )
(root / "summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
if status != "pass":
    raise SystemExit("Stage 5A formal A/B gate failed; inspect summary.json")
PY
}

{
  echo "stage5a_four_dataset_formal_ab"
  echo "started_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "git_commit=$(git -C "$repo_root" rev-parse HEAD)"
  echo "coordinator_sha256=$coordinator_sha"
  echo "importer_sha256=$importer_sha"
  echo "checker_sha256=$checker_sha"
  echo "cls_sha256=$cls_sha"
  echo "ceph_version=$(ceph --version)"
  echo "ceph_fsid=$(ceph --keyring "$CEPH_KEYRING" fsid)"
  echo "datasets=${datasets[*]}"
  echo "repetitions=$repetitions"
  echo "window_seconds=$window_seconds"
  echo "update_parallelism=$parallelism"
  echo "control=OSD-only M2 with distance microbatch disabled"
  echo "optimized=OSD-only Stage 5A wait 250us executor_threads 5"
  echo "paired_recall_max_regression=$max_regression"
  echo "cache_condition=fresh-pool-after-import (not a controlled cold-cache run)"
} >"$run_root/manifest.txt"

log "Stage 5A four-dataset formal A/B started"
quality_failures=0
for repetition in $(seq 1 "$repetitions"); do
  for dataset in "${datasets[@]}"; do
    cache="$run_root/cache/rep-$repetition/$dataset.hnsw"
    mkdir -p "$(dirname "$cache")"
    if (( repetition % 2 == 1 )); then
      run_case "$repetition" "$dataset" control false build "$cache"
      cache_sha=$(sha "$cache")
      cache_bytes=$(stat -c %s "$cache")
      run_case "$repetition" "$dataset" optimized true load "$cache"
    else
      run_case "$repetition" "$dataset" optimized true build "$cache"
      cache_sha=$(sha "$cache")
      cache_bytes=$(stat -c %s "$cache")
      run_case "$repetition" "$dataset" control false load "$cache"
    fi
    [[ "$(sha "$cache")" == "$cache_sha" ]] || {
      echo "HNSW cache changed within pair: rep=$repetition dataset=$dataset" >&2
      exit 1
    }
    printf 'rep=%s dataset=%s sha256=%s bytes=%s\n' \
      "$repetition" "$dataset" "$cache_sha" "$cache_bytes" >>"$run_root/cache-manifest.txt"
    control="$run_root/rep-$repetition/control/$dataset"
    optimized="$run_root/rep-$repetition/optimized/$dataset"
    paired="$run_root/rep-$repetition/paired-quality/$dataset.json"
    if ! python3 "$compare" "$(case_update "$control" "$dataset")" \
      "$(case_update "$optimized" "$dataset")" --max-regression "$max_regression" \
      --output "$paired"; then
      quality_failures=$((quality_failures + 1))
      log "QUALITY_GATE_FAIL rep=$repetition dataset=$dataset; continuing"
    fi
  done
done

write_summary
log "Stage 5A four-dataset formal A/B passed: $run_root quality_failures=$quality_failures"
