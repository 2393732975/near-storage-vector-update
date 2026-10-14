#!/usr/bin/env bash
set -euo pipefail

if [[ "${1:-}" != "--confirm-reset" ]]; then
  echo "Refusing to reset experiment pools. Re-run with --confirm-reset." >&2
  exit 2
fi

: "${CEPH_KEYRING:?Set CEPH_KEYRING to a local, untracked Ceph keyring.}"
: "${DATASET_ROOT:?Set DATASET_ROOT to the benchmark dataset directory.}"

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
run_root=${RUN_ROOT:-"$repo_root/results/stage5a-fixed-trace-$(date -u +%Y%m%dT%H%M%SZ)"}
fixed_updates=${FIXED_NUM_UPDATES:-200}
parallel_repetitions=${PARALLEL_REPETITIONS:-3}
max_regression=${PAIRED_RECALL_MAX_REGRESSION:-0.005}
runner="$repo_root/scripts/run-phase1-ab.sh"
compare="$repo_root/scripts/compare-paired-recall.py"
index_cache="$run_root/cache/gist1m.hnsw"
index_cache_sha=""

[[ "$fixed_updates" =~ ^[1-9][0-9]*$ ]] || {
  echo "FIXED_NUM_UPDATES must be positive." >&2
  exit 2
}
[[ "$parallel_repetitions" =~ ^[1-9][0-9]*$ ]] || {
  echo "PARALLEL_REPETITIONS must be positive." >&2
  exit 2
}

if [[ -e "$run_root" ]]; then
  [[ -d "$run_root" ]] || { echo "RUN_ROOT is not a directory: $run_root" >&2; exit 2; }
  unexpected=$(find "$run_root" -mindepth 1 -maxdepth 1 \
    ! -name runner.log ! -name runner.pid -print -quit)
  [[ -z "$unexpected" ]] || { echo "RUN_ROOT is not empty: $run_root" >&2; exit 2; }
else
  mkdir -p "$run_root"
fi

exec > >(tee -a "$run_root/fixed-trace.log") 2>&1
mkdir -p "$run_root/cache"

log() {
  echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $*"
}

update_json() {
  echo "$1/rep-1/osd/gist1m/update.json"
}

run_case() {
  local output=$1 microbatch=$2 parallelism=$3 cache_mode=$4
  if [[ "$cache_mode" == load ]]; then
    [[ -s "$index_cache" && -n "$index_cache_sha" ]] || {
      echo "HNSW index cache is unavailable before load case: $index_cache" >&2
      return 1
    }
    [[ "$(sha256sum "$index_cache" | awk '{print $1}')" == "$index_cache_sha" ]] || {
      echo "HNSW index cache changed before load case: $index_cache" >&2
      return 1
    }
  fi
  mkdir -p "$output"
  log "START output=$output microbatch=$microbatch parallelism=$parallelism updates=$fixed_updates cache_mode=$cache_mode"
  env \
    CEPH_KEYRING="$CEPH_KEYRING" \
    DATASET_ROOT="$DATASET_ROOT" \
    DATASETS=gist1m \
    MODES=osd \
    REPETITIONS=1 \
    WINDOW_SECONDS=0 \
    FIXED_NUM_UPDATES="$fixed_updates" \
    HNSW_INDEX_CACHE="$index_cache" \
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
  if [[ "$cache_mode" == load &&
        "$(sha256sum "$index_cache" | awk '{print $1}')" != "$index_cache_sha" ]]; then
    echo "HNSW index cache changed during load case: $index_cache" >&2
    return 1
  fi
  log "PASS output=$output"
}

compare_recall() {
  local control=$1 optimized=$2 output=$3
  python3 "$compare" "$(update_json "$control")" "$(update_json "$optimized")" \
    --expected-queries "$fixed_updates" --max-regression "$max_regression" \
    --output "$output"
}

write_manifest() {
  {
    echo "stage5a_fixed_trace_gate"
    echo "started_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "git_commit=$(git -C "$repo_root" rev-parse HEAD)"
    echo "coordinator_sha256=$(sha256sum "$repo_root/build/nsvu-update-coordinator" | awk '{print $1}')"
    echo "importer_sha256=$(sha256sum "$repo_root/build/nsvu-base-importer" | awk '{print $1}')"
    echo "checker_sha256=$(sha256sum "$repo_root/build/nsvu-index-checker" | awk '{print $1}')"
    echo "cls_sha256=$(sha256sum "$repo_root/build/libcls_hnsw_global.so" | awk '{print $1}')"
    echo "ceph_version=$(ceph --version)"
    echo "ceph_fsid=$(ceph --keyring "$CEPH_KEYRING" fsid)"
    echo "dataset=gist1m"
    echo "fixed_num_updates=$fixed_updates"
    echo "parallel_repetitions=$parallel_repetitions"
    echo "paired_recall_max_regression=$max_regression"
    echo "deterministic_parallelism=1"
    echo "performance_parallelism=4"
    echo "distance_microbatch_wait_us=250"
    echo "distance_executor_threads=5"
    echo "cache_condition=fresh-pool-after-import (not a controlled cold-cache run)"
  } >"$run_root/manifest.txt"
}

write_summary() {
  python3 - "$run_root" "$fixed_updates" "$parallel_repetitions" <<'PY'
import json
import statistics
import sys
from pathlib import Path

root = Path(sys.argv[1])
fixed_updates = int(sys.argv[2])
repetitions = int(sys.argv[3])

def load(path):
    with path.open(encoding="utf-8") as stream:
        return json.load(stream)

def update(case):
    return load(case / "rep-1/osd/gist1m/update.json")

serial_control = update(root / "deterministic/control")
serial_optimized = update(root / "deterministic/optimized")
exact = load(root / "deterministic/exact-trace.json")

parallel_rows = []
for repetition in range(1, repetitions + 1):
    pair = root / f"parallel/rep-{repetition}"
    control = update(pair / "control")
    optimized = update(pair / "optimized")
    quality = load(pair / "paired-recall.json")
    parallel_rows.append({
        "repetition": repetition,
        "control": control,
        "optimized": optimized,
        "paired_recall": quality,
        "latency_change": optimized["avg_update_latency_ms"] / control["avg_update_latency_ms"] - 1.0,
        "p99_change": optimized["p99_update_latency_ms"] / control["p99_update_latency_ms"] - 1.0,
        "throughput_change": optimized["throughput_updates_per_sec"] / control["throughput_updates_per_sec"] - 1.0,
    })

all_updates = [serial_control, serial_optimized]
for row in parallel_rows:
    all_updates.extend((row["control"], row["optimized"]))
if any(item["vectors_processed"] != fixed_updates for item in all_updates):
    raise SystemExit("fixed-update count mismatch")
if any(item["failed_updates"] != 0 for item in all_updates):
    raise SystemExit("one or more fixed-trace runs reported failed updates")

mean_control_latency = statistics.mean(
    row["control"]["avg_update_latency_ms"] for row in parallel_rows
)
mean_optimized_latency = statistics.mean(
    row["optimized"]["avg_update_latency_ms"] for row in parallel_rows
)
mean_control_p99 = statistics.mean(
    row["control"]["p99_update_latency_ms"] for row in parallel_rows
)
mean_optimized_p99 = statistics.mean(
    row["optimized"]["p99_update_latency_ms"] for row in parallel_rows
)
mean_control_throughput = statistics.mean(
    row["control"]["throughput_updates_per_sec"] for row in parallel_rows
)
mean_optimized_throughput = statistics.mean(
    row["optimized"]["throughput_updates_per_sec"] for row in parallel_rows
)
mean_latency_change = mean_optimized_latency / mean_control_latency - 1.0
mean_p99_change = mean_optimized_p99 / mean_control_p99 - 1.0
mean_throughput_change = mean_optimized_throughput / mean_control_throughput - 1.0
quality_passed = all(row["paired_recall"]["status"] == "pass" for row in parallel_rows)
performance_passed = mean_latency_change <= -0.05 and mean_p99_change <= 0.05
gate_passed = exact["status"] == "pass" and quality_passed and performance_passed

summary = {
    "status": "pass" if gate_passed else "fail",
    "fixed_num_updates": fixed_updates,
    "deterministic_exact_trace": exact,
    "deterministic_control": serial_control,
    "deterministic_optimized": serial_optimized,
    "parallel_rows": parallel_rows,
    "parallel_mean": {
        "control_avg_update_latency_ms": mean_control_latency,
        "optimized_avg_update_latency_ms": mean_optimized_latency,
        "latency_change": mean_latency_change,
        "control_p99_update_latency_ms": mean_control_p99,
        "optimized_p99_update_latency_ms": mean_optimized_p99,
        "p99_change": mean_p99_change,
        "control_throughput_updates_per_sec": mean_control_throughput,
        "optimized_throughput_updates_per_sec": mean_optimized_throughput,
        "throughput_change": mean_throughput_change,
    },
    "gates": {
        "deterministic_exact_trace": exact["status"] == "pass",
        "parallel_quality": quality_passed,
        "parallel_performance": performance_passed,
    },
}
(root / "summary.json").write_text(
    json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
)

lines = [
    "# Stage 5A Fixed-Trace Quality Gate",
    "",
    f"Status: {summary['status']}",
    "",
    f"Fixed updates per run: {fixed_updates}",
    "",
    "## Deterministic parallelism-1 gate",
    "",
    f"- Exact sample trace: {exact['status']}",
    f"- Exact mismatches: {exact['exact_mismatch_count']}",
    f"- Recall delta: {exact['osd_minus_compute'] * 100:+.3f} pp",
    "",
    "## Parallelism-4 repetitions",
    "",
    "| Rep | Control latency | Optimized latency | Change | P99 change | Throughput change | Recall delta | Gate |",
    "| ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
]
for row in parallel_rows:
    quality = row["paired_recall"]
    lines.append(
        f"| {row['repetition']} | {row['control']['avg_update_latency_ms']:.3f} ms | "
        f"{row['optimized']['avg_update_latency_ms']:.3f} ms | "
        f"{row['latency_change'] * 100:+.2f}% | {row['p99_change'] * 100:+.2f}% | "
        f"{row['throughput_change'] * 100:+.2f}% | "
        f"{quality['osd_minus_compute'] * 100:+.3f} pp | {quality['status']} |"
    )
lines.extend([
    "",
    "## Three-run mean",
    "",
    f"- Latency: {mean_control_latency:.3f} -> {mean_optimized_latency:.3f} ms ({mean_latency_change * 100:+.2f}%)",
    f"- P99: {mean_control_p99:.3f} -> {mean_optimized_p99:.3f} ms ({mean_p99_change * 100:+.2f}%)",
    f"- Throughput: {mean_control_throughput:.3f} -> {mean_optimized_throughput:.3f} update/s ({mean_throughput_change * 100:+.2f}%)",
    f"- Deterministic trace gate: {'pass' if exact['status'] == 'pass' else 'fail'}",
    f"- Parallel quality gate: {'pass' if quality_passed else 'fail'}",
    f"- Parallel performance gate: {'pass' if performance_passed else 'fail'}",
])
(root / "summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
if not gate_passed:
    raise SystemExit("Stage 5A fixed-trace gate failed; inspect summary.json")
PY
}

write_manifest
log "Stage 5A fixed-trace gate started"

run_case "$run_root/deterministic/control" false 1 build
index_cache_sha=$(sha256sum "$index_cache" | awk '{print $1}')
echo "hnsw_index_cache_sha256=$index_cache_sha" >>"$run_root/manifest.txt"
echo "hnsw_index_cache_bytes=$(stat -c %s "$index_cache")" >>"$run_root/manifest.txt"
run_case "$run_root/deterministic/optimized" true 1 load
python3 "$compare" \
  "$(update_json "$run_root/deterministic/control")" \
  "$(update_json "$run_root/deterministic/optimized")" \
  --expected-queries "$fixed_updates" --max-regression 0 \
  --require-exact-samples --output "$run_root/deterministic/exact-trace.json"

quality_failures=0
for repetition in $(seq 1 "$parallel_repetitions"); do
  pair="$run_root/parallel/rep-$repetition"
  if (( repetition % 2 == 1 )); then
    run_case "$pair/control" false 4 load
    run_case "$pair/optimized" true 4 load
  else
    run_case "$pair/optimized" true 4 load
    run_case "$pair/control" false 4 load
  fi
  if ! compare_recall "$pair/control" "$pair/optimized" "$pair/paired-recall.json"; then
    quality_failures=$((quality_failures + 1))
    log "QUALITY_GATE_FAIL repetition=$repetition; continuing remaining repetitions"
  fi
done

write_summary
log "Stage 5A fixed-trace gate passed: $run_root"
