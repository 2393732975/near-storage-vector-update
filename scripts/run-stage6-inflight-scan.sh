#!/usr/bin/env bash
set -euo pipefail

if [[ "${1:-}" != "--confirm-reset" ]]; then
  echo "Refusing to reset experiment pools. Re-run with --confirm-reset." >&2
  exit 2
fi

: "${CEPH_KEYRING:?Set CEPH_KEYRING to a local, untracked Ceph keyring.}"
: "${DATASET_ROOT:?Set DATASET_ROOT to the benchmark dataset directory.}"

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
run_root=${RUN_ROOT:-"$repo_root/results/stage6-inflight-scan-$(date -u +%Y%m%dT%H%M%SZ)"}
dataset=${DATASET:-deep100m}
repetitions=${REPETITIONS:-3}
window_seconds=${WINDOW_SECONDS:-300}
parallelism=${UPDATE_PARALLELISM:-4}
executor_threads=${DISTANCE_EXECUTOR_THREADS:-5}
wait_us=${DISTANCE_MICROBATCH_WAIT_US:-250}
historical_m2_latency_ms=${HISTORICAL_M2_LATENCY_MS:-511.790}
max_regression=${PAIRED_RECALL_MAX_REGRESSION:-0.005}
read -r -a caps <<< "${TARGET_INFLIGHT_CAPS:-0 1 2 4}"
runner="$repo_root/scripts/run-phase1-ab.sh"
compare="$repo_root/scripts/compare-paired-recall.py"

[[ "$dataset" == deep100m ]] || {
  echo "Stage 6 first gate is pinned to DATASET=deep100m." >&2
  exit 2
}
[[ "$repetitions" =~ ^[1-9][0-9]*$ ]] || { echo "REPETITIONS must be positive." >&2; exit 2; }
[[ "$window_seconds" =~ ^[1-9][0-9]*$ ]] || { echo "WINDOW_SECONDS must be positive." >&2; exit 2; }
[[ "$parallelism" =~ ^[1-9][0-9]*$ ]] || { echo "UPDATE_PARALLELISM must be positive." >&2; exit 2; }
[[ "$executor_threads" =~ ^[1-9][0-9]*$ ]] || { echo "DISTANCE_EXECUTOR_THREADS must be positive." >&2; exit 2; }
[[ "$wait_us" =~ ^[0-9]+$ ]] || { echo "DISTANCE_MICROBATCH_WAIT_US must be non-negative." >&2; exit 2; }
(( ${#caps[@]} >= 2 )) || { echo "TARGET_INFLIGHT_CAPS must contain 0 and at least one cap." >&2; exit 2; }

declare -A seen_caps=()
has_control=false
for cap in "${caps[@]}"; do
  [[ "$cap" =~ ^[0-9]+$ ]] || { echo "Invalid target inflight cap: $cap" >&2; exit 2; }
  [[ -z "${seen_caps[$cap]:-}" ]] || { echo "Duplicate target inflight cap: $cap" >&2; exit 2; }
  seen_caps[$cap]=1
  if (( cap == 0 )); then
    has_control=true
  elif (( cap > executor_threads )); then
    echo "Target inflight cap $cap exceeds executor thread count $executor_threads." >&2
    exit 2
  fi
done
[[ "$has_control" == true ]] || { echo "TARGET_INFLIGHT_CAPS must include 0." >&2; exit 2; }

if [[ -e "$run_root" ]]; then
  [[ -d "$run_root" ]] || { echo "RUN_ROOT is not a directory: $run_root" >&2; exit 2; }
  unexpected=$(find "$run_root" -mindepth 1 -maxdepth 1 \
    ! -name runner.log ! -name runner.pid -print -quit)
  [[ -z "$unexpected" ]] || { echo "RUN_ROOT is not empty: $run_root" >&2; exit 2; }
else
  mkdir -p "$run_root"
fi

exec > >(tee -a "$run_root/scan.log") 2>&1
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
    echo "Build artifacts changed during Stage 6; refusing to mix binaries." >&2
    return 1
  }
}

variant_name() {
  if (( $1 == 0 )); then
    echo uncapped
  else
    echo "cap$1"
  fi
}

case_update() {
  local case_root=$1
  echo "$case_root/rep-1/osd/$dataset/update.json"
}

run_case() {
  local repetition=$1 cap=$2 cache_mode=$3 cache=$4
  local variant output
  variant=$(variant_name "$cap")
  output="$run_root/rep-$repetition/$variant"
  verify_binaries
  mkdir -p "$output"
  log "START rep=$repetition variant=$variant cache_mode=$cache_mode"
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
    DISTANCE_MICROBATCH=true \
    DISTANCE_MICROBATCH_WAIT_US="$wait_us" \
    DISTANCE_MICROBATCH_MAX_BYTES=262144 \
    DISTANCE_MICROBATCH_MAX_CANDIDATES=256 \
    DISTANCE_MICROBATCH_MAX_SUBREQUESTS=32 \
    DISTANCE_EXECUTOR_THREADS="$executor_threads" \
    DISTANCE_TARGET_MAX_INFLIGHT="$cap" \
    CHECKER_BATCH_SIZE=8192 \
    SEMANTIC_SAMPLES=256 \
    SEMANTIC_MIN_EDGE_WIN_RATE=0.60 \
    OSD_OP_TIMEOUT_SECONDS=45 \
    OSD_OP_RETRY_LIMIT=3 \
    RECALL_K=10 \
    RUN_ROOT="$output" \
    "$runner" --confirm-reset >"$output/runner.log" 2>&1
  verify_binaries
  log "PASS rep=$repetition variant=$variant"
}

write_summary() {
  python3 - "$run_root" "$repetitions" "${caps[*]}" \
    "$historical_m2_latency_ms" <<'PY'
import json
import statistics
import sys
from pathlib import Path

root = Path(sys.argv[1])
repetitions = int(sys.argv[2])
caps = [int(value) for value in sys.argv[3].split()]
historical_m2_latency_ms = float(sys.argv[4])

def variant(cap):
    return "uncapped" if cap == 0 else f"cap{cap}"

def load(path):
    with path.open(encoding="utf-8") as stream:
        return json.load(stream)

def update(rep, cap):
    return load(root / f"rep-{rep}" / variant(cap) / "rep-1/osd/deep100m/update.json")

rows = []
for rep in range(1, repetitions + 1):
    for cap in caps:
        data = update(rep, cap)
        obs = data["observability"]
        rows.append({
            "repetition": rep,
            "cap": cap,
            "variant": variant(cap),
            "vectors_processed": data["vectors_processed"],
            "failed_updates": data["failed_updates"],
            "rados_exec_retries": data["rados_exec_retries"],
            "avg_update_latency_ms": data["avg_update_latency_ms"],
            "p99_update_latency_ms": data["p99_update_latency_ms"],
            "throughput_updates_per_sec": data["throughput_updates_per_sec"],
            "queue_p95_ms": obs["distance_executor_queue_p95_ms"],
            "queue_p99_ms": obs["distance_executor_queue_p99_ms"],
            "rpc_p95_ms": obs["distance_executor_rpc_p95_ms"],
            "roundtrip_queue_p95_ms": obs["distance_executor_roundtrip_queue_p95_ms"],
            "max_target_inflight": obs["max_distance_target_inflight"],
            "limited_fraction": obs["distance_target_limited_fraction"],
        })

control_rows = [row for row in rows if row["cap"] == 0]
control_latency = statistics.mean(row["avg_update_latency_ms"] for row in control_rows)
control_p99 = statistics.mean(row["p99_update_latency_ms"] for row in control_rows)
control_throughput = statistics.mean(row["throughput_updates_per_sec"] for row in control_rows)
latency_ceiling = historical_m2_latency_ms * 0.95

variants = []
for cap in caps:
    selected = [row for row in rows if row["cap"] == cap]
    result = {
        "cap": cap,
        "variant": variant(cap),
        "avg_update_latency_ms": statistics.mean(row["avg_update_latency_ms"] for row in selected),
        "p99_update_latency_ms": statistics.mean(row["p99_update_latency_ms"] for row in selected),
        "throughput_updates_per_sec": statistics.mean(row["throughput_updates_per_sec"] for row in selected),
        "queue_p95_ms": statistics.mean(row["queue_p95_ms"] for row in selected),
        "queue_p99_ms": statistics.mean(row["queue_p99_ms"] for row in selected),
        "rpc_p95_ms": statistics.mean(row["rpc_p95_ms"] for row in selected),
        "roundtrip_queue_p95_ms": statistics.mean(row["roundtrip_queue_p95_ms"] for row in selected),
        "max_target_inflight": max(row["max_target_inflight"] for row in selected),
        "limited_fraction": statistics.mean(row["limited_fraction"] for row in selected),
        "zero_failures": all(row["failed_updates"] == 0 and row["rados_exec_retries"] == 0 for row in selected),
    }
    result["latency_change_vs_uncapped"] = result["avg_update_latency_ms"] / control_latency - 1
    result["p99_change_vs_uncapped"] = result["p99_update_latency_ms"] / control_p99 - 1
    result["throughput_change_vs_uncapped"] = result["throughput_updates_per_sec"] / control_throughput - 1
    if cap:
        qualities = [
            load(root / f"rep-{rep}/paired-quality/cap{cap}.json")
            for rep in range(1, repetitions + 1)
        ]
        result["quality_pass"] = all(item["status"] == "pass" for item in qualities)
        result["performance_pass"] = (
            result["p99_change_vs_uncapped"] <= 0.0
            and result["avg_update_latency_ms"] <= latency_ceiling
        )
        result["status"] = "pass" if (
            result["quality_pass"] and result["zero_failures"]
            and result["performance_pass"]
        ) else "fail"
    else:
        result["quality_pass"] = True
        result["performance_pass"] = True
        result["status"] = "control"
    variants.append(result)

eligible = [item for item in variants if item["cap"] and item["status"] == "pass"]
selected_cap = min(
    eligible,
    key=lambda item: (item["p99_update_latency_ms"], item["avg_update_latency_ms"]),
    default=None,
)
status = "pass" if selected_cap is not None else "fail"
document = {
    "status": status,
    "historical_m2_latency_ms": historical_m2_latency_ms,
    "latency_ceiling_ms_for_5pct_gain": latency_ceiling,
    "control": {
        "avg_update_latency_ms": control_latency,
        "p99_update_latency_ms": control_p99,
        "throughput_updates_per_sec": control_throughput,
    },
    "selected_cap": selected_cap["cap"] if selected_cap else None,
    "variants": variants,
    "rows": rows,
}
(root / "summary.json").write_text(
    json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8"
)

lines = [
    "# Stage 6 Deep100M Per-Target Inflight Scan",
    "",
    f"Status: {status}",
    "",
    "| Cap | Mean latency | vs uncapped | P99 | vs uncapped | Throughput | Queue P95 | RTT/queue P95 | Limited | Gate |",
    "| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
]
for item in variants:
    cap = "unlimited" if item["cap"] == 0 else str(item["cap"])
    lines.append(
        f"| {cap} | {item['avg_update_latency_ms']:.3f} ms | "
        f"{item['latency_change_vs_uncapped'] * 100:+.2f}% | "
        f"{item['p99_update_latency_ms']:.3f} ms | "
        f"{item['p99_change_vs_uncapped'] * 100:+.2f}% | "
        f"{item['throughput_updates_per_sec']:.3f}/s | "
        f"{item['queue_p95_ms']:.3f} ms | "
        f"{item['roundtrip_queue_p95_ms']:.3f} ms | "
        f"{item['limited_fraction'] * 100:.2f}% | {item['status']} |"
    )
lines.extend([
    "",
    f"Historical M2 mean-latency ceiling for retaining a 5% gain: {latency_ceiling:.3f} ms.",
    f"Selected fixed cap: {selected_cap['cap'] if selected_cap else 'none'}.",
])
(root / "summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
if status != "pass":
    raise SystemExit("Stage 6 fixed-inflight gate failed; inspect summary.json")
PY
}

{
  echo "stage6_deep100m_per_target_inflight_scan"
  echo "started_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "git_commit=$(git -C "$repo_root" rev-parse HEAD)"
  echo "coordinator_sha256=$coordinator_sha"
  echo "importer_sha256=$importer_sha"
  echo "checker_sha256=$checker_sha"
  echo "cls_sha256=$cls_sha"
  echo "ceph_version=$(ceph --version)"
  echo "ceph_fsid=$(ceph --keyring "$CEPH_KEYRING" fsid)"
  echo "dataset=$dataset"
  echo "repetitions=$repetitions"
  echo "window_seconds=$window_seconds"
  echo "update_parallelism=$parallelism"
  echo "distance_executor_threads=$executor_threads"
  echo "distance_microbatch_wait_us=$wait_us"
  echo "target_inflight_caps=${caps[*]}"
  echo "historical_m2_latency_ms=$historical_m2_latency_ms"
  echo "paired_recall_max_regression=$max_regression"
  echo "cache_condition=same frozen HNSW cache within each repetition"
} >"$run_root/manifest.txt"

log "Stage 6 Deep100M inflight scan started"
quality_failures=0
for repetition in $(seq 1 "$repetitions"); do
  cache="$run_root/cache/rep-$repetition/$dataset.hnsw"
  mkdir -p "$(dirname "$cache")"
  offset=$(( (repetition - 1) % ${#caps[@]} ))
  order=()
  for index in $(seq 0 $(( ${#caps[@]} - 1 ))); do
    order+=("${caps[$(( (index + offset) % ${#caps[@]} ))]}")
  done
  log "ORDER rep=$repetition caps=${order[*]}"
  cache_mode=build
  for cap in "${order[@]}"; do
    run_case "$repetition" "$cap" "$cache_mode" "$cache"
    if [[ "$cache_mode" == build ]]; then
      cache_sha=$(sha "$cache")
      cache_bytes=$(stat -c %s "$cache")
      printf 'rep=%s dataset=%s sha256=%s bytes=%s\n' \
        "$repetition" "$dataset" "$cache_sha" "$cache_bytes" \
        >>"$run_root/cache-manifest.txt"
      cache_mode=load
    fi
  done
  [[ "$(sha "$cache")" == "$cache_sha" ]] || {
    echo "HNSW cache changed during repetition $repetition." >&2
    exit 1
  }
  uncapped="$run_root/rep-$repetition/uncapped"
  for cap in "${caps[@]}"; do
    (( cap > 0 )) || continue
    capped="$run_root/rep-$repetition/cap$cap"
    paired="$run_root/rep-$repetition/paired-quality/cap$cap.json"
    if ! python3 "$compare" "$(case_update "$uncapped")" \
      "$(case_update "$capped")" --max-regression "$max_regression" \
      --output "$paired"; then
      quality_failures=$((quality_failures + 1))
      log "QUALITY_GATE_FAIL rep=$repetition cap=$cap; continuing scan"
    fi
  done
done

write_summary
log "Stage 6 Deep100M inflight scan passed: $run_root quality_failures=$quality_failures"
