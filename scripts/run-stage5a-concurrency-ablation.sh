#!/usr/bin/env bash
set -euo pipefail

if [[ "${1:-}" != "--confirm-reset" ]]; then
  echo "Refusing to reset experiment pools. Re-run with --confirm-reset." >&2
  exit 2
fi

: "${CEPH_KEYRING:?Set CEPH_KEYRING to a local, untracked Ceph keyring.}"
: "${DATASET_ROOT:?Set DATASET_ROOT to the benchmark dataset directory.}"

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
run_root=${RUN_ROOT:-"$repo_root/results/stage5a-concurrency-$(date -u +%Y%m%dT%H%M%SZ)"}
window_seconds=${WINDOW_SECONDS:-30}
wait_us=${DISTANCE_MICROBATCH_WAIT_US:-250}
thread_values=${EXECUTOR_THREAD_VALUES:-"1 2 5 10"}
parallelism_values=${UPDATE_PARALLELISM_VALUES:-"1 2 4 8"}
fixed_parallelism=${THREAD_SCAN_UPDATE_PARALLELISM:-4}
max_regression=${PAIRED_RECALL_MAX_REGRESSION:-0.005}
runner="$repo_root/scripts/run-phase1-ab.sh"
compare="$repo_root/scripts/compare-paired-recall.py"

read -r -a threads <<<"$thread_values"
read -r -a parallelisms <<<"$parallelism_values"
(( ${#threads[@]} > 0 )) || { echo "EXECUTOR_THREAD_VALUES must not be empty." >&2; exit 2; }
(( ${#parallelisms[@]} > 0 )) || { echo "UPDATE_PARALLELISM_VALUES must not be empty." >&2; exit 2; }
for value in "${threads[@]}" "$fixed_parallelism" "${parallelisms[@]}"; do
  [[ "$value" =~ ^[1-9][0-9]*$ ]] || {
    echo "Thread and parallelism values must be positive integers: $value" >&2
    exit 2
  }
done
[[ "$window_seconds" =~ ^[1-9][0-9]*$ ]] || { echo "WINDOW_SECONDS must be positive." >&2; exit 2; }
[[ "$wait_us" =~ ^[0-9]+$ ]] || { echo "DISTANCE_MICROBATCH_WAIT_US must be non-negative." >&2; exit 2; }

if [[ -e "$run_root" ]]; then
  [[ -d "$run_root" ]] || { echo "RUN_ROOT is not a directory: $run_root" >&2; exit 2; }
  unexpected=$(find "$run_root" -mindepth 1 -maxdepth 1 \
    ! -name runner.log ! -name runner.pid -print -quit)
  [[ -z "$unexpected" ]] || { echo "RUN_ROOT is not empty: $run_root" >&2; exit 2; }
else
  mkdir -p "$run_root"
fi

exec > >(tee -a "$run_root/ablation.log") 2>&1

log() {
  echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $*"
}

update_json() {
  echo "$1/rep-1/osd/gist1m/update.json"
}

run_case() {
  local group=$1 name=$2 microbatch=$3 executor_threads=$4 parallelism=$5
  local output="$run_root/$group/$name"
  mkdir -p "$output"
  log "START group=$group name=$name microbatch=$microbatch threads=$executor_threads parallelism=$parallelism"
  if ! env \
      CEPH_KEYRING="$CEPH_KEYRING" \
      DATASET_ROOT="$DATASET_ROOT" \
      DATASETS=gist1m \
      MODES=osd \
      REPETITIONS=1 \
      WINDOW_SECONDS="$window_seconds" \
      UPDATE_PARALLELISM="$parallelism" \
      FRONTIER_WINDOW=1 \
      FRONTIER_ADAPTIVE=false \
      DISTANCE_MICROBATCH="$microbatch" \
      DISTANCE_MICROBATCH_WAIT_US="$wait_us" \
      DISTANCE_MICROBATCH_MAX_BYTES=262144 \
      DISTANCE_MICROBATCH_MAX_CANDIDATES=256 \
      DISTANCE_MICROBATCH_MAX_SUBREQUESTS=32 \
      DISTANCE_EXECUTOR_THREADS="$executor_threads" \
      CHECKER_BATCH_SIZE=8192 \
      SEMANTIC_SAMPLES=256 \
      SEMANTIC_MIN_EDGE_WIN_RATE=0.60 \
      OSD_OP_TIMEOUT_SECONDS=45 \
      OSD_OP_RETRY_LIMIT=3 \
      RECALL_K=10 \
      PAIRED_RECALL_MAX_REGRESSION="$max_regression" \
      RUN_ROOT="$output" \
      "$runner" --confirm-reset >"$output/runner.log" 2>&1; then
    tail -80 "$output/runner.log" >&2 || true
    log "FAIL group=$group name=$name"
    return 1
  fi
  log "PASS group=$group name=$name"
}

compare_pair() {
  local control=$1 optimized=$2 output=$3
  set +e
  python3 "$compare" "$(update_json "$control")" "$(update_json "$optimized")" \
    --max-regression "$max_regression" --output "$output"
  local rc=$?
  set -e
  if (( rc != 0 )); then
    log "QUALITY_GATE_FAIL output=$output"
  fi
}

write_manifest() {
  {
    echo "stage5a_concurrency_ablation"
    echo "started_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "git_commit=$(git -C "$repo_root" rev-parse HEAD)"
    echo "ceph_version=$(ceph --version)"
    echo "ceph_fsid=$(ceph --keyring "$CEPH_KEYRING" fsid)"
    echo "dataset=gist1m"
    echo "window_seconds=$window_seconds"
    echo "distance_microbatch_wait_us=$wait_us"
    echo "executor_thread_values=${threads[*]}"
    echo "thread_scan_update_parallelism=$fixed_parallelism"
    echo "update_parallelism_values=${parallelisms[*]}"
    echo "paired_recall_max_regression=$max_regression"
    echo "cache_condition=fresh-pool-after-import (not a controlled cold-cache run)"
  } >"$run_root/manifest.txt"
}

select_thread() {
  python3 - "$run_root" "${threads[@]}" <<'PY'
import json
import sys
from pathlib import Path

root = Path(sys.argv[1])
threads = [int(value) for value in sys.argv[2:]]

def load_update(path):
    with path.open(encoding="utf-8") as stream:
        return json.load(stream)

control = load_update(
    root / "thread-scan" / "control-p4" / "rep-1/osd/gist1m/update.json"
)
rows = []
eligible = []
for threads_count in threads:
    update = load_update(
        root / "thread-scan" / f"threads-{threads_count}" /
        "rep-1/osd/gist1m/update.json"
    )
    with (root / "thread-scan" / f"paired-recall-threads-{threads_count}.json").open(
        encoding="utf-8"
    ) as stream:
        paired = json.load(stream)
    latency_change = update["avg_update_latency_ms"] / control["avg_update_latency_ms"] - 1.0
    p99_change = update["p99_update_latency_ms"] / control["p99_update_latency_ms"] - 1.0
    gate = (
        paired["status"] == "pass"
        and latency_change <= -0.05
        and p99_change <= 0.05
    )
    row = {
        "threads": threads_count,
        "avg_update_latency_ms": update["avg_update_latency_ms"],
        "p99_update_latency_ms": update["p99_update_latency_ms"],
        "throughput_updates_per_sec": update["throughput_updates_per_sec"],
        "latency_change": latency_change,
        "p99_change": p99_change,
        "paired_recall_delta": paired["osd_minus_compute"],
        "gate": "pass" if gate else "fail",
    }
    rows.append(row)
    if gate:
        eligible.append(row)

if not eligible:
    raise SystemExit("no executor-thread configuration passed the Stage 5A gate")
selected = min(eligible, key=lambda row: (row["avg_update_latency_ms"], row["threads"]))
document = {
    "control": {
        "avg_update_latency_ms": control["avg_update_latency_ms"],
        "p99_update_latency_ms": control["p99_update_latency_ms"],
        "throughput_updates_per_sec": control["throughput_updates_per_sec"],
    },
    "candidates": rows,
    "selected_threads": selected["threads"],
    "selection_rule": "lowest mean latency among quality/performance gate passes",
}
(root / "thread-scan" / "selection.json").write_text(
    json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8"
)
print(selected["threads"])
PY
}

write_summary() {
  python3 - "$run_root" "$selected_threads" "${threads[*]}" "${parallelisms[*]}" <<'PY'
import json
import sys
from pathlib import Path

root = Path(sys.argv[1])
selected_threads = int(sys.argv[2])
threads = [int(value) for value in sys.argv[3].split()]
parallelisms = [int(value) for value in sys.argv[4].split()]

def update(path):
    with path.open(encoding="utf-8") as stream:
        return json.load(stream)

def paired(path):
    with path.open(encoding="utf-8") as stream:
        return json.load(stream)

def metrics(document):
    obs = document["observability"]
    return {
        "vectors_processed": document["vectors_processed"],
        "avg_update_latency_ms": document["avg_update_latency_ms"],
        "p99_update_latency_ms": document["p99_update_latency_ms"],
        "throughput_updates_per_sec": document["throughput_updates_per_sec"],
        "cls_calls_per_update": obs["cls_calls_per_update_attempt"],
        "distance_batches_per_update": obs["distance_batches"] / obs["update_attempts"],
        "distance_subrequests_per_rpc": obs["distance_subrequests_per_rpc"],
        "microbatch_queue_ms_per_subrequest": obs["distance_microbatch_queue_ms_per_subrequest"],
    }

thread_control = update(
    root / "thread-scan/control-p4/rep-1/osd/gist1m/update.json"
)
thread_rows = []
for value in threads:
    candidate = update(
        root / f"thread-scan/threads-{value}/rep-1/osd/gist1m/update.json"
    )
    quality = paired(root / f"thread-scan/paired-recall-threads-{value}.json")
    thread_rows.append({
        "threads": value,
        "metrics": metrics(candidate),
        "latency_change": candidate["avg_update_latency_ms"] / thread_control["avg_update_latency_ms"] - 1.0,
        "p99_change": candidate["p99_update_latency_ms"] / thread_control["p99_update_latency_ms"] - 1.0,
        "paired_recall": quality,
    })

parallel_rows = []
for value in parallelisms:
    control = update(
        root / f"parallelism-scan/p{value}-control/rep-1/osd/gist1m/update.json"
    )
    candidate = update(
        root / f"parallelism-scan/p{value}-optimized/rep-1/osd/gist1m/update.json"
    )
    quality = paired(root / f"parallelism-scan/paired-recall-p{value}.json")
    parallel_rows.append({
        "update_parallelism": value,
        "control": metrics(control),
        "optimized": metrics(candidate),
        "latency_change": candidate["avg_update_latency_ms"] / control["avg_update_latency_ms"] - 1.0,
        "p99_change": candidate["p99_update_latency_ms"] / control["p99_update_latency_ms"] - 1.0,
        "throughput_change": candidate["throughput_updates_per_sec"] / control["throughput_updates_per_sec"] - 1.0,
        "paired_recall": quality,
    })

document = {
    "selected_executor_threads": selected_threads,
    "thread_scan_control": metrics(thread_control),
    "thread_scan": thread_rows,
    "parallelism_scan": parallel_rows,
}
(root / "summary.json").write_text(
    json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8"
)

lines = [
    "# Stage 5A Concurrency Ablation Summary",
    "",
    f"Selected executor threads: {selected_threads}",
    "",
    "## Executor thread scan (update parallelism 4)",
    "",
    "| Threads | Avg latency (ms) | P99 (ms) | Throughput | Latency change | Paired Recall delta | Gate |",
    "| ---: | ---: | ---: | ---: | ---: | ---: | --- |",
]
selection = json.loads((root / "thread-scan/selection.json").read_text(encoding="utf-8"))
gates = {row["threads"]: row["gate"] for row in selection["candidates"]}
for row in thread_rows:
    item = row["metrics"]
    lines.append(
        f"| {row['threads']} | {item['avg_update_latency_ms']:.3f} | "
        f"{item['p99_update_latency_ms']:.3f} | {item['throughput_updates_per_sec']:.3f} | "
        f"{row['latency_change'] * 100:+.2f}% | "
        f"{row['paired_recall']['osd_minus_compute'] * 100:+.3f} pp | "
        f"{gates[row['threads']]} |"
    )
lines.extend([
    "",
    "## Update parallelism scan",
    "",
    "| Parallelism | Control latency | Optimized latency | Change | Control throughput | Optimized throughput | Change | subrequests/RPC | Paired Recall delta |",
    "| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
])
for row in parallel_rows:
    control = row["control"]
    optimized = row["optimized"]
    lines.append(
        f"| {row['update_parallelism']} | {control['avg_update_latency_ms']:.3f} ms | "
        f"{optimized['avg_update_latency_ms']:.3f} ms | {row['latency_change'] * 100:+.2f}% | "
        f"{control['throughput_updates_per_sec']:.3f} | {optimized['throughput_updates_per_sec']:.3f} | "
        f"{row['throughput_change'] * 100:+.2f}% | "
        f"{optimized['distance_subrequests_per_rpc']:.3f} | "
        f"{row['paired_recall']['osd_minus_compute'] * 100:+.3f} pp |"
    )
(root / "summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
PY
}

write_manifest
log "Stage 5A concurrency ablation started"

control_thread_dir="$run_root/thread-scan/control-p4"
run_case thread-scan control-p4 false 5 "$fixed_parallelism"
for value in "${threads[@]}"; do
  candidate_dir="$run_root/thread-scan/threads-$value"
  run_case thread-scan "threads-$value" true "$value" "$fixed_parallelism"
  compare_pair "$control_thread_dir" "$candidate_dir" \
    "$run_root/thread-scan/paired-recall-threads-$value.json"
done

selected_threads=$(select_thread)
echo "selected_executor_threads=$selected_threads" >>"$run_root/manifest.txt"
log "Selected executor threads: $selected_threads"

index=0
for value in "${parallelisms[@]}"; do
  control_dir="$run_root/parallelism-scan/p$value-control"
  optimized_dir="$run_root/parallelism-scan/p$value-optimized"
  if (( index % 2 == 0 )); then
    run_case parallelism-scan "p$value-control" false "$selected_threads" "$value"
    run_case parallelism-scan "p$value-optimized" true "$selected_threads" "$value"
  else
    run_case parallelism-scan "p$value-optimized" true "$selected_threads" "$value"
    run_case parallelism-scan "p$value-control" false "$selected_threads" "$value"
  fi
  compare_pair "$control_dir" "$optimized_dir" \
    "$run_root/parallelism-scan/paired-recall-p$value.json"
  index=$((index + 1))
done

write_summary
log "Stage 5A concurrency ablation complete: $run_root"
