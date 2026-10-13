#!/usr/bin/env python3
"""Compare Recall@K on the longest shared query prefix of two update runs."""

import argparse
import json
from pathlib import Path


def load_samples(path: Path):
    with path.open(encoding="utf-8") as stream:
        document = json.load(stream)
    quality = document.get("quality", {})
    samples = {}
    for sample in quality.get("paired_query_samples", []):
        query_index = int(sample["query_index"])
        denominator = int(sample["denominator"])
        hits = int(sample["hits"])
        if denominator <= 0 or hits < 0 or hits > denominator:
            raise ValueError(f"invalid Recall sample for query {query_index} in {path}")
        if query_index in samples:
            raise ValueError(f"duplicate query index {query_index} in {path}")
        samples[query_index] = {
            "hits": hits,
            "denominator": denominator,
            "update_sequence": int(sample.get("update_sequence", query_index)),
            "target_label": sample.get("target_label"),
            "inserted_global_id": sample.get("inserted_global_id"),
            "inserted_level": sample.get("inserted_level"),
            "candidate_hash": sample.get("candidate_hash"),
        }
    return samples, int(quality.get("recall_k", 0))


def compare(
    compute_path: Path,
    osd_path: Path,
    max_delta: float,
    expected_queries=None,
    require_exact_samples=False,
):
    compute, compute_k = load_samples(compute_path)
    osd, osd_k = load_samples(osd_path)
    if compute_k <= 0 or compute_k != osd_k:
        raise ValueError(f"Recall K mismatch: compute={compute_k}, osd={osd_k}")

    prefix_length = 0
    while prefix_length in compute and prefix_length in osd:
        prefix_length += 1
    if prefix_length == 0:
        raise ValueError("runs have no shared query prefix beginning at query index 0")
    if expected_queries is not None and prefix_length != expected_queries:
        raise ValueError(
            "shared query prefix does not match the fixed-update gate: "
            f"expected={expected_queries}, actual={prefix_length}"
        )

    compute_hits = sum(compute[index]["hits"] for index in range(prefix_length))
    compute_denominator = sum(
        compute[index]["denominator"] for index in range(prefix_length)
    )
    osd_hits = sum(osd[index]["hits"] for index in range(prefix_length))
    osd_denominator = sum(osd[index]["denominator"] for index in range(prefix_length))
    if compute_denominator != osd_denominator:
        raise ValueError(
            "shared-prefix Recall denominators differ: "
            f"compute={compute_denominator}, osd={osd_denominator}"
        )

    compute_recall = compute_hits / compute_denominator
    osd_recall = osd_hits / osd_denominator
    delta = osd_recall - compute_recall
    trace_fields = (
        "update_sequence",
        "target_label",
        "inserted_global_id",
        "inserted_level",
        "candidate_hash",
        "hits",
        "denominator",
    )
    trace_mismatches = []
    if require_exact_samples:
        for query_index in range(prefix_length):
            left = compute[query_index]
            right = osd[query_index]
            missing = [
                field
                for field in trace_fields[1:5]
                if left.get(field) is None or right.get(field) is None
            ]
            if missing:
                raise ValueError(
                    f"query {query_index} lacks exact-trace fields: {missing}"
                )
            if any(left[field] != right[field] for field in trace_fields):
                trace_mismatches.append(query_index)
    recall_gate_passed = delta + max_delta >= -1e-12
    exact_gate_passed = not require_exact_samples or not trace_mismatches
    return {
        "status": "pass" if recall_gate_passed and exact_gate_passed else "fail",
        "recall_k": compute_k,
        "shared_prefix_queries": prefix_length,
        "expected_queries": expected_queries,
        "denominator": compute_denominator,
        "compute_hits": compute_hits,
        "osd_hits": osd_hits,
        "compute_recall": compute_recall,
        "osd_recall": osd_recall,
        "osd_minus_compute": delta,
        "max_allowed_regression": max_delta,
        "recall_gate_passed": recall_gate_passed,
        "exact_samples_required": require_exact_samples,
        "exact_samples_match": exact_gate_passed,
        "exact_mismatch_count": len(trace_mismatches),
        "exact_mismatch_queries": trace_mismatches[:20],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("compute_update_json", type=Path)
    parser.add_argument("osd_update_json", type=Path)
    parser.add_argument("--max-regression", type=float, default=0.005)
    parser.add_argument("--expected-queries", type=int)
    parser.add_argument("--require-exact-samples", action="store_true")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if not 0.0 <= args.max_regression <= 1.0:
        parser.error("--max-regression must be in [0, 1]")
    if args.expected_queries is not None and args.expected_queries <= 0:
        parser.error("--expected-queries must be positive")

    result = compare(
        args.compute_update_json,
        args.osd_update_json,
        args.max_regression,
        expected_queries=args.expected_queries,
        require_exact_samples=args.require_exact_samples,
    )
    rendered = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    if result["status"] != "pass":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
