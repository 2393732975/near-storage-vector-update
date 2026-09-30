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
        samples[query_index] = (hits, denominator)
    return samples, int(quality.get("recall_k", 0))


def compare(compute_path: Path, osd_path: Path, max_delta: float):
    compute, compute_k = load_samples(compute_path)
    osd, osd_k = load_samples(osd_path)
    if compute_k <= 0 or compute_k != osd_k:
        raise ValueError(f"Recall K mismatch: compute={compute_k}, osd={osd_k}")

    prefix_length = 0
    while prefix_length in compute and prefix_length in osd:
        prefix_length += 1
    if prefix_length == 0:
        raise ValueError("runs have no shared query prefix beginning at query index 0")

    compute_hits = sum(compute[index][0] for index in range(prefix_length))
    compute_denominator = sum(compute[index][1] for index in range(prefix_length))
    osd_hits = sum(osd[index][0] for index in range(prefix_length))
    osd_denominator = sum(osd[index][1] for index in range(prefix_length))
    if compute_denominator != osd_denominator:
        raise ValueError(
            "shared-prefix Recall denominators differ: "
            f"compute={compute_denominator}, osd={osd_denominator}"
        )

    compute_recall = compute_hits / compute_denominator
    osd_recall = osd_hits / osd_denominator
    delta = osd_recall - compute_recall
    return {
        "status": "pass" if delta + max_delta >= -1e-12 else "fail",
        "recall_k": compute_k,
        "shared_prefix_queries": prefix_length,
        "denominator": compute_denominator,
        "compute_hits": compute_hits,
        "osd_hits": osd_hits,
        "compute_recall": compute_recall,
        "osd_recall": osd_recall,
        "osd_minus_compute": delta,
        "max_allowed_regression": max_delta,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("compute_update_json", type=Path)
    parser.add_argument("osd_update_json", type=Path)
    parser.add_argument("--max-regression", type=float, default=0.005)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if not 0.0 <= args.max_regression <= 1.0:
        parser.error("--max-regression must be in [0, 1]")

    result = compare(
        args.compute_update_json, args.osd_update_json, args.max_regression
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
