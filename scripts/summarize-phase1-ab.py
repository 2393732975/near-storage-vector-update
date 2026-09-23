#!/usr/bin/env python3
"""Summarize independent phase-1 compute/OSD A/B repetitions."""

import argparse
import json
import math
import statistics
from collections import defaultdict
from pathlib import Path


T_CRITICAL_95 = {
    1: 12.706,
    2: 4.303,
    3: 3.182,
    4: 2.776,
    5: 2.571,
    6: 2.447,
    7: 2.365,
    8: 2.306,
    9: 2.262,
    10: 2.228,
    11: 2.201,
    12: 2.179,
    13: 2.160,
    14: 2.145,
    15: 2.131,
    16: 2.120,
    17: 2.110,
    18: 2.101,
    19: 2.093,
    20: 2.086,
    21: 2.080,
    22: 2.074,
    23: 2.069,
    24: 2.064,
    25: 2.060,
    26: 2.056,
    27: 2.052,
    28: 2.048,
    29: 2.045,
    30: 2.042,
}


def nested(data, *keys, default=0.0):
    value = data
    for key in keys:
        if not isinstance(value, dict) or key not in value:
            return default
        value = value[key]
    return value


def ratio(numerator, denominator):
    return float(numerator) / float(denominator) if denominator else 0.0


def describe(values):
    if not values:
        return {"n": 0, "mean": 0.0, "stdev": 0.0, "ci95": 0.0}
    mean = statistics.fmean(values)
    if len(values) == 1:
        return {"n": 1, "mean": mean, "stdev": 0.0, "ci95": 0.0}
    stdev = statistics.stdev(values)
    degrees = len(values) - 1
    critical = T_CRITICAL_95.get(degrees, 1.960)
    return {
        "n": len(values),
        "mean": mean,
        "stdev": stdev,
        "ci95": critical * stdev / math.sqrt(len(values)),
    }


def load_runs(root):
    runs = []
    for path in sorted(root.glob("rep-*/*/*/update.json")):
        relative = path.relative_to(root).parts
        if len(relative) != 4:
            continue
        repetition, mode, dataset, _ = relative
        check_path = path.with_name("index-check.json")
        if not check_path.exists():
            raise ValueError(f"missing consistency result for {path}")
        update = json.loads(path.read_text(encoding="utf-8"))
        check = json.loads(check_path.read_text(encoding="utf-8"))
        vectors = update.get("vectors_processed", 0)
        attempts = nested(update, "observability", "update_attempts", default=0)
        runs.append(
            {
                "repetition": repetition,
                "mode": mode,
                "dataset": dataset,
                "path": str(path),
                "failed_updates": update.get("failed_updates", 0),
                "checker_status": check.get("status", "missing"),
                "checker_errors": check.get("errors", 0),
                "vectors_processed": vectors,
                "throughput": update.get("throughput_updates_per_sec", 0.0),
                "avg_latency_ms": update.get("avg_update_latency_ms", 0.0),
                "p99_latency_ms": update.get("p99_update_latency_ms", 0.0),
                "recall_at_k": nested(
                    update, "quality", "precommit_static_recall_at_k", default=0.0
                ),
                "cls_calls_per_update": ratio(
                    nested(update, "observability", "total_cls_exec_calls", default=0),
                    attempts,
                ),
                "distance_batches_per_update": ratio(
                    nested(update, "observability", "distance_batches", default=0),
                    attempts,
                ),
                "candidates_per_distance_batch": nested(
                    update,
                    "observability",
                    "candidates_per_distance_batch",
                    default=0.0,
                ),
                "patch_calls_per_update": ratio(
                    update.get("remote_patch_calls", 0), attempts
                ),
                "logical_query_kib_per_update": ratio(
                    nested(
                        update,
                        "observability",
                        "logical_distance_query_bytes",
                        default=0,
                    ),
                    vectors * 1024,
                ),
                "cls_kib_per_update": ratio(
                    nested(update, "observability", "cls_request_bytes", default=0)
                    + nested(update, "observability", "cls_reply_bytes", default=0),
                    vectors * 1024,
                ),
            }
        )
    if not runs:
        raise ValueError(f"no phase-1 update.json files found below {root}")
    return runs


def aggregate(runs):
    groups = defaultdict(list)
    for run in runs:
        groups[(run["dataset"], run["mode"])].append(run)
    metric_names = (
        "throughput",
        "avg_latency_ms",
        "p99_latency_ms",
        "recall_at_k",
        "cls_calls_per_update",
        "distance_batches_per_update",
        "candidates_per_distance_batch",
        "patch_calls_per_update",
        "logical_query_kib_per_update",
        "cls_kib_per_update",
    )
    output = {}
    for (dataset, mode), items in sorted(groups.items()):
        stats = {
            metric: describe([float(item[metric]) for item in items])
            for metric in metric_names
        }
        stats["valid_runs"] = sum(
            item["failed_updates"] == 0
            and item["checker_status"] == "pass"
            and item["checker_errors"] == 0
            for item in items
        )
        stats["runs"] = len(items)
        output.setdefault(dataset, {})[mode] = stats
    return output


def fmt(stats, digits=3):
    return f'{stats["mean"]:.{digits}f} ± {stats["ci95"]:.{digits}f}'


def percent_change(osd, compute):
    return (osd / compute - 1.0) * 100.0 if compute else 0.0


def write_markdown(root, aggregate_data, output):
    lines = [
        "# 阶段 1 Compute/OSD 严格 A/B 汇总",
        "",
        f"结果目录：`{root}`。误差项为独立重复样本均值的 95% Student-t 置信区间。",
        "只有 `failed_updates=0` 且一致性检查通过的完整运行才具备正式比较资格。",
        "",
        "## 延迟、吞吐与质量",
        "",
        "| 数据集 | 模式 | 有效轮次 | 吞吐 (update/s) | 平均延迟 (ms) | P99 (ms) | 静态 Recall@K |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for dataset, modes in aggregate_data.items():
        for mode in ("compute", "osd"):
            if mode not in modes:
                continue
            values = modes[mode]
            lines.append(
                f'| {dataset} | {mode} | {values["valid_runs"]}/{values["runs"]} '
                f'| {fmt(values["throughput"])} | {fmt(values["avg_latency_ms"])} '
                f'| {fmt(values["p99_latency_ms"])} | {fmt(values["recall_at_k"], 4)} |'
            )

    lines.extend(
        [
            "",
            "## OSD 相对 Compute",
            "",
            "| 数据集 | 平均延迟变化 | P99 变化 | 吞吐变化 | Recall 差值 (pp) |",
            "| --- | ---: | ---: | ---: | ---: |",
        ]
    )
    for dataset, modes in aggregate_data.items():
        if "compute" not in modes or "osd" not in modes:
            continue
        compute = modes["compute"]
        osd = modes["osd"]
        lines.append(
            f'| {dataset} '
            f'| {percent_change(osd["avg_latency_ms"]["mean"], compute["avg_latency_ms"]["mean"]):+.2f}% '
            f'| {percent_change(osd["p99_latency_ms"]["mean"], compute["p99_latency_ms"]["mean"]):+.2f}% '
            f'| {percent_change(osd["throughput"]["mean"], compute["throughput"]["mean"]):+.2f}% '
            f'| {(osd["recall_at_k"]["mean"] - compute["recall_at_k"]["mean"]) * 100:+.3f} |'
        )

    lines.extend(
        [
            "",
            "## 调用形态与数据移动",
            "",
            "| 数据集 | 模式 | CLS calls/update | distance batches/update | candidates/batch | query KiB/update | CLS KiB/update | patch calls/update |",
            "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for dataset, modes in aggregate_data.items():
        for mode in ("compute", "osd"):
            if mode not in modes:
                continue
            values = modes[mode]
            lines.append(
                f'| {dataset} | {mode} | {values["cls_calls_per_update"]["mean"]:.2f} '
                f'| {values["distance_batches_per_update"]["mean"]:.2f} '
                f'| {values["candidates_per_distance_batch"]["mean"]:.3f} '
                f'| {values["logical_query_kib_per_update"]["mean"]:.2f} '
                f'| {values["cls_kib_per_update"]["mean"]:.2f} '
                f'| {values["patch_calls_per_update"]["mean"]:.2f} |'
            )

    lines.extend(
        [
            "",
            "## 口径限制",
            "",
            "- Recall 是更新提交前的 level-0 搜索结果相对原始 base ground truth 的静态门槛，"
            "不是更新后动态语料库的精确 Recall。",
            "- 每次测量都重新建池并导入 base，但导入本身会预热客户端和 OSD 缓存；"
            "本结果应标记为 `fresh-pool-after-import`，不能称为受控冷缓存实验。",
            "- 非距离 CLS 尚未返回服务端内部耗时；同步 baseline 也不存在可记录的微批等待。",
            "",
        ]
    )
    output.write_text("\n".join(lines), encoding="utf-8")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("results_root", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--json-output", type=Path)
    args = parser.parse_args()

    runs = load_runs(args.results_root)
    aggregate_data = aggregate(runs)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    write_markdown(args.results_root, aggregate_data, args.output)
    if args.json_output:
        args.json_output.write_text(
            json.dumps(
                {"results_root": str(args.results_root), "runs": runs, "aggregate": aggregate_data},
                indent=2,
                ensure_ascii=False,
            )
            + "\n",
            encoding="utf-8",
        )
    print(args.output)


if __name__ == "__main__":
    main()
