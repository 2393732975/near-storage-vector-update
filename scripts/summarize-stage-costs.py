#!/usr/bin/env python3
"""Render PPT-aligned update cost shares from coordinator metrics JSON files."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable


DATASETS = ("gist1m", "text2image10m", "deep100m", "sift100m")
MODES = ("osd", "compute")


def value(profile: dict, name: str) -> float:
    return float(profile.get(name, 0.0))


def percent(numerator: float, denominator: float) -> str:
    return "n/a" if denominator <= 0 else f"{numerator * 100 / denominator:.1f}%"


def rows(root: Path) -> Iterable[tuple[str, str, dict, dict]]:
    for mode in MODES:
        for dataset in DATASETS:
            path = root / mode / dataset / "update.json"
            if not path.is_file():
                continue
            with path.open(encoding="utf-8") as metrics_file:
                metrics = json.load(metrics_file)
            yield mode, dataset, metrics, metrics.get("stage_profile", {})


def build_report(root: Path) -> str:
    measurements = list(rows(root))
    if not measurements:
        raise ValueError(f"No update.json files found below {root}")

    lines = [
        "# 更新阶段开销监测",
        "",
        f"输入目录：`{root}`。",
        "",
        "## 主路径累计工作时间占比",
        "",
        "分母为 `accounted_update_seconds`，即各 worker 的不重叠主路径阶段累计时间。"
        "它适用于并发更新；不要使用直接除以 `graph_seconds` 的旧 `_pct` 字段，"
        "后者在多 worker 下会因累计时间超过墙钟时间而大于 100%。",
        "",
        "| 路径 | 数据集 | 已处理/失败 | 远端距离 | 全局 meta | 邻接 patch | 图搜索控制 | stale | 查旧点 |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for mode, dataset, metrics, profile in measurements:
        denominator = value(profile, "accounted_update_seconds")
        processed = int(metrics.get("vectors_processed", 0))
        failed = int(metrics.get("failed_updates", 0))
        cells = [
            mode,
            dataset,
            f"{processed}/{failed}",
            percent(value(profile, "remote_distance_seconds"), denominator),
            percent(value(profile, "global_meta_update_seconds"), denominator),
            percent(value(profile, "adjacency_patch_exclusive_seconds"), denominator),
            percent(value(profile, "full_graph_search_exclusive_seconds"), denominator),
            percent(value(profile, "mark_stale_seconds"), denominator),
            percent(value(profile, "lookup_old_seconds"), denominator),
        ]
        lines.append("| " + " | ".join(cells) + " |")

    lines += [
        "",
        "## 距离路径内部开销",
        "",
        "分母为 Coordinator 观测到的 `remote_distance_seconds`。OSD 路径把 CLS 执行"
        "拆为 OMAP 引用读取、payload 读取与纯距离计算；计算节点路径拆为向量 RPC"
        "与本地计算。OSD 的 `RTT/排队`是端到端距离调用扣除 CLS 执行后的剩余时间，"
        "不适用于计算节点路径。",
        "",
        "| 路径 | 数据集 | RTT/排队（仅 OSD） | CLS 总执行 | OMAP 引用 | payload 读取 | 纯算距 | 向量 RPC | 本地算距 | 未归类 |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for mode, dataset, _metrics, profile in measurements:
        distance = value(profile, "remote_distance_seconds")
        cls_total = value(profile, "distance_cls_total_seconds")
        cells = [
            mode,
            dataset,
            percent(value(profile, "distance_roundtrip_queue_seconds"), distance)
            if mode == "osd" else "n/a",
            percent(cls_total, distance),
            percent(value(profile, "distance_vector_ref_seconds"), distance),
            percent(value(profile, "distance_payload_read_seconds"), distance),
            percent(value(profile, "distance_compute_seconds"), distance),
            percent(value(profile, "distance_fetch_rpc_seconds"), distance),
            percent(value(profile, "distance_local_compute_seconds"), distance),
            percent(value(profile, "distance_cls_unaccounted_seconds"), distance)
            if mode == "osd" else percent(value(profile, "distance_compute_node_unaccounted_seconds"), distance),
        ]
        lines.append("| " + " | ".join(cells) + " |")
    lines += [
        "",
        "## 判读",
        "",
        "- 对应 PPT 观测点 A：重点比较 RTT/排队、payload 读取和纯算距。",
        "- 对应观测点 B：`邻接 patch` 是已扣除距离调用的独占累计工作时间。",
        "- 对应观测点 C：`全局 meta` 包含 `cas_global_meta` 的读改写与重试。",
        "- 每轮都应同时记录 `update_parallelism`、失败数和 `stopped_by_time_limit`；失败非零时只可用于瓶颈定位，不能作为最终性能结论。",
        "",
    ]
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("results_root", type=Path, help="directory containing <mode>/<dataset>/update.json")
    parser.add_argument("--output", type=Path, help="write Markdown to this file instead of stdout")
    args = parser.parse_args()
    report = build_report(args.results_root)
    if args.output:
        args.output.write_text(report, encoding="utf-8")
    else:
        print(report)


if __name__ == "__main__":
    main()
