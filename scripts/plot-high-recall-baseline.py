#!/usr/bin/env python3
"""Plot the three completed high-recall baseline datasets from saved evidence.

Reads only qualified Deep100M/GIST1M/SIFT100M cases, never graph caches or Ceph.
Requires matplotlib and a Chinese font file (or an installed Chinese font).
"""

import argparse
import json
import math
import statistics
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import font_manager


REPO = Path(__file__).resolve().parents[1]
RUN_NAME = "high-recall-formal-auto-resume-20261126T113624.245041Z"
DATASETS = ["deep100m", "gist1m", "sift100m"]
LABELS = ["Deep100M", "GIST1M", "SIFT100M"]
COLORS = {"compute": "#8A98A7", "osd": "#147F82", "search": "#7877B6",
          "patch": "#D78D32", "other": "#B9C1C8", "rtt": "#3F80AA",
          "ref": "#9397C4", "payload": "#D78D32", "math": "#398B66"}


def configure_font(path):
    if path:
        font_manager.fontManager.addfont(str(path))
        name = font_manager.FontProperties(fname=str(path)).get_name()
    else:
        names = {font.name for font in font_manager.fontManager.ttflist}
        name = next((n for n in ["Noto Sans CJK SC", "Source Han Sans SC", "SimHei"] if n in names), None)
        if not name:
            raise ValueError("Pass --font /path/to/a/Chinese/font.otf")
    plt.rcParams.update({"font.family": name, "font.size": 11, "axes.titlesize": 13,
                         "axes.spines.top": False, "axes.spines.right": False,
                         "axes.unicode_minus": False, "text.color": "#243648",
                         "figure.facecolor": "white", "axes.facecolor": "white",
                         "svg.fonttype": "path"})


def load_cases(root):
    summary = json.loads((root / "formal-summary.json").read_text())
    records = {}
    for dataset in DATASETS:
        ds = summary["datasets"][dataset]
        if ds["qualified_pairs"] != 3:
            raise ValueError(f"Dataset lacks three qualified pairs: {dataset}")
        pairs = sorted(ds["pairs"], key=lambda p: p["rep"])
        if [p["rep"] for p in pairs] != [1, 2, 3]:
            raise ValueError(f"Unexpected repetitions: {dataset}")
        for route in ["compute", "osd"]:
            values = []
            for pair in pairs:
                case = pair["cases"][route]
                if pair["qualification"] != "pass" or case["qualification"] != "pass":
                    raise ValueError("Unqualified pair or case")
                hashes = {pair["cases"][mode]["case"]["frozen_graph"]["cache_sha256"]
                          for mode in ["compute", "osd"]}
                if len(hashes) != 1:
                    raise ValueError("Pair does not share a frozen graph")
                directory = root / "pilot/cases" / dataset / f"rep-{pair['rep']}" / route
                update = json.loads((directory / "update.json").read_text())
                if (update["failed_updates"] != 0 or not update["complete_stage_breakdown_valid"]
                        or update["vectors_processed"] != case["expected_updates"]
                        or update["distance_microbatch"] or update["frontier_window"] != 1
                        or case["final"]["evaluation"] != "remote_active"
                        or case["final"]["recall_at_k"] < 0.90):
                    raise ValueError(f"Invalid baseline case: {directory}")
                values.append(update)
            records[dataset, route] = values
    return records


def mean_ci(records, key):
    values = [r[key] for r in records]
    return statistics.mean(values), 4.303 * statistics.stdev(values) / math.sqrt(3)


def stage_ms(records, key, top_level=False):
    seconds = sum((r if top_level else r["stage_profile"])[key] for r in records)
    return 1000 * seconds / sum(r["vectors_processed"] for r in records)


def style(ax, xlabel):
    ax.set_xlabel(xlabel)
    ax.set_axisbelow(True)
    ax.grid(axis="x", alpha=0.18)
    ax.spines["left"].set_visible(False)
    ax.tick_params(axis="y", length=0)


def save(fig, output, name, note):
    fig.text(0.02, 0.02, note + "\n数据源为 " + RUN_NAME + " 的 Deep100M、GIST1M、SIFT100M 三组正式配对。",
             fontsize=8.5, color="#526171", va="bottom")
    fig.savefig(output / (name + ".png"), dpi=180, bbox_inches="tight")
    svg = output / (name + ".svg")
    fig.savefig(svg, bbox_inches="tight")
    svg.write_text("\n".join(line.rstrip() for line in svg.read_text().splitlines()) + "\n")
    plt.close(fig)


def performance(records, output):
    fig, axes = plt.subplots(1, 2, figsize=(13.2, 5.7))
    fig.subplots_adjust(left=0.13, right=0.97, top=0.79, bottom=0.22, wspace=0.35)
    fig.suptitle("高召回率基线的更新延迟与吞吐", fontsize=18, y=0.98)
    for ax, key, label in zip(axes, ["avg_update_latency_ms", "throughput_updates_per_sec"],
                              ["平均更新延迟（ms，越低越好）", "吞吐（update/s，越高越好）"]):
        maximum = max(m + c for d in DATASETS for route in ["compute", "osd"]
                      for m, c in [mean_ci(records[d, route], key)])
        for route, shift, legend in [("compute", -0.18, "Compute：原生 RADOS"), ("osd", 0.18, "OSD：CLS")]:
            pairs = [mean_ci(records[d, route], key) for d in DATASETS]
            means, cis = zip(*pairs)
            ax.barh([i + shift for i in range(3)], means, 0.29, xerr=cis,
                    color=COLORS[route], capsize=3, label=legend,
                    error_kw={"elinewidth": 1, "ecolor": "#2B3F51"})
            for i, value in enumerate(means):
                ax.text(maximum * 1.025, i + shift, f"{value:.2f}" if key.startswith("avg") else f"{value:.3f}",
                        va="center", fontsize=10)
        ax.set_yticks(range(3), LABELS)
        ax.invert_yaxis()
        ax.set_xlim(0, maximum * 1.23)
        style(ax, label)
    fig.legend(*axes[0].get_legend_handles_labels(), loc="upper center", bbox_to_anchor=(0.53, 0.91),
               ncol=2, frameon=False)
    save(fig, output, "01-paired-performance",
         "每数据集三对独立基础图，配对双方加载同图。柱长为三轮 case 算术均值，误差线为均值的 95% Student-t CI。\n"
         "固定更新数：Deep/SIFT 每 case 2000，GIST 每 case 400；四个更新 worker，终态远端 active Recall@10 均 ≥ 0.90。")


def stages(records, output):
    fig, axes = plt.subplots(2, 1, figsize=(12.5, 9.3))
    fig.subplots_adjust(left=0.19, right=0.84, top=0.87, bottom=0.16, hspace=0.72)
    fig.suptitle("距离访问占更新耗时的九成以上", fontsize=17, y=0.985)
    names = ["距离访问", "图搜索控制", "邻接 patch", "其他阶段与残差"]
    colors = [COLORS[k] for k in ["osd", "search", "patch", "other"]]
    rows = [(d, route) for d in DATASETS for route in ["compute", "osd"]]
    for i, (dataset, route) in enumerate(rows):
        data = records[dataset, route]
        total = stage_ms(data, "complete_update_seconds", True)
        values = [stage_ms(data, k) for k in ["remote_distance_seconds", "full_graph_search_exclusive_seconds", "adjacency_patch_exclusive_seconds"]]
        values.append(total - sum(values))
        if values[-1] < 0:
            raise ValueError("Negative residual in complete-stage breakdown")
        left = 0
        for j, (v, color) in enumerate(zip(values, colors)):
            axes[0].barh(i, v, 0.66, left=left, color=color, label=names[j] if i == 0 else None)
            if j == 0:
                axes[0].text(v / 2, i, f"{v:.0f} ms  ({v / total * 100:.1f}%)", ha="center", va="center", color="white", fontsize=10)
            left += v
        axes[0].text(total + 25, i, f"{total:.0f}", va="center", fontsize=9)
    axes[0].set_yticks(range(6), [LABELS[DATASETS.index(d)] + "  " + ("Compute" if route == "compute" else "OSD") for d, route in rows])
    axes[0].invert_yaxis()
    axes[0].set_xlim(0, 4400)
    axes[0].legend(loc="lower left", bbox_to_anchor=(0, 1.02), ncol=2, frameon=False, fontsize=10)
    style(axes[0], "完整更新阶段（ms/update，按成功更新数归一化）")
    keys = ["distance_roundtrip_queue_seconds", "distance_payload_read_seconds", "distance_vector_ref_seconds", "distance_compute_seconds", "distance_cls_unaccounted_seconds"]
    names = ["系统往返 / 排队残差", "payload 读取", "VectorRef 读取", "OSD 算距", "CLS 未细分"]
    colors = [COLORS[k] for k in ["rtt", "payload", "ref", "math", "other"]]
    for i, dataset in enumerate(DATASETS):
        data = records[dataset, "osd"]
        total = stage_ms(data, "remote_distance_seconds")
        left = 0
        for j, (key, color) in enumerate(zip(keys, colors)):
            value = stage_ms(data, key) / total * 100
            axes[1].barh(i, value, 0.55, left=left, color=color, label=names[j] if i == 0 else None)
            if j < 2:
                axes[1].text(left + value / 2, i, f"{value:.1f}%", ha="center", va="center", color="white", fontsize=10)
            left += value
        pure = stage_ms(data, "distance_compute_seconds")
        axes[1].text(102, i, f"纯算距 {pure:.2f} ms\n占距离 {pure / total * 100:.2f}%", va="center", fontsize=9)
    axes[1].set_yticks(range(3), LABELS)
    axes[1].invert_yaxis()
    axes[1].set_xlim(0, 100)
    axes[1].legend(loc="lower left", bbox_to_anchor=(0, 1.02), ncol=3, frameon=False, fontsize=9.5)
    style(axes[1], "OSD 距离阶段内部组成（%）")
    save(fig, output, "02-stage-costs",
         "各阶段累计 worker 秒数按成功更新数归一化。其他阶段含写入、label CAS、锁等待与完整残差。\n"
         "往返/排队残差包含网络、队列和客户端调用等系统开销。")


def tails(records, output):
    fig, axes = plt.subplots(1, 3, figsize=(13, 5.8), sharey=True)
    fig.subplots_adjust(left=0.08, right=0.98, top=0.74, bottom=0.22, wspace=0.16)
    fig.suptitle("三数据集逐轮配对的更新 P99 均下降", fontsize=18, y=0.98)
    for ax, dataset, name in zip(axes, DATASETS, LABELS):
        for i in range(3):
            c = records[dataset, "compute"][i]["p99_update_latency_ms"]
            o = records[dataset, "osd"][i]["p99_update_latency_ms"]
            ax.plot([i + 1, i + 1], [o, c], color="#C8D1D7", linewidth=1.2)
        for route, marker, label in [("compute", "o", "Compute / raw RADOS"), ("osd", "s", "OSD / CLS")]:
            values = [r["p99_update_latency_ms"] for r in records[dataset, route]]
            ax.scatter([1, 2, 3], values, s=60, marker=marker, color=COLORS[route], label=label, zorder=3)
            for i, value in enumerate(values):
                ax.text(i + 1, value + (140 if route == "compute" else -220), f"{value:.0f}", ha="center", va="center", fontsize=9)
        delta = (statistics.mean(r["p99_update_latency_ms"] for r in records[dataset, "osd"]) /
                 statistics.mean(r["p99_update_latency_ms"] for r in records[dataset, "compute"]) - 1) * 100
        ax.set_title(f"{name}\nP99 均值变化 {delta:+.2f}%", fontsize=12, pad=14)
        ax.set_xticks([1, 2, 3], ["rep 1", "rep 2", "rep 3"])
        ax.set_xlim(0.5, 3.5)
        ax.set_ylim(3000, 7500)
        ax.grid(axis="y", alpha=0.18)
    axes[0].set_ylabel("每轮 case 的更新 P99（ms）")
    fig.legend(*axes[0].get_legend_handles_labels(), loc="upper center", bbox_to_anchor=(0.53, 0.91), ncol=2, frameon=False)
    save(fig, output, "03-p99-repetitions",
         "每个点为一个 case 的 P99。同一竖线连接同冻结图的 Compute 与 OSD。纵轴从 3000 ms 起。\n"
         "百分比按 OSD 与 Compute 两条路线的三轮 P99 均值计算。")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results-root", type=Path, default=REPO / "results" / RUN_NAME)
    parser.add_argument("--output-dir", type=Path, default=REPO / "docs/figures/high-recall-baseline-three-datasets")
    parser.add_argument("--font", type=Path)
    args = parser.parse_args()
    configure_font(args.font)
    records = load_cases(args.results_root)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    performance(records, args.output_dir)
    stages(records, args.output_dir)
    tails(records, args.output_dir)
    print("Rendered 3 PNG figures from 18 qualified cases; Text2Image10M excluded.")


if __name__ == "__main__":
    main()
