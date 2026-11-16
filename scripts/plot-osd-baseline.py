#!/usr/bin/env python3
"""Render the historical Stage 1 baseline from its 24 local result files.

Requires matplotlib and a Chinese font. Does not connect to Ceph. Raw results
and fonts remain local; only the compact PNG/SVG figures belong in docs/.
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
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch
from matplotlib.ticker import FuncFormatter


REPO = Path(__file__).resolve().parents[1]
DATASETS = ["gist1m", "text2image10m", "deep100m", "sift100m"]
LABELS = ["GIST1M", "Text2Image10M", "Deep100M", "SIFT100M"]
SOURCE_COMMIT = "b74a89792725c874069cfd59d8bf58e15f4ec32e"
SOURCE_DIR = "phase1-raw-vs-osd-20260928T063622Z"
COLORS = {
    "compute": "#8794A3", "osd": "#187B80", "distance": "#187B80",
    "patch": "#D58A30", "search": "#6C76B7", "writes": "#8AB4A2",
    "other": "#B7BEC7", "rtt": "#397EAB", "payload": "#D58A30",
    "ref": "#8B93C3", "math": "#308D68", "text": "#223345",
}
def configure_font(path):
    if path:
        font_manager.fontManager.addfont(str(path))
        family = font_manager.FontProperties(fname=str(path)).get_name()
    else:
        available = {font.name for font in font_manager.fontManager.ttflist}
        family = next((name for name in ["Noto Sans CJK SC", "Source Han Sans SC",
                                       "WenQuanYi Zen Hei", "SimHei"]
                       if name in available), None)
        if not family:
            raise ValueError("A Chinese font is required; pass --font /path/to/font.otf")
    plt.rcParams.update({
        "font.family": family, "font.size": 11, "axes.titlesize": 14,
        "axes.labelsize": 11, "axes.unicode_minus": False,
        "axes.spines.top": False, "axes.spines.right": False,
        "axes.labelcolor": COLORS["text"], "text.color": COLORS["text"],
        "xtick.color": COLORS["text"], "ytick.color": COLORS["text"],
        "figure.facecolor": "white", "axes.facecolor": "white",
        "svg.fonttype": "path", "svg.hashsalt": "nsvu-stage1-baseline",
    })


def load_runs(root):
    manifest = (root / "manifest.txt").read_text(encoding="utf-8")
    if f"git_commit={SOURCE_COMMIT}" not in manifest:
        raise ValueError("Input is not the pinned Stage 1 experiment")
    runs = {}
    for dataset in DATASETS:
        for mode in ["compute", "osd"]:
            records = []
            for repetition in range(1, 4):
                directory = root / f"rep-{repetition}" / mode / dataset
                record = json.loads((directory / "update.json").read_text())
                check = json.loads((directory / "index-check.json").read_text())
                if record["failed_updates"] or check["status"] != "pass" or check["errors"]:
                    raise ValueError(f"Invalid case: {directory}")
                if record["vectors_processed"] <= 0 or record["distance_mode"] != mode:
                    raise ValueError(f"Wrong or empty case: {directory}")
                obs = record["observability"]
                if mode == "compute" and (record["storage_access_mode"] != "raw_rados"
                                          or obs["total_cls_exec_calls"] != 0
                                          or obs["total_raw_rados_calls"] <= 0):
                    raise ValueError(f"Invalid raw baseline: {directory}")
                if mode == "osd" and (record["storage_access_mode"] != "cls"
                                      or obs["total_cls_exec_calls"] <= 0):
                    raise ValueError(f"Invalid CLS baseline: {directory}")
                records.append(record)
            runs[dataset, mode] = records
    return runs


def mean_ci(records, key):
    values = [record[key] for record in records]
    # Student t, two-sided 95%, df=2; same coefficient as the historical summary.
    return statistics.mean(values), 4.303 * statistics.stdev(values) / math.sqrt(3)


def stage_ms(records, key):
    return sum(r["stage_profile"][key] for r in records) * 1000 / sum(
        r["vectors_processed"] for r in records)


def weighted_latency(records):
    return sum(r["avg_update_latency_ms"] * r["vectors_processed"] for r in records) / sum(
        r["vectors_processed"] for r in records)


def per_case_ratio(records, numerator, denominator="update_attempts"):
    return statistics.mean(r["observability"][numerator] / r["observability"][denominator]
                           for r in records)


def decorate(ax, xlabel):
    ax.set_xlabel(xlabel)
    ax.grid(axis="x", alpha=0.18)
    ax.set_axisbelow(True)
    ax.spines["left"].set_visible(False)
    ax.tick_params(axis="y", length=0)


def save(fig, output, name, caption):
    caption += "\n来源：阶段 1 严格 A/B，运行提交 b74a897；每数据集每路径 3 轮，原始 JSON 重新聚合。"
    fig.text(0.02, 0.025, caption, fontsize=9, color="#526171", va="bottom")
    fig.savefig(output / f"{name}.png", dpi=180, bbox_inches="tight")
    svg_path = output / f"{name}.svg"
    fig.savefig(svg_path, bbox_inches="tight", metadata={"Date": None})
    # Matplotlib emits trailing spaces in multiline path data; retain the
    # separating newlines while keeping generated assets clean in Git diffs.
    svg_path.write_text("\n".join(line.rstrip() for line in
                                  svg_path.read_text(encoding="utf-8").splitlines()) + "\n",
                        encoding="utf-8")
    plt.close(fig)


def workflow(output):
    fig, axes = plt.subplots(1, 2, figsize=(13, 8.2))
    fig.subplots_adjust(left=0.03, right=0.97, top=0.88, bottom=0.14, wspace=0.15)
    fig.suptitle("OSD-side CLS baseline：实验怎么做，距离请求怎么执行", y=0.98, fontsize=18)
    groups = [
        ("一轮 case；共 4 数据集 × 2 路径 × 3 轮", [
            "锁定版本、数据与二进制\n记录 manifest",
            "重建实验池、记录实际落点\n独立 hnswlib 建图 → 原生 RADOS 导入",
            "单 Coordinator，4 个更新 worker\n300 s 配置窗；约 270 s 后只排空在途任务",
            "update.json：延迟 / 阶段 / 静态 Recall\n更新后 checker：结构 / label / 语义局部性",
            "检查零失败与正确访问路径\n完成 24 case 后汇总均值、CI 与阶段成本",
            "保存报告和原始工件\n另行执行白名单清理并核对收尾",
        ]),
        ("OSD 更新中的距离调用与最终提交", [
            "Coordinator：读取邻接、选出候选\n按 owner / object 分组",
            "发送 query + 本对象候选 ID\n每个目标对象一次 CLS 调用",
            "OSD / CLS：读取 VectorRef\n按 offset / length 读取对象 payload",
            "OSD / CLS：计算 L2 / IP 距离\n返回 (ID, distance) 和分项耗时",
            "Coordinator：更新候选队列\n决定下一轮访问哪些节点",
            "插入完成后：finalize → CAS label\n最后标记旧节点 stale",
        ]),
    ]
    for ax, (title, labels) in zip(axes, groups):
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        ax.axis("off")
        ax.set_title(title, pad=16, fontsize=13)
        for i, label in enumerate(labels):
            y = 0.93 - i * 0.158
            ax.add_patch(FancyBboxPatch((0.03, y - 0.105), 0.94, 0.12,
                                       boxstyle="round,pad=0.008", linewidth=0,
                                       facecolor="#E9F2F2" if i % 2 == 0 else "#F0F2F5"))
            ax.text(0.5, y - 0.045, label, ha="center", va="center", fontsize=10.5)
            if i < len(labels) - 1:
                ax.add_patch(FancyArrowPatch((0.5, y - 0.116), (0.5, y - 0.139),
                                            arrowstyle="-|>", mutation_scale=13,
                                            color="#718494", linewidth=1.2))
    save(fig, output, "01-experiment-flow",
         "阶段 1 / b74a897：每个 case 独立构图，无冻结 cache 配对；baseline 尚无跨更新微批与 target cap。\n"
         "导入时间不计入更新性能；清理是实验结束后的独立步骤。")


def benefit(runs, output):
    fig, axes = plt.subplots(1, 2, figsize=(13, 5.6))
    fig.subplots_adjust(left=0.15, right=0.98, top=0.79, bottom=0.19, wspace=0.38)
    fig.suptitle("卸载的端到端收益：平均延迟下降，吞吐提高", y=0.98, fontsize=18)
    for ax, key, title, xlabel in zip(axes,
            ["avg_update_latency_ms", "throughput_updates_per_sec"],
            ["平均更新延迟（越低越好）", "更新吞吐（越高越好）"],
            ["ms / update", "update / s"]):
        maximum = 0
        for mode, shift, label in [("compute", -0.18, "Compute / raw RADOS"),
                                    ("osd", 0.18, "OSD / CLS")]:
            pairs = [mean_ci(runs[d, mode], key) for d in DATASETS]
            means, cis = zip(*pairs)
            maximum = max(maximum, max(m + c for m, c in pairs))
            positions = [i + shift for i in range(4)]
            ax.barh(positions, means, 0.30, xerr=cis, color=COLORS[mode],
                    label=label, capsize=3, error_kw={"elinewidth": 1, "ecolor": "#253A4C"})
        ax.set_yticks(range(4), LABELS)
        ax.invert_yaxis()
        ax.set_xlim(0, maximum * 1.28)
        for i, dataset in enumerate(DATASETS):
            control = mean_ci(runs[dataset, "compute"], key)[0]
            osd = mean_ci(runs[dataset, "osd"], key)[0]
            ax.text(maximum * 1.06, i + 0.18, f"{(osd / control - 1) * 100:+.2f}%",
                    va="center", fontsize=10)
        ax.set_title(title, pad=16)
        decorate(ax, xlabel)
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", bbox_to_anchor=(0.55, 0.91), ncol=2, frameon=False)
    save(fig, output, "02-latency-throughput",
         "每组为 3 轮算术均值；误差线为均值的 95% Student-t CI（df=2）；右侧为相对变化（OSD / compute − 1）。\n"
         "描述观察到的方向与幅度，不把 n=3 的差异表述为已完成显著性或因果验证。")


def stages(runs, output):
    fig, ax = plt.subplots(figsize=(12.5, 5.6))
    fig.subplots_adjust(left=0.16, right=0.93, top=0.78, bottom=0.20)
    fig.suptitle("OSD 更新仍有约 70% 的时间花在距离阶段", y=0.98, fontsize=18)
    labels = ["远端距离", "修补旧邻接", "搜索控制", "写向量与新邻接", "其余阶段与残差"]
    colors = [COLORS[k] for k in ["distance", "patch", "search", "writes", "other"]]
    for i, dataset in enumerate(DATASETS):
        records = runs[dataset, "osd"]
        total = weighted_latency(records)
        values = [stage_ms(records, "remote_distance_seconds"),
                  stage_ms(records, "adjacency_patch_exclusive_seconds"),
                  stage_ms(records, "full_graph_search_exclusive_seconds"),
                  stage_ms(records, "store_vector_seconds") + stage_ms(records, "set_new_adjacency_seconds")]
        values.append(total - sum(values))
        left = 0
        for j, (value, color) in enumerate(zip(values, colors)):
            ax.barh(i, value, 0.55, left=left, color=color, label=labels[j] if i == 0 else None)
            if j < 2:
                ax.text(left + value / 2, i, f"{value:.1f} ms\n{value / total * 100:.1f}%",
                        ha="center", va="center", color="white", fontsize=10)
            left += value
        ax.text(left + 5, i, f"{total:.1f} ms", va="center", fontsize=10)
    ax.set_yticks(range(4), LABELS)
    ax.invert_yaxis()
    ax.set_xlim(0, 570)
    decorate(ax, "按成功更新数加权的 ms / update")
    fig.legend(*ax.get_legend_handles_labels(), loc="upper center", bbox_to_anchor=(0.53, 0.92),
               ncol=3, frameon=False, fontsize=10)
    save(fig, output, "03-osd-update-stages",
         "各阶段 = 三轮累计 worker 秒数 × 1000 / 三轮成功更新数；总量为按成功数加权的平均延迟。\n"
         "这与图 2 的三轮算术均值不同；其余包含查 label、元数据、stale、label CAS / 调度等残差。")


def distance(runs, output):
    fig, ax = plt.subplots(figsize=(13, 5.8))
    fig.subplots_adjust(left=0.15, right=0.79, top=0.77, bottom=0.20)
    fig.suptitle("距离已经下沉，剩余开销主要是系统往返与 payload 读取", y=0.98, fontsize=17)
    keys = ["distance_roundtrip_queue_seconds", "distance_payload_read_seconds",
            "distance_vector_ref_seconds", "distance_compute_seconds", "distance_cls_unaccounted_seconds"]
    names = ["系统往返 / 排队残差", "payload 读取", "VectorRef 读取", "OSD 算距", "CLS 未细分"]
    colors = [COLORS[k] for k in ["rtt", "payload", "ref", "math", "other"]]
    for i, dataset in enumerate(DATASETS):
        records = runs[dataset, "osd"]
        total = stage_ms(records, "remote_distance_seconds")
        values = [stage_ms(records, key) / total * 100 for key in keys]
        left = 0
        for j, (value, color) in enumerate(zip(values, colors)):
            ax.barh(i, value, 0.53, left=left, color=color, label=names[j] if i == 0 else None)
            if j < 2:
                ax.text(left + value / 2, i, f"{value:.1f}%", ha="center", va="center", color="white", fontsize=11)
            left += value
        math_ms = stage_ms(records, "distance_compute_seconds")
        ax.text(103, i, f"距离总计 {total:.2f} ms\n纯算距 {math_ms:.3f} ms（{values[3]:.3f}%）",
                va="center", fontsize=9.5)
    ax.set_yticks(range(4), LABELS)
    ax.invert_yaxis()
    ax.set_xlim(0, 100)
    decorate(ax, "占 OSD 距离阶段的比例（%）")
    fig.legend(*ax.get_legend_handles_labels(), loc="upper center", bbox_to_anchor=(0.52, 0.92),
               ncol=3, frameon=False, fontsize=10)
    save(fig, output, "04-distance-breakdown",
         "从三轮原始累计计时重算；系统往返 / 排队 = 客户端距离耗时 − CLS 服务耗时。\n"
         "该差值含网络、librados、OSD 等待和线程调度，不是纯网络时间；极小算距分段用右侧精确值补充。")


def granularity(runs, output):
    fig, axes = plt.subplots(1, 3, figsize=(15.5, 5.8), gridspec_kw={"width_ratios": [1.25, 1, 1.1]})
    fig.subplots_adjust(left=0.12, right=0.98, top=0.80, bottom=0.21, wspace=0.26)
    fig.suptitle("批次仍然很碎：每次更新数百次 CLS，每批约一个候选", y=0.98, fontsize=18)
    ax = axes[0]
    for key, shift, label, color in [("total_cls_exec_calls", -0.17, "全部 CLS 调用", COLORS["other"]),
                                    ("distance_batches", 0.17, "距离批次", COLORS["osd"])]:
        values = [per_case_ratio(runs[d, "osd"], key) for d in DATASETS]
        ax.barh([i + shift for i in range(4)], values, 0.28, color=color, label=label)
        for i, value in enumerate(values):
            ax.text(value + 8, i + shift, f"{value:.1f}", va="center", fontsize=9)
    ax.set_xlim(0, 870)
    ax.legend(loc="upper left", bbox_to_anchor=(0, 1.20), frameon=False, fontsize=9)
    decorate(ax, "次 / update（OSD 路径）")
    values = [statistics.mean(r["observability"]["candidates_per_distance_batch"]
                              for r in runs[d, "osd"]) for d in DATASETS]
    axes[1].barh(range(4), values, 0.52, color=COLORS["osd"])
    axes[1].axvline(1, color="#586A79", linewidth=0.9, linestyle=":")
    for i, value in enumerate(values):
        axes[1].text(value + 0.035, i, f"{value:.3f}", va="center", fontsize=10)
    axes[1].set_xlim(0, 1.4)
    axes[1].set_title("OSD 距离候选 / 批次", pad=18, fontsize=12)
    decorate(axes[1], "候选数 / 距离批次")
    for mode, shift in [("compute", -0.17), ("osd", 0.17)]:
        kind = "raw_rados" if mode == "compute" else "cls"
        values = [statistics.mean((r["observability"][kind + "_request_bytes"] +
                                   r["observability"][kind + "_reply_bytes"]) /
                                  r["vectors_processed"] / 1024 for r in runs[d, mode])
                  for d in DATASETS]
        axes[2].barh([i + shift for i in range(4)], values, 0.28, color=COLORS[mode], label=mode)
        for i, value in enumerate(values):
            axes[2].text(value + 20, i + shift, f"{value:.0f}", va="center", fontsize=9)
    axes[2].set_xlim(0, 2200)
    axes[2].legend(loc="upper left", bbox_to_anchor=(0, 1.20), ncol=2, frameon=False, fontsize=9)
    decorate(axes[2], "记录的应用层 KiB / update")
    for i, ax in enumerate(axes):
        ax.set_yticks(range(4), LABELS if i == 0 else [""] * 4)
        ax.invert_yaxis()
    save(fig, output, "05-request-granularity",
         "每轮先归一化，再取三轮均值。右图为各路径已记录的应用层请求+回复字节，非网卡流量。\n"
         "OSD 避免回传完整候选向量，但仍为大量对象反复发送 query；raw API 调用与 CLS 调用不是等成本的一对一单位。")


def tail(runs, output):
    fig, ax = plt.subplots(figsize=(12, 5.8))
    fig.subplots_adjust(left=0.09, right=0.97, top=0.82, bottom=0.20)
    fig.suptitle("查看每轮 P99：SIFT compute 的一次长尾明显拉高均值", y=0.98, fontsize=17)
    for mode, base_shift, marker, label in [("compute", -0.18, "o", "Compute / raw RADOS"),
                                            ("osd", 0.18, "s", "OSD / CLS")]:
        for i, dataset in enumerate(DATASETS):
            ys = [r["p99_update_latency_ms"] for r in runs[dataset, mode]]
            xs = [i + base_shift + (rep - 1) * 0.07 for rep in range(3)]
            ax.scatter(xs, ys, color=COLORS[mode], marker=marker, s=62,
                       label=label if i == 0 else None, zorder=3)
    sift_p99 = [r["p99_update_latency_ms"] for r in runs["sift100m", "compute"]]
    ax.annotate(f"第 1 轮：{sift_p99[0]:.2f} ms", xy=(2.75, sift_p99[0]), xytext=(1.65, 5700),
                arrowprops={"arrowstyle": "->", "color": "#596B7A"}, fontsize=11)
    ax.set_yscale("log")
    ax.set_ylim(450, 12000)
    ax.set_yticks([500, 1000, 2000, 5000, 10000])
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, pos: f"{x:.0f}"))
    ax.set_ylabel("每轮 case 的更新 P99（ms，对数轴）")
    ax.set_xticks(range(4), LABELS)
    ax.grid(axis="y", alpha=0.18, which="major")
    ax.legend(loc="upper left", frameon=False, ncol=2)
    ax.set_xlim(-0.5, 3.55)
    save(fig, output, "06-p99-repetitions",
         "每点是一轮 case 的 P99；同模式左到右为 rep 1、2、3。并非单次更新散点，也不是全部更新合并后的 P99。\n"
         "SIFT compute 三轮为 " + " / ".join(f"{v:.2f}" for v in sift_p99) +
         " ms；观察到长尾差异，现有数据尚不能确定其系统根因。")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results-root", type=Path, default=REPO / "results" / SOURCE_DIR)
    parser.add_argument("--output-dir", type=Path, default=REPO / "docs/figures/osd-cls-baseline")
    parser.add_argument("--font", type=Path, help="Chinese font file, if not installed system-wide")
    args = parser.parse_args()
    configure_font(args.font)
    runs = load_runs(args.results_root)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    workflow(args.output_dir)
    for renderer in [benefit, stages, distance, granularity, tail]:
        renderer(runs, args.output_dir)
    print(f"Rendered 6 PNG + 6 SVG figures from 24 cases to {args.output_dir}")
    for dataset, label in zip(DATASETS, LABELS):
        records = runs[dataset, "osd"]
        print(f"{label}: weighted total={weighted_latency(records):.6f} ms; "
              f"distance={stage_ms(records, 'remote_distance_seconds'):.6f} ms; "
              f"compute={stage_ms(records, 'distance_compute_seconds'):.6f} ms")


if __name__ == "__main__":
    main()
