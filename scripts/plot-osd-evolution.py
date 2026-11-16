#!/usr/bin/env python3
"""Plot pre-optimization baselines (Stages 0-2) and later measured changes.

Stage 0 is a qualification study, not pooled performance data. Each change
uses its own experiment's control. Requires Matplotlib and a Chinese font.
Reads saved JSON only; does not contact Ceph or read HNSW cache files.
"""

import argparse
import importlib.util
import json
import statistics
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


REPO = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("baseline_plot", REPO / "scripts/plot-osd-baseline.py")
base = importlib.util.module_from_spec(spec)
spec.loader.exec_module(base)
DATASETS, LABELS = base.DATASETS, base.LABELS
COLORS = base.COLORS
SOURCES = {
    "phase2": ("phase2-osd-full-20261009T032432Z", "2978d7805d0d9a525c162e77bf7425efa61a4665"),
    "stage3": ("stage3-adaptive-fixed-smoke-20261010T132445Z", "2f7c60f1931dbb89f718c29d1cc15b76529eff7b"),
    "stage5a": ("stage5a-full-ab-20261015T080235Z", "51766ffa7a5e578f58c8553927a47a81378f5085"),
    "stage6": ("stage6-inflight-scan-20261027T101210Z", "1e278749f8c079e2dc29efdd76b6490b1387162d"),
}


def check_manifest(directory, commit):
    if f"git_commit={commit}" not in (directory / "manifest.txt").read_text():
        raise ValueError(f"Unexpected experiment identity: {directory}")


def load_case(directory, microbatch=False, cap=0):
    data = json.loads((directory / "update.json").read_text())
    check = json.loads((directory / "index-check.json").read_text())
    if (data["failed_updates"] or check["status"] != "pass" or check["errors"]
            or data["vectors_processed"] <= 0 or data["storage_access_mode"] != "cls"
            or data["observability"]["total_cls_exec_calls"] <= 0):
        raise ValueError(f"Invalid OSD case: {directory}")
    if data.get("distance_microbatch", False) != microbatch:
        raise ValueError(f"Unexpected microbatch mode: {directory}")
    if data.get("distance_target_max_inflight", 0) != cap:
        raise ValueError(f"Unexpected inflight cap: {directory}")
    return data


def load_all(root):
    phase1 = base.load_runs(root / base.SOURCE_DIR)
    phase2, stage3, stage5a, stage6 = {}, {}, {}, {}
    for key in ["phase2", "stage5a", "stage6"]:
        directory, commit = SOURCES[key]
        check_manifest(root / directory, commit)
    for dataset in DATASETS:
        phase2[dataset] = [load_case(root / SOURCES["phase2"][0] / f"rep-{rep}/osd/{dataset}")
                           for rep in range(1, 4)]
        for variant in ["control", "optimized"]:
            stage5a[dataset, variant] = [load_case(
                root / SOURCES["stage5a"][0] / f"rep-{rep}/{variant}/{dataset}/rep-1/osd/{dataset}",
                microbatch=(variant == "optimized")) for rep in range(1, 4)]
    for variant in ["control-w1", "adaptive-w8", "adaptive-w8-slack1"]:
        directory = root / SOURCES["stage3"][0] / variant
        check_manifest(directory, SOURCES["stage3"][1])
        stage3[variant] = load_case(directory / "rep-1/osd/gist1m")
    for cap in [0, 1, 2, 4]:
        variant = "uncapped" if cap == 0 else f"cap{cap}"
        stage6[cap] = [load_case(root / SOURCES["stage6"][0] /
                      f"rep-{rep}/{variant}/rep-1/osd/deep100m", microbatch=True, cap=cap)
                      for rep in range(1, 4)]
    return phase1, phase2, stage3, stage5a, stage6


def mean(records, key):
    return statistics.mean(r[key] for r in records)


def change(control, optimized):
    return (optimized / control - 1) * 100


def ratio(records, numerator, denominator="update_attempts"):
    return sum(r["observability"][numerator] for r in records) / sum(
        r["observability"][denominator] for r in records)


def save(fig, output, name, caption, source):
    fig.text(0.02, 0.025, caption + "\n来源：" + source, fontsize=9, color="#526171", va="bottom")
    fig.savefig(output / f"{name}.png", dpi=180, bbox_inches="tight")
    svg = output / f"{name}.svg"
    fig.savefig(svg, bbox_inches="tight", metadata={"Date": None})
    svg.write_text("\n".join(line.rstrip() for line in svg.read_text().splitlines()) + "\n")
    plt.close(fig)


def baseline_overview(phase1, phase2, output):
    fig, axes = plt.subplots(1, 2, figsize=(14, 6.6))
    fig.subplots_adjust(left=0.14, right=0.96, top=0.78, bottom=0.20, wspace=0.36)
    fig.suptitle("基线范围止于阶段 2：主 OSD 基线取 M2", fontsize=18, y=0.98)
    variants = [("阶段 1 C0：原生 RADOS", "#A0AAB5", -0.25,
                 lambda d: phase1[d, "compute"]),
                ("阶段 1 B0：原始 OSD", "#76B1B2", 0,
                 lambda d: phase1[d, "osd"]),
                ("阶段 2 M2：主 OSD 基线", "#187B80", 0.25, lambda d: phase2[d])]
    for ax, key, xlabel in zip(axes, ["avg_update_latency_ms", "throughput_updates_per_sec"],
                               ["平均更新延迟（ms，越低越好）", "吞吐（update/s，越高越好）"]):
        maximum = 0
        for label, color, shift, getter in variants:
            values = [base.mean_ci(getter(d), key) for d in DATASETS]
            means, cis = zip(*values)
            maximum = max(maximum, max(v + ci for v, ci in values))
            ax.barh([i + shift for i in range(4)], means, 0.21, xerr=cis,
                    color=color, label=label, capsize=2, error_kw={"elinewidth": 0.9})
        for i, dataset in enumerate(DATASETS):
            value = mean(phase2[dataset], key)
            ax.text(maximum * 1.035, i + 0.25, f"{value:.3f}", va="center", fontsize=10)
        ax.set_xlim(0, maximum * 1.24)
        ax.set_yticks(range(4), LABELS)
        ax.invert_yaxis()
        base.decorate(ax, xlabel)
    fig.legend(*axes[0].get_legend_handles_labels(), loc="upper center",
               bbox_to_anchor=(0.53, 0.91), ncol=3, frameon=False, fontsize=10)
    save(fig, output, "01-baselines-through-phase2",
         "三轮 case 算术均值 ± 95% Student-t CI。阶段 1 / 2 是不同批次；本图描述基线建立，不计算后续优化收益。\n"
         "阶段 0 为连续运行正确性验收，不把其轮次合并进性能均值。右侧数字对应阶段 2 M2。",
         "阶段 1 b74a897；阶段 2 2978d78；各 3 次独立重导入。")


def phase2_profile(phase2, output):
    fig, axes = plt.subplots(2, 1, figsize=(13, 9.5))
    fig.subplots_adjust(left=0.16, right=0.81, top=0.86, bottom=0.15, hspace=0.65)
    fig.suptitle("阶段 2 主基线：距离占整次更新约 67%–70%，纯算距仍很小", fontsize=17, y=0.985)
    names = ["距离", "邻接 patch", "搜索控制", "其余"]
    colors = [COLORS[k] for k in ["distance", "patch", "search", "other"]]
    distance_keys = ["distance_roundtrip_queue_seconds", "distance_payload_read_seconds",
                     "distance_vector_ref_seconds", "distance_compute_seconds", "distance_cls_unaccounted_seconds"]
    distance_names = ["系统往返 / 排队残差", "payload", "VectorRef", "算距", "CLS 未细分"]
    distance_colors = [COLORS[k] for k in ["rtt", "payload", "ref", "math", "other"]]
    for i, dataset in enumerate(DATASETS):
        records = phase2[dataset]
        total = base.weighted_latency(records)
        values = [base.stage_ms(records, key) for key in ["remote_distance_seconds",
                  "adjacency_patch_exclusive_seconds", "full_graph_search_exclusive_seconds"]]
        values.append(total - sum(values))
        left = 0
        for j, (v, color) in enumerate(zip(values, colors)):
            axes[0].barh(i, v, 0.55, left=left, color=color, label=names[j] if i == 0 else None)
            if j < 2:
                axes[0].text(left + v / 2, i, f"{v:.1f} ms", ha="center", va="center", color="white", fontsize=10)
            left += v
        axes[0].text(total + 5, i, f"{total:.1f} ms", va="center", fontsize=10)
        distance = values[0]
        left = 0
        for j, (key, color) in enumerate(zip(distance_keys, distance_colors)):
            value = base.stage_ms(records, key) / distance * 100
            axes[1].barh(i, value, 0.55, left=left, color=color, label=distance_names[j] if i == 0 else None)
            if j < 2:
                axes[1].text(left + value / 2, i, f"{value:.1f}%", ha="center", va="center", color="white", fontsize=10)
            left += value
        math_ms = base.stage_ms(records, "distance_compute_seconds")
        axes[1].text(102, i, f"算距 {math_ms:.3f} ms\n占距离 {math_ms / distance * 100:.3f}%", va="center", fontsize=9)
    for ax in axes:
        ax.set_yticks(range(4), LABELS)
        ax.invert_yaxis()
        ax.legend(loc="lower left", bbox_to_anchor=(0, 1.02), ncol=3, frameon=False, fontsize=10)
    axes[0].set_xlim(0, 570)
    axes[1].set_xlim(0, 100)
    base.decorate(axes[0], "完整更新阶段：按成功数加权的 ms/update")
    base.decorate(axes[1], "距离内部组成：占该阶段的比例（%）")
    save(fig, output, "02-phase2-cost-profile",
         "累计 worker 时间按成功更新数加权；总量与三轮等权均值略有不同。系统往返/排队不是纯网络时间。\n"
         "这些是阶段 2 原始 M2 的实测值，未使用阶段 5A 或阶段 6 的优化组数据。",
         "phase2-osd-full-20261009T032432Z / 2978d78，12 case。")


def frontier(stage3, output):
    control = stage3["control-w1"]
    functions = [("平均延迟", lambda r: r["avg_update_latency_ms"]),
                 ("更新 P99", lambda r: r["p99_update_latency_ms"]),
                 ("距离批次 / update", lambda r: r["observability"]["distance_batches"] / r["vectors_processed"]),
                 ("query 字节 / update", lambda r: r["observability"]["logical_distance_query_bytes"] / r["vectors_processed"])]
    fig, ax = plt.subplots(figsize=(11.5, 5.8))
    fig.subplots_adjust(left=0.22, right=0.94, top=0.80, bottom=0.22)
    fig.suptitle("阶段 3：批次仅小幅减少，延迟反而上升", y=0.98, fontsize=18)
    for variant, shift, label, color in [("adaptive-w8", -0.18, "自适应 W8，slack=0.25", "#B17831"),
                                        ("adaptive-w8-slack1", 0.18, "自适应 W8，slack=1.00", "#7567A1")]:
        values = [change(getter(control), getter(stage3[variant])) for _, getter in functions]
        ax.barh([i + shift for i in range(4)], values, 0.30, label=label, color=color)
        for i, value in enumerate(values):
            ax.text(value + (0.3 if value >= 0 else -0.3), i + shift, f"{value:+.2f}%",
                    ha="left" if value >= 0 else "right", va="center", fontsize=10)
    ax.axvline(0, color="#556370", linewidth=0.9)
    ax.set_yticks(range(4), [name for name, _ in functions])
    ax.invert_yaxis()
    ax.set_xlim(-8, 18)
    base.decorate(ax, "相对同批 W1 的变化（%，这些指标均越低越好）")
    fig.legend(*ax.get_legend_handles_labels(), loc="upper center", bbox_to_anchor=(0.53, 0.91), ncol=2, frameon=False)
    save(fig, output, "03-frontier-effects",
         "GIST1M、30 s 配置窗、每配置仅 1 轮，未共享冻结图；显示观察差异，不画多轮 CI，也不作确定因果归因。\n"
         "未达到批次/query 字节至少下降 10% 且延迟/P99 不回退的要求；自适应 frontier 默认关闭。",
         "stage3-adaptive-fixed-smoke-20261010T132445Z / 2f7c60f；已排除缺陷轮。")


def formal(stage5a, output):
    fig, axes = plt.subplots(1, 3, figsize=(15, 6))
    fig.subplots_adjust(left=0.13, right=0.96, top=0.79, bottom=0.22, wspace=0.33)
    fig.suptitle("阶段 5A：平均延迟全部改善，但 Deep100M P99 回退", y=0.98, fontsize=18)
    for ax, key, title in zip(axes,
            ["avg_update_latency_ms", "p99_update_latency_ms", "throughput_updates_per_sec"],
            ["平均延迟变化：负值更好", "P99 变化：负值更好", "吞吐变化：正值更好"]):
        values = [change(mean(stage5a[d, "control"], key), mean(stage5a[d, "optimized"], key)) for d in DATASETS]
        colors = ["#B94A48" if key != "throughput_updates_per_sec" and v > 0 else COLORS["osd"] for v in values]
        ax.barh(range(4), values, 0.56, color=colors)
        ax.axvline(0, color="#596B7A", linewidth=0.9)
        for i, value in enumerate(values):
            ax.text(value + (1 if value >= 0 else -1), i, f"{value:+.2f}%",
                    va="center", ha="left" if value >= 0 else "right", fontsize=11)
        ax.set_yticks(range(4), LABELS if ax is axes[0] else [""] * 4)
        ax.invert_yaxis()
        ax.set_title(title, pad=16, fontsize=12)
        ax.set_xlim((-43, 35) if key != "throughput_updates_per_sec" else (0, 40))
        base.decorate(ax, "相对本批次 M2 控制组（%）")
    save(fig, output, "04-stage5a-formal-effects",
         "每组 3 轮；每对同提交、同冻结基础图。百分比 = 优化组三轮均值 / 同批控制组三轮均值 − 1。\n"
         "控制为关闭执行器的 M2；优化为 250 μs + 5 executor 线程。Deep P99 +23.58%，整体性能门禁失败。",
         "stage5a-full-ab-20261015T080235Z / 51766ff；不是用阶段 2 历史数当分母。")


def mechanism(stage5a, output):
    fig, axes = plt.subplots(1, 2, figsize=(13, 5.8), gridspec_kw={"width_ratios": [1.3, 1]})
    fig.subplots_adjust(left=0.16, right=0.95, top=0.79, bottom=0.22, wspace=0.25)
    fig.suptitle("阶段 5A：逻辑距离等待缩短，物理合批仍弱", y=0.98, fontsize=18)
    for j, (label, getter, color) in enumerate([
            ("距离阶段耗时", lambda r: base.stage_ms(r, "remote_distance_seconds"), COLORS["osd"]),
            ("CLS 调用 / update", lambda r: ratio(r, "total_cls_exec_calls"), "#9CA7B2")]):
        values = [change(getter(stage5a[d, "control"]), getter(stage5a[d, "optimized"])) for d in DATASETS]
        shift = (j - 0.5) * 0.34
        axes[0].barh([i + shift for i in range(4)], values, 0.28, label=label, color=color)
        for i, value in enumerate(values):
            axes[0].text(value - 0.6, i + shift, f"{value:+.2f}%", ha="right", va="center", fontsize=10)
    axes[0].set_xlim(-48, 0)
    axes[0].legend(loc="lower left", bbox_to_anchor=(0, 1.02), frameon=False, ncol=2, fontsize=10)
    values = [ratio(stage5a[d, "optimized"], "distance_multi_query_subrequests", "distance_multi_query_rpcs") for d in DATASETS]
    axes[1].barh(range(4), values, 0.55, color=COLORS["osd"])
    axes[1].axvline(1, linewidth=1, color="#687887", linestyle=":")
    for i, value in enumerate(values):
        axes[1].text(value + 0.025, i, f"{value:.3f}", va="center", fontsize=10)
    axes[1].set_xlim(0, 1.30)
    axes[1].set_title("优化组：逻辑子请求 / 物理距离 RPC", fontsize=12, pad=16)
    for i, ax in enumerate(axes):
        ax.set_yticks(range(4), LABELS if i == 0 else [""] * 4)
        ax.invert_yaxis()
    base.decorate(axes[0], "相对同批 M2 的变化（%）")
    base.decorate(axes[1], "子请求 / RPC（1 表示没有多个子请求合并）")
    save(fig, output, "05-stage5a-mechanism",
         "阶段耗时按成功数加权，调用按总尝试数归一化，子请求/RPC 用总量比；这些分母与主性能图的三轮等权不同。\n"
         "收益与独立对象等待重叠一致，不能解释为同等幅度的 RPC 减少；写 patch、label 和 meta 未跨 update 合批。",
         "阶段 5A 四数据集正式 A/B / 51766ff。")


def inflight(stage6, output):
    fig, axes = plt.subplots(1, 2, figsize=(14, 6.4), gridspec_kw={"width_ratios": [1, 1.25]})
    fig.subplots_adjust(left=0.16, right=0.96, top=0.78, bottom=0.22, wspace=0.33)
    fig.suptitle("阶段 6：在已启用 5A 的路径上限流；均值改善由异常轮主导", y=0.98, fontsize=17)
    caps = [0, 1, 2, 4]
    pairs = [base.mean_ci(stage6[cap], "avg_update_latency_ms") for cap in caps]
    labels = []
    for cap in caps:
        limited = sum(r["observability"]["distance_target_limited_subrequests"] for r in stage6[cap])
        labels.append("uncapped（已启用 5A）" if cap == 0 else f"cap={cap}；受限 {limited:,} 次")
    axes[0].barh(range(4), [p[0] for p in pairs], 0.55, xerr=[p[1] for p in pairs],
                 color=["#9CA7B2", COLORS["osd"], "#B7C9CA", "#B7C9CA"], capsize=3)
    for i, (value, ci) in enumerate(pairs):
        axes[0].text(680, i, f"{value:.2f}", va="center", fontsize=10)
    axes[0].set_yticks(range(4), labels, fontsize=10)
    axes[0].invert_yaxis()
    axes[0].set_xlim(0, 800)
    axes[0].set_title("平均延迟：三轮均值 ± 95% CI", fontsize=12, pad=16)
    base.decorate(axes[0], "ms / update")
    for cap, shift, color, label in [(0, -0.17, "#9CA7B2", "uncapped：已启用 5A"),
                                    (1, 0.17, COLORS["osd"], "cap=1：5A + 固定限流")]:
        values = [r["p99_update_latency_ms"] for r in stage6[cap]]
        axes[1].bar([i + shift for i in range(3)], values, 0.29, color=color, label=label)
        for i, value in enumerate(values):
            axes[1].text(i + shift, value + 90, f"{value:.0f}", ha="center", va="bottom", fontsize=10)
    axes[1].set_xticks(range(3), ["第 1 轮：cap1 回退", "第 2 轮：改善", "第 3 轮：cap1 回退"], fontsize=10)
    axes[1].set_ylabel("每轮 update P99（ms）")
    axes[1].set_ylim(0, 4850)
    axes[1].grid(axis="y", alpha=0.18)
    axes[1].set_axisbelow(True)
    axes[1].legend(loc="lower left", bbox_to_anchor=(0, 1.02), frameon=False, fontsize=10)
    save(fig, output, "06-stage6-inflight-effects",
         "相对同批 uncapped：cap1 平均延迟 −3.38%、P99 均值 −51.60%、吞吐 +2.31%；但第 1、3 轮延迟/P99 均回退。\n"
         "queue P95 仅改善 0.71%，推导往返/排队 P95 改善 3.23%；cap4 未触发限流，不能把其变快归因于限流。",
         "stage6-inflight-scan-20261027T101210Z / 1e27874；各 3 轮，同轮同冻结图。")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results-dir", type=Path, default=REPO / "results")
    parser.add_argument("--output-dir", type=Path, default=REPO / "docs/figures/osd-baseline-optimizations")
    parser.add_argument("--font", type=Path)
    args = parser.parse_args()
    base.configure_font(args.font)
    phase1, phase2, stage3, stage5a, stage6 = load_all(args.results_dir)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    baseline_overview(phase1, phase2, args.output_dir)
    phase2_profile(phase2, args.output_dir)
    frontier(stage3, args.output_dir)
    formal(stage5a, args.output_dir)
    mechanism(stage5a, args.output_dir)
    inflight(stage6, args.output_dir)
    print("Rendered 6 PNG + 6 SVG; 75 cases read across Stage 1/2/3/5A/6; no cluster access.")


if __name__ == "__main__":
    main()
