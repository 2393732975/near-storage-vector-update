# 文档索引

本目录只保留当前设计、实验方法、正式报告和必要的历史追溯材料。运行生成的 JSON、
日志和图表位于本机 `results/`，不提交仓库。

## 当前文档

- [实验复现与数据准备手册](experiment-runbook.md)：数据获取、参数、运行命令、结果汇总和安全清理。
- [优化实施方案](optimization-implementation-plan.md)：阶段依赖、目标架构、验收门槛和消融设计。
- [阶段 0 正确性与观测基线](correctness-observability-baseline.md)：更新协议、离线检查器和正式验收流程。
- [阶段耗时与数据移动打点](stage-profiling.md)：compute/OSD 路径、指标边界、统计口径和运行入口。
- [阶段 0 集群验收报告](reports/phase0-validation-report-2026-09-22.md)：当前零失败、通过一致性检查的正式结果。
- [Compute-node 背景实验报告](reports/compute-background-report-2026-09-23.md)：PPT 背景数据移动实验的零失败复现结果。

## 参考材料

- [开题答辩 PPT](references/proposal-defense.pptx)：研究问题、背景实验和优化设计的原始依据。

## 历史材料

`archive/` 保存修正正确性问题之前的实验记录。这些报告用于追溯 PPT 复现过程，
包含失败更新或非严格 A/B 对照，不能作为当前性能结论：

- [早期 PPT 基线复现](archive/ppt-baseline-reproduction-2026-09-18.md)
- [早期 compute/OSD 对比](archive/compute-vs-osd-comparison-2026-09-22.md)

新增报告应放入 `reports/` 并在本索引登记；被新结果替代但仍有追溯价值的文档移入
`archive/`，不要把临时运行说明或本机路径拆成新的独立文档。
