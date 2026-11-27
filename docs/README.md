# 文档索引

本目录只保留当前设计、实验方法、正式报告和必要的历史追溯材料。运行生成的 JSON、
日志和图表位于本机 `results/`，不提交仓库。

## 当前文档

- [源码阅读指南](source-reading-guide.md)：从 ID 与对象布局、基础图导入、在线更新调用链到 CLS、只读执行器、检查器和实验门禁，附源码定位、流程图、阅读顺序与验证练习。
- [三数据集高召回基线三轮结果与瓶颈分析](reports/high-recall-baseline-three-dataset-bottlenecks-2026-11-27.md)：汇总 Deep100M、GIST1M、SIFT100M 的 18 个合格 case，分析距离与元数据、逐轮 P99、尾部工作量、导入和质量验收周转，并保留质量与因果解释的边界。
- [Deep100M 高召回基线阶段性瓶颈分析](reports/high-recall-deep100m-bottleneck-analysis-2026-11-24.md)：截至 11-24 的两轮历史快照。完整三轮及 GIST/SIFT 结果见上述三数据集报告。
- [Compute 与 OSD/CLS Baseline 实现对照报告](reports/compute-osd-baseline-architecture-2026-11-23.md) 逐模块说明两路的共用流程、HNSW 搜索、对象读写和验收，并对照 Deep100M 正式重测的首轮结果。
- [HNSW 元数据管理与 Ceph CRUSH 对象放置报告](reports/metadata-management-and-crush-placement-2026-11-23.md) 解释向量与 OMAP 的同对象布局、label 与全局元数据、覆盖更新顺序，并给出 Deep100M 的实际 PG 和 OSD 映射。
- [实验复现与数据准备手册](experiment-runbook.md)：数据获取、参数、运行命令、结果汇总和安全清理。
- [高召回 Compute / OSD baseline 重测执行计划](high-recall-baseline-execution-plan-2026-11-17.md)：用户授权在故障盘停用后继续，已制定四 owner→OSD 0/2/3/4 的新组恢复流程。复用本地图，重测 GIST 导入配置并继续 Deep 资格；旧五盘远端结果不复用，正式 24-case 尚未启动。
- [高召回准备任务停止诊断与 SSD 维修排查](reports/baseline-preparation-stop-diagnosis-2026-11-19.md)：恢复任务因 osd.1 新慢告警停止；对应 SSD 在排除数据库预设后仍报健康失败，授权短自检也失败。已备份单副本实验池，设备尚未修复，需厂商诊断或换盘；Deep 实际导入尚未开始。
- [故障 osd.1 数据迁移与停用记录](reports/osd1-data-evacuation-report-2026-11-19.md)：已完成 8 池备份及前后 SHA-256 校验，39 PG clean；故障 OSD 已 down/out 并禁用自启，其余四个 OSD 正常。原五 OSD 实验布局已改变，继续暂停。
- [优化实施方案](optimization-implementation-plan.md)：阶段依赖、目标架构、验收门槛和消融设计。
- [项目问题评估与后续验证报告](reports/project-issues-assessment-2026-11-12.md)：九类现存问题的事实证据、原因与影响、能力边界，以及处理顺序和建议验收方法。
- [阶段 0 正确性与观测基线](correctness-observability-baseline.md)：更新协议、离线检查器和正式验收流程。
- [阶段耗时与数据移动打点](stage-profiling.md)：compute/OSD 路径、指标边界、统计口径和运行入口。
- [阶段 0 集群验收报告](reports/phase0-validation-report-2026-09-22.md)：当前零失败、通过一致性检查的正式结果。
- [阶段 1 Strict Raw-RADOS/OSD A/B 报告](reports/phase1-strict-raw-vs-osd-report-2026-09-30.md)：纯原生 RADOS compute 与 OSD/CLS 的三次独立重复、阶段耗时和瓶颈结论。
- [OSD/CLS 基线（阶段 0–2）与后续优化图解](reports/osd-cls-baseline-visual-summary-2026-11-16.md)：以阶段 2 M2 为主 OSD 基线，明确阶段 1 数据出处，配六张新图展示阶段 3、5A、6 的措施、同期对照、实际收益与回退；区分历史基线值和后续重跑控制组。
- [阶段 2 协议与路由抽象验收报告](reports/phase2-protocol-routing-validation-2026-09-30.md)：schema v1 滚动部署、GIST1M smoke、指标闭合与幂等重放验证。
- [阶段 2 OSD 全量回归报告](reports/phase2-osd-full-regression-report-2026-10-10.md)：四数据集三轮 OSD-only 复测、与阶段 1 OSD 的性能对比及具体代码改动。
- [阶段 3 Frontier 聚合可行性报告](reports/phase3-frontier-feasibility-report-2026-10-10.md)：固定窗口与自适应窗口的 GIST1M 验证、失败根因和停止结论。
- [阶段 5A 只读执行器可行性与重复验证报告](reports/phase5a-read-executor-feasibility-report-2026-10-12.md)：等待窗口扫描、250 μs 三轮正式重复、配对 Recall、异步收益来源和后续并行度门禁。
- [阶段 5A 并发度消融报告](reports/phase5a-concurrency-ablation-report-2026-10-13.md)：执行器线程与更新并发度扫描、异步/合批收益拆分、饱和点和质量门禁限制。
- [阶段 5A 固定轨迹质量与性能门禁报告](reports/phase5a-fixed-trace-validation-report-2026-10-15.md)：冻结同一基础图后的串行精确轨迹、并行三轮质量复核与正式参数决策。
- [阶段 5A 四数据集正式 A/B 报告](reports/phase5a-four-dataset-formal-ab-report-2026-10-27.md)：同基础图配对的 24-case 正式结果、Deep100M P99 失败分析与阶段 6 决策。
- [阶段 6 Deep100M Per-Target Inflight 扫描报告](reports/phase6-deep100m-inflight-scan-report-2026-11-12.md)：固定上限三轮消融、逐 target 尾部观测、稳健性限制与 AIMD 暂缓结论。
- [Compute-node 背景实验报告](reports/compute-background-report-2026-09-23.md)：PPT 背景数据移动实验的零失败复现结果。

## 参考材料

- [开题答辩 PPT](references/proposal-defense.pptx)：研究问题、背景实验和优化设计的原始依据。

## 历史材料

`archive/` 保存修正正确性问题之前的实验记录。这些报告用于追溯 PPT 复现过程，
包含失败更新或非严格 A/B 对照，不能作为当前性能结论：

- [早期 PPT 基线复现](archive/ppt-baseline-reproduction-2026-09-18.md)
- [早期 compute/OSD 对比](archive/compute-vs-osd-comparison-2026-09-22.md)
- [历史混合路径阶段 1 报告](archive/phase1-mixed-path-report-2026-09-25.md)

新增报告应放入 `reports/` 并在本索引登记；被新结果替代但仍有追溯价值的文档移入
`archive/`，不要把临时运行说明或本机路径拆成新的独立文档。
