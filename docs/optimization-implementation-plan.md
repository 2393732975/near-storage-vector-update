# 近存储向量更新优化实施方案

## 1. 修订依据与核心判断

本方案依据开题答辩 PPT 第 14–18 页的三层设计，并以
[2026-09-30 Strict Raw-RADOS/OSD A/B 报告](reports/phase1-strict-raw-vs-osd-report-2026-09-30.md)
冻结 C0/B0 性能基线，以
[阶段 2 OSD 全量回归报告](reports/phase2-osd-full-regression-report-2026-10-10.md)
记录当前 M2 实现状态；PPT 背景数据搬运结论由
[Compute-node 背景实验报告](reports/compute-background-report-2026-09-23.md)
补充。旧的混合 compute/CLS 结果只用于历史追溯，不参与目标设定。

本方案固定两个基线角色：

- **C0（传统存算分离对照）**：全更新链路仅使用原生 librados，候选向量返回
  Coordinator 后算距，CLS 调用严格为 0。
- **B0（冻结的近存储优化起点）**：阶段 1 OSD/CLS 实现，候选向量留在 OSD 内
  算距；它是长期纵向分母，不能把 C0 当作“优化前 OSD”。
- **M2（当前执行起点）**：保持 B0 搜索与放置语义，但加入统一路由、schema v1、
  幂等重放和聚合观测。B1 必须相对同提交的 M2 控制组计算直接收益，同时报告相对
  冻结 B0 的长期变化，避免把基础重构波动记为搜索聚合收益。

严格 A/B 共 24/24 轮、55,023 次成功更新，全部零失败并通过结构与语义检查：

| 数据集 | C0 延迟 | B0 延迟 | B0 延迟变化 | C0/B0 吞吐 | B0 吞吐变化 |
| --- | ---: | ---: | ---: | ---: | ---: |
| GIST1M | 441.18 ms | 345.39 ms | -21.71% | 9.06 / 11.60 | +28.03% |
| Text2Image10M | 495.99 ms | 385.27 ms | -22.32% | 8.06 / 10.43 | +29.39% |
| Deep100M | 579.80 ms | 506.33 ms | -12.67% | 6.90 / 7.89 | +14.33% |
| SIFT100M | 661.78 ms | 515.31 ms | -22.13% | 6.10 / 7.79 | +27.73% |

最新数据支持以下判断：

- C0 的距离阶段为 338–511 ms/update，其中拉取候选向量占 98.79%–99.84%；
  本地算距不是瓶颈。B0 已取得稳定方向性收益，证明近存储算距有价值。
- B0 的距离阶段仍为 241–357 ms/update，约占完整更新的 70%。其中真正算距仅占
  0.07%–0.77%，roundtrip/queue 占 57.01%–85.55%，payload 读取占
  10.20%–36.01%。下一步应优化请求形态和对象访问，而不是算距内核。
- B0 每次更新仍有约 418–590 个距离批次、501–747 次 CLS 调用，每个距离批次
  只有 1.009–1.112 个候选，实质上仍接近逐候选同步 RPC。
- 每次更新触达约 65–363 个数据对象。按当前 modulo 布局，即使单次更新内做到
  完美对象聚合，GIST1M/Text2Image10M/Deep100M/SIFT100M 的理论平均候选数也仅约
  7.19/3.36/1.64/2.03 每对象。因此统一要求 `candidates/batch >= 8` 或
  `distance batches/update < 50` 对 Deep100M/SIFT100M 物理上不可达，必须依赖
  跨更新微批或降低对象扇出的布局改造。
- B0 邻接 patch 为 59–85 ms/update、约 14–17 calls/update，是距离聚合后的第二
  优先级；搜索控制在 Deep100M/SIFT100M 已达到 54/47 ms，也应防止随批窗口扩大。
- B0 global meta 只有约 1.7–1.8 ms/update，且 24 轮无 CAS 重试。元数据改造首先
  服务于恢复和多 Coordinator 扩展，不作为近期平均延迟的主要收益来源。
- M2 四数据集三轮正式复测表明平均延迟和吞吐相对 B0 的变化绝对值均小于 5%；
  CLS calls/update 和 distance batches/update 基本不变。schema v1 固定信封使
  CLS KiB/update 增加约 2.6%–44.1%，但没有改变 RTT/排队与 payload 读取主导的
  瓶颈结构。
- cross-owner edge ratio 为 74.9%–80.0%，并且 Deep100M/SIFT100M 分别触达约
  363/293 个对象；图感知有界布局是突破单次更新聚合上限的必要阶段，不再只是
  可选的末端优化。

因此实施路线分为两条相互约束的主线：

1. **性能主线**：B0 冻结 → M2 协议/路由基础（已完成）→ 单次更新对象聚合 →
   只读异步/跨更新微批 → 邻接写聚合 → 拥塞控制 → 图感知有界布局。
2. **正确性主线**：现有幂等基线 → schema v1（已完成）→ intent/saga 与恢复器 →
   多 Coordinator 元数据扩展。

单次更新的只读聚合可以先行；跨 update 的修改型微批必须在恢复协议通过后启用。
所有性能结论必须同时满足零失败、图一致性检查和配对 Recall 门槛。

## 2. 目标架构与 Ceph 边界

```text
Update Coordinator
  ├─ Update Protocol：版本、幂等、intent/saga、恢复
  ├─ Windowed Graph Search：有界 frontier、邻居去重、按对象合并
  ├─ Remote Executor
  │    ├─ per-object 只读队列与 deadline
  │    ├─ 单 update 同 query 聚合、跨 update 子请求微批
  │    ├─ 修改型队列（通过 saga 门禁后启用）
  │    └─ per-target 拥塞窗口
  └─ Placement Manager
       ├─ global_id → group/chunk/object
       └─ 局部性、容量和负载约束

Ceph/RADOS
  ├─ Global Header 与 ID Allocator
  └─ Locality Group Objects
       ├─ payload、VectorRef、adjacency
       ├─ batch distance / frontier expansion
       ├─ idempotent patch
       └─ shard meta 与 update intent
```

必须遵守以下执行边界：

- CLS 一次只能操作当前对象；同一 PG 不等于能跨对象原子读取或写入。
- 修改型 CLS 必须走 RADOS primary 和正常副本协议，不能直接向多个副本下发写。
- 聚合键至少包含 pool、locator、object、opcode 和 schema version，不能只按 OSD。
- 同对象邻居可在 `expand_frontier_batch` 内融合；跨对象邻居必须返回 ID，由
  Coordinator 重新按目标对象分组后批量算距。
- 必须分别统计“单 query 候选数/对象批次”和“跨 update 子请求数/RPC”。跨 update
  微批减少物理调用数，但不同 query 不能合并成一个语义距离请求，也不会自然减少
  query bytes。

## 3. 阶段 0：正确性与观测基线（已完成）

阶段 0 已完成动态 M、原子 label CAS、失败分类、低频探针、幂等超时重放和离线
一致性检查器。最新严格 A/B 再次覆盖这些门禁：四数据集、两种路径、三次重复共
55,023 次成功更新，24/24 轮均零失败且检查通过。

现阶段仍需保留的基线约束：

- label 只能指向 ACTIVE 节点；global ID 唯一；payload、VectorRef 和 adjacency
  完整；节点度数满足配置。
- 每轮记录 failures、分类重试、request/reply bytes、调用数、批大小、对象/PG/OSD
  fan-out、跨 owner 边比例及各阶段 P50/P95/P99。
- `timed_noop` 只能低频采样，不能每批调用并污染正式结果。
- 当前幂等机制能处理客户端超时重放，但还不等于进程崩溃后可恢复的跨对象事务。

## 4. 阶段 1：建立可比较的性能与质量基线（已完成）

阶段 1 已在提交 `b74a897` 上完成。C0 与 B0 使用同一代码、数据、池布局和 runner
交错执行，结果归档于最新严格 A/B 报告。后续优化不得重新引用旧混合路径数据。

### 4.1 严格 A/B

同一轮实验必须固定 Git commit、Coordinator/Importer/CLS 二进制、数据切分、池
配置、PG 数、CRUSH 落点和并发度，并交错运行 `compute` 与 `osd`。每个配置至少
三次**重新导入后的独立重复**；冷启动与预热结果分开报告，不把同一索引上连续
三轮当独立样本。每次运行创建唯一 `RUN_ROOT`。

compute 的验收条件为 `storage_access_mode=raw_rados`、
`total_cls_exec_calls=0` 且 `total_raw_rados_calls>0`；OSD 路径必须实际产生 CLS
调用。未通过访问路径门禁的轮次不得进入 A/B 汇总。

### 4.2 补齐质量和协议指标

- 使用数据集 ground truth 报告 Recall@10；没有 Recall 的性能结果只能用于诊断。
- 导入器必须把 hnswlib internal ID 显式转换成 external label 后再持久化；每轮以
  256 个节点的 level-0 图边对随机边胜率（至少 0.60）做独立语义门禁。
- 分 opcode 记录 calls/update、request/reply bytes、query bytes、候选数、batch
  fill ratio、batch wait、client roundtrip 和 CLS service time。
- 明确区分 VectorRef/payload I/O、OSD 排队、网络/librados 与 Coordinator 调度；
  `roundtrip - CLS internal` 只能称为 roundtrip/queue，不能直接称为网络时间。
- 分别统计 adjacency read、distance、new adjacency、patch、meta 和状态切换调用。

退出条件已经满足：24/24 轮零失败，结构与语义一致性检查通过；Recall@10 可用；
每个数据集/模式有三次独立样本并报告 95% 置信区间。修复 ID 映射或新 chunk 创建
语义之前的数据只能作为故障诊断样本。

阶段 2 已补齐**相同 query 前缀的配对质量检查**。固定时间窗下不同路径处理数量
不同，非配对 Recall 差值仍不能单独证明语义等价；B1 起必须继续在共同前缀上计算
paired Recall delta，不得只引用各自完整时间窗的静态 Recall。

## 5. 阶段 2：聚合前置的最小协议与路由抽象

这一阶段只做阶段 3 所需的最小重构，时间上限为一个开发迭代，不能演变为先重写
整个元数据系统。当前 `GroupIds` 已能按 owner/chunk 分组；在此基础上抽出稳定的
对象路由接口，并增加距离对象扇出的专用指标：

```cpp
struct PhysicalLocation {
  uint32_t group_id;
  uint32_t shard_id;
  uint64_t chunk_id;
  uint64_t placement_epoch;
};

class PlacementResolver {
 public:
  PlacementResolver(
      uint32_t shard_count,
      uint64_t points_per_object,
      uint64_t placement_epoch = 0);
  PhysicalLocation Resolve(uint64_t global_id);
  PhysicalLocation SelectForInsert(uint64_t global_id);
  uint32_t ResolveLabelShard(uint64_t external_label);
};
```

第一版 resolver 继续实现现有 modulo 规则。新 CLS 协议统一携带 magic、
schema_version、opcode、request_id；写请求再携带 update_id、expected_version
和 placement_epoch。未知版本必须明确拒绝，不能静默按旧结构解码。

新增观测字段至少包括 `unique_distance_objects/update`、
`candidates_per_unique_distance_object`、frontier window fill、去重前后候选数和
每对象批次大小直方图。现有 `avg_unique_data_objects_per_update_attempt` 混合了
邻接、距离和写路径，不能直接作为距离批次的精确理论下限。

退出条件：modulo 模式的对象映射逐 ID 等价；失败注入仍可幂等重放；新指标可
代数闭合；四数据集三次独立复测均零失败并通过一致性检查；重构后相对 B0 的平均
延迟和吞吐变化均不劣于 5%。

### 5.1 最终实施状态（2026-10-10）

阶段 2 的代码改造已经完成：importer、Coordinator 和 checker 共用保持原 modulo
语义的 `PlacementResolver`；CLS schema v1 的请求与响应统一携带 magic、opcode、
request ID 和 placement epoch，在线写请求进一步传递 update ID 或 expected
version，未知 schema/opcode 会明确拒绝。严格 A/B runner 还会在两条路径的最长
共同 query 前缀上执行 Recall@10 回归门禁。

观测已补齐 distance-only 唯一对象数、候选/对象、每对象批大小直方图、frontier
window fill，以及 visited 去重前后的候选数。modulo 边界单测、协议 round-trip、
未知版本拒绝、Python 单测和全量编译均已通过。

集群验收已于 2026-09-30 完成。schema v1 CLS 已滚动部署到 5 个 OSD，逐 OSD
恢复门禁均满足 5/5 `up/in`、全部 PG `active+clean`。GIST1M 严格 smoke 的
compute/OSD 两条路径分别完成 2,421/3,197 次更新，均为 0 失败、结构与语义检查
通过；共同 1,000-query 前缀的 OSD Recall@10 相对 compute 变化为 -0.003，低于
0.005 门槛。

新增指标的直方图计数、批次和候选数可代数闭合，schema 版本为 1；字节级相同的
reserve/finalize 请求在模拟响应丢失后重放，global meta 均只推进一次。GIST1M
smoke 最初显示延迟 -2.16%、吞吐 +1.98%；随后完成的四数据集、三轮 OSD-only
正式回归显示平均延迟变化为 -2.06% 至 +4.68%，吞吐变化为 -4.91% 至 +2.40%，
全部满足性能变化不劣于 5% 的退出条件。调用数和距离批次数保持不变，协议信封的
固定字节开销未改变主要瓶颈。M2 状态更新为“完成”，下一步进入阶段 3 的有界
frontier 聚合；完整证据见阶段 2 集群验收报告与阶段 2 OSD 全量回归报告。

正式回归共 12/12 轮、29,992 次成功更新，所有轮次 `failed_updates=0`，12 次
离线检查全部 `pass`、0 error、0 warning，图边语义胜率为 0.994480–1.000000：

| 数据集 | B0 延迟 | M2 延迟 | 延迟变化 | B0/M2 吞吐 | 吞吐变化 |
| --- | ---: | ---: | ---: | ---: | ---: |
| GIST1M | 345.387 ms | 350.664 ms | +1.53% | 11.601 / 11.427 | -1.50% |
| Text2Image10M | 385.269 ms | 403.281 ms | +4.68% | 10.427 / 9.915 | -4.91% |
| Deep100M | 506.332 ms | 523.354 ms | +3.36% | 7.894 / 7.667 | -2.88% |
| SIFT100M | 515.311 ms | 504.720 ms | -2.06% | 7.788 / 7.975 | +2.40% |

Deep100M 和 SIFT100M 第三轮存在 P99 长尾，运行期间同时出现 BlueStore slow-op
指示；因此 M2 的退出结论限定为平均延迟/吞吐无系统性回退，不能宣称 P99 已改善。
schema v1 的固定字段使低维数据集的相对字节增幅更明显；阶段 3 减少物理调用时
必须同时验证 calls、query bytes 和 P99，不能只看平均延迟。

## 6. 阶段 3：最高优先级——单次更新的读路径聚合

这是最新数据指向的首个性能改动。现有 `SearchLayer` 虽然调用
`DistanceToMany`，但一次只展开一个 frontier 节点；当邻居按 owner/chunk 分组后，
每个对象通常只剩一个候选。第一版直接复用现有 `get_node_adjacency_batch` 和
`distance_to_local_batch`，先改 Coordinator 调用形态，不立即增加融合 CLS。

### 6.1 固定窗口首轮验证与计划修正（2026-10-10）

提交 `53284fc` 已实现默认关闭的 level-0 窗口、批量邻接读取、合并后 visited
去重、按对象 batch distance、稳定 tie-break 和投机工作指标。`W=1` 保持 M2
控制语义；`W>1` 才启用窗口。GIST1M、OSD、30 s、并发度 4 的同提交 smoke 如下：

| 窗口 | 更新数 | 候选/update | distance batches/update | candidates/batch | 平均延迟 | P99 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 240 | 502.35 | 455.47 | 1.103 | 452.96 ms | 737.97 ms |
| 4 | 229 | 592.30 | 458.89 | 1.291 | 476.53 ms | 909.28 ms |
| 8 | 210 | 702.05 | 454.59 | 1.544 | 518.49 ms | 847.69 ms |

三轮均为零失败、checker `pass`；W4/W8 的共同前缀 Recall 没有退化。但 W4
虽然把额外候选控制在 17.9%，distance batch 反而增加 0.75%，平均延迟/P99
分别回退 5.2%/23.2%；W8 的候选增加 39.8%，batch 仅下降 0.19%，平均延迟/P99
回退 14.5%/14.9%。W8 的距离对象扇出仍约 64.93/update，与 W1 的 64.66 基本
相同；query bytes 也只下降 0.19%。因此固定 `W=4/8/16/32` 扫描不能作为 B1：
更大的窗口提高了批内密度，却用投机距离工作抵消了物理调用收益。W16/W32 不再
继续执行。

### 6.2 自适应有界 frontier 窗口

1. `W=1` 继续作为默认值和同提交 M2 控制组；固定 W 只保留为诊断开关，不直接
   作为正式优化配置。
2. level 0 可查看最多 8 个有序候选，上层 `GreedySearch` 始终串行。先按
   `(distance, global_id)` 排序，并用 placement resolver 计算 adjacency 对象；
   未被选中的 lookahead 必须原样放回候选堆。
3. 批量读取 lookahead 邻接后，对每个候选前缀估算距离对象调用减少量、合并后的
   唯一邻居数和新增 payload 工作。收益模型使用 W1 实测的每批
   roundtrip/queue 与每候选 payload/compute 成本；只有预计节省为正且带安全余量
   时才扩大窗口，否则本次展开退化为 W1。
4. 每次 update 的窗口外推节点和新增唯一候选均受预算约束；滚动候选膨胀不得超过
   W1 控制统计的 20%。若实际 distance batches 未下降或候选预算超限，熔断后续
   自适应展开并记录原因，禁止用无界额外工作换取更大的 candidates/batch。
5. 合并邻居 ID 后先做 visited 去重，再按**距离数据对象**分组；同一对象的一批
   candidate 只携带一次 query。第一版保持同步提交，阶段 5 再引入异步与跨 update
   微批，避免同时改变搜索语义和排队。
6. 新增 `lookahead_nodes`、`accepted_frontier_nodes`、`rejected_no_gain`、
   `predicted_batches_saved`、`incremental_candidates` 和熔断计数；报告必须同时展示
   候选膨胀、物理 batch、query bytes、平均延迟和 P99。

### 6.3 对象扇出约束下的验收

M2 的距离批次、候选和 distance-only 对象扇出如下。最后一列是用正式三轮数据
估算的单次 update 完美对象合并上限；首轮 smoke 后将其定位为长期理论参考，而非
阶段 3 同步窗口可直接达到的执行基线。

| 数据集 | M2 distance batches/update | candidates/batch | distance objects/update | 候选/对象上限 | 完美合并批次降幅 |
| --- | ---: | ---: | ---: | ---: | ---: |
| GIST1M | 418.49 | 1.114 | 64.64 | 7.21 | 84.6% |
| Text2Image10M | 502.91 | 1.030 | 154.22 | 3.36 | 69.3% |
| Deep100M | 587.69 | 1.009 | 361.25 | 1.64 | 38.5% |
| SIFT100M | 582.59 | 1.017 | 291.34 | 2.03 | 50.0% |

这里的“完美合并”是整次 update 的理论下限；首轮 smoke 证明同步有界窗口无法在
不做投机工作的情况下直接达到它。因此它移作 B5 的长期参考，不再作为 M3 的直接
退出线。阶段 3 采用两级门禁：

- GIST1M feasibility smoke 必须相对同提交 W1 将 distance batches 和 query bytes
  至少降低 10%，候选膨胀不超过 20%，平均延迟与 P99 均不回退；否则不得启动
  四数据集正式实验；
- 正式 B1 要求四数据集 distance batches/query bytes 均下降，distance
  roundtrip/queue 加权下降至少 10%，四数据集加权平均延迟下降至少 5%，任何
  数据集平均延迟或 P99 不回退超过 5%，同时报告相对冻结 B0 的长期变化；
- 共同 query 前缀的 Recall@10 下降不超过 0.5 pp，零失败且一致性检查通过。

达到 feasibility gate 后，再根据实际窗口可合并率设定 M3 最终 batch 降幅目标，
不得继续用整次 update 的唯一对象数虚构同步窗口可以达到的收益。如果自适应窗口
仍过不了 gate，应把结果记录为 Coordinator-only 聚合的负结论，并提前评估阶段 5
的异步重叠或阶段 7 的布局，而不是放宽正确性和尾延迟门禁。

### 6.4 自适应窗口执行结果与阶段结论（2026-10-10）

提交 `a96b608` 实现了对象收益预测、累计候选预算、邻接预取缓存、被拒绝候选回堆
和观测指标。首轮 smoke 暴露自适应分支绕过 HNSW `lower_bound` 终止条件的问题；
提交 `2f7c60f` 修复并增加回归测试。缺陷轮只用于诊断，不纳入性能判断。

修复后以同一提交分别重导入 GIST1M，运行 OSD-only、30 s、并发度 4。控制组为
W1；自适应组均为 W8、20% 候选预算和 25% 最低预计批次收益：

| 配置 | 更新数 | 候选膨胀 | distance batches/update | query KiB/update | 平均延迟 | P99 | 吞吐 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| W1 | 244 | 0% | 458.94 | 1721.03 | 446.88 ms | 761.70 ms | 8.891/s |
| 自适应，slack=0.25 | 224 | 4.20% | 443.67 (-3.33%) | 1663.74 (-3.33%) | 485.46 ms (+8.63%) | 866.45 ms (+13.75%) | 8.183/s (-7.96%) |
| 自适应，slack=1.00 | 236 | 12.52% | 453.48 (-1.19%) | 1700.56 (-1.19%) | 461.04 ms (+3.17%) | 780.44 ms (+2.46%) | 8.633/s (-2.89%) |

三组均零失败、checker `pass`，共同 query 前缀 Recall@10 未退化。两种自适应配置
均未达到 batches/query bytes 至少下降 10% 且延迟不回退的 feasibility gate。
slack=0.25 的邻接读取从 58.64 增到 64.88 calls/update；slack=1.00 增到
81.49 calls/update。同步决策必须先读取大量 lookahead 邻接，但最终只能获得有限的
距离对象复用，预取和投机工作抵消了 1.19%–3.33% 的批次收益。

因此 M3 以“Coordinator-only 同步 frontier 聚合不可行”的负结论关闭，不启动四
数据集正式实验，也不放宽门禁。`FRONTIER_ADAPTIVE` 保持默认关闭，代码与指标仅
作为诊断基础。性能主线下一步提前评估阶段 5 的异步/跨 update 只读微批；若仍受
对象扇出限制，再进入阶段 7 的图感知有界布局。完整证据见
[阶段 3 Frontier 聚合可行性报告](reports/phase3-frontier-feasibility-report-2026-10-10.md)。

### 6.5 可选的同对象融合

只有 Coordinator 聚合达到对象扇出下限后，再增加只读
`expand_frontier_batch`：在一个对象内读取多个 frontier 的邻接，直接计算其中
**同对象**邻居的距离并返回 `neighbor_id + distance + version`；跨对象邻居只返回
ID，交由 Coordinator 再分组。CLS 不能假设可读取另一个对象。

第一版不把 query 持久写入 OMAP：这会给只读路径增加写放大、清理和故障语义。
若聚合后仍受 query bytes 限制，再单独消融短生命周期、可失效的 query handle。

## 7. 阶段 4：可恢复更新与可扩展元数据

此阶段首先解决正确性和多 Coordinator 扩展，不再预设它能带来 30%–50% 的当前
平均延迟收益。B0 的 global meta 仅约 1.7–1.8 ms/update，优化收益应由实测决定。

### 7.1 状态拆分

- **Global Header**：entrypoint、max_level、M、ef、维度、类型、metric、schema
  和 header_epoch。普通插入只读缓存；只有提高层级或更改配置时 CAS。
- **ID Allocator**：Coordinator 按 1024/4096 个 ID 申请租约；并发度较低时先保留
  可配置开关，只有 allocator 成为热点后再默认启用。
- **Shard Meta**：local_epoch、live/stale count、committed_updates、bytes_used。
- **Update Intent**：update_id、旧/新节点、目标对象集合、阶段和逐对象状态。

### 7.2 Saga 状态机

一次覆盖更新执行 PREPARE 新节点 → 持久化 intent → 幂等 patch → 激活新节点 →
CAS label → 标记旧节点 STALE → 更新 shard meta → COMMIT intent。跨对象操作不
假设原子性；恢复器扫描未完成 intent，优先向前完成，无法完成的 PREPARED 节点
转为 ABORTED 并回收。所有修改型批次必须返回逐项状态，单项冲突不能重试整批。

退出条件：对每个提交阶段执行 kill/timeout 故障注入后均能恢复；无 active
orphan、重复 patch 或计数漂移；普通更新的 Global Header 写接近 0。只有达到这些
条件，阶段 5 才能开启跨 update 的修改型聚合。

## 8. 阶段 5：异步 RemoteExecutor 与两级微批

建立共享 RemoteExecutor，为每个目标对象维护读写队列、batch builder、异步提交
和 completion dispatcher。逻辑请求通过 future/promise 收取逐项结果。

读执行器不修改持久状态，可在阶段 4 故障恢复工作并行开发；修改型队列必须等
intent/saga 退出条件满足后才能启用。异步化的目标是重叠独立对象等待，不允许
改变 HNSW frontier 的依赖顺序或用无限 inflight 掩盖小请求问题。

### 8.1 读批次

单次 update 的 query-aware 合并优先；跨 update 微批可在同一 object/opcode RPC
中携带多个子请求，但每个子请求有独立 query 和候选集合。初始参数扫描范围为
50–500 微秒、256 KiB–1 MiB、256–2048 个候选，并受最老请求 deadline 约束。

跨 update 合并只减少物理 RPC 数，不提高“单 query candidates/batch”的布局上限，
也不必然减少 query bytes；每个子请求仍必须携带自己的 query。因此同时记录
`subrequests/RPC`、`candidates/subrequest`、`queries/RPC` 和物理 calls/update，
禁止把多 query 装入同一 RPC 后误报为同一批候选填充率提高。

### 8.2 写批次

将 edge patch 按对象和版本聚合，协议携带逐项 update_id、expected_version 和
返回码。不同对象仍是独立请求；发生局部冲突只重放对应项目。new adjacency、
label CAS 和生命周期切换不为追求批量而破坏 saga 顺序。

### 8.3 阶段 5A 实施状态与门禁（2026-10-12）

阶段 3 已证明同步 frontier 扩窗不能稳定降低延迟，因此先实现不改变搜索语义的
只读执行器。当前实现新增 `distance_multi_query_batch` 只读 CLS 方法和共享
`DistanceReadExecutor`：Coordinator 先按 owner/chunk 拆分一次逻辑距离请求，
并发提交独立对象；执行器再按同一物理对象聚合来自不同 update 的子请求。OSD
只对重叠 candidate ID 复用 VectorRef/payload 读取，每个 query 独立算距并返回
逐子请求状态。HNSW frontier 的产生和终止顺序不变。

第一版默认关闭，初始参数为等待 100 μs、256 KiB、256 candidates、32 个
subrequests 和 5 个执行线程。它只允许 `mode=update`、`distance-mode=osd`；runner
进一步要求 `MODES=osd`，防止污染纯 Raw-RADOS 的 C0。新增观测包括物理
multi-query RPC、逻辑 subrequest、queries/RPC、candidates/subrequest、候选复用
率、微批排队时间和批大小直方图。修改型请求仍走 M2 原路径，未绕过阶段 4 门禁。

代码完成后先执行 GIST1M 30 s smoke，要求零失败、checker 通过、multi-query
RPC 大于 0；然后在同一提交上对 M2 控制组和 50/100/250/500 μs 等候窗口做独立
重导入对照。可行性门槛为距离等待或端到端平均延迟至少降低 5%、P99 不回退超过
5%、静态 Recall 与语义检查通过。`subrequests/RPC` 必须如实报告；如果它接近 1
但延迟改善，结论只能归因于独立对象异步重叠，不能宣称跨 update 合批成功。若
平均延迟无收益或 P99 恶化，则关闭 Stage 5A 默认开关并提前进入阶段 7 布局验证。

### 8.4 阶段 5A 单轮可行性结果（2026-10-12）

提交 `35f7f1f` 上完成同提交 M2 控制组和 50/100/250/500 μs 等待窗口的 GIST1M
独立重导入对照。更新并发度固定为 4，执行器线程为 5。四个异步配置相对控制组的
平均延迟降低 20.35%–23.19%，P99 降低 10.93%–17.23%，吞吐提高
25.30%–30.01%；所有轮次零失败且 checker 通过。

收益来自异步重叠而非充分合批：`subrequests/RPC` 仅 1.108–1.116，约 89% 的
物理 RPC 只有一个子请求，候选读取复用率低于 0.11%。distance batches/update
只下降 7.65%–8.90%，但距离 RTT/排队由 238.61 降至 123.38–133.28 ms/update。
因此 5A 证明了只读异步执行器的方向可行，没有达到完整阶段 5 的调用数目标。

250 μs 是唯一通过单轮共同 235-query Recall 门禁的候选值，差值为 -0.468 pp；
其余窗口为 -0.638 至 -2.511 pp。等待窗口不改变搜索语义，这些差异必须通过独立
重复和可复现调度复核，不能直接归因为算法退化，也不能忽略门禁。默认开关继续
关闭。下一步先做 250 μs 至少三次重复，再扫描执行器线程 `1/2/5/10` 和更新并发
度 `1/2/4/8`；通过后才运行四数据集正式实验。完整证据见
[阶段 5A 只读执行器可行性报告](reports/phase5a-read-executor-feasibility-report-2026-10-12.md)。

完整阶段 5 的退出条件（不是 5A 只读 feasibility gate）：全部 CLS calls/update
相对 M2 的 501–747 降低至少 60%；patch calls/update 从约 14–17 降至 5 以下，
patch 阶段占完整更新低于 10%；只读异步使
阶段 3 的独立对象等待得到重叠，但 P99 不回退；写批次零失败且故障恢复测试通过。

## 9. 阶段 6：拥塞感知调度

请求数量下降后，再用真实 queue 指标调节并发，避免用拥塞控制掩盖过多小 RPC。
每目标维护 RTT、CLS service、推导 queue、inflight、cwnd、batch limit 和 timeout
计数。第一版采用 AIMD：低 queue 时缓慢增加 cwnd；queue P95、超时或 slow-op
增加时窗口减半；接近 deadline 的批次立即刷新。

OSD 映射不可得或 CRUSH 变化时，以 target object 为控制粒度，不使用陈旧 OSD ID
强制路由。只读副本执行作为独立消融，优先使用 Ceph 原生 replica-read 能力；
修改请求不进行“多副本客户端并行下发”。

退出条件：零 timeout、零失败；queue estimate P95 和 update P99 相对阶段 5 至少
降低 20%，吞吐增益不能以尾延迟恶化换取。

## 10. 阶段 7：图结构感知的有界对象布局

当前 cross-owner edge ratio 为 74.9%–80.0%；一次 B0 更新平均触达约 65–363 个
数据对象，Deep100M/SIFT100M 即使完美做单次更新对象聚合也只有约 1.64/2.03 个
候选/对象。布局改造因此承担明确职责：降低**对象扇出**并提高同 query、同对象的
候选密度。只映射到同一 PG/OSD 而仍分散在不同对象，不能减少 CLS 调用。

插入时按下式选择 locality group：

```text
Locality(v,g) = Σ level_weight(u,v) × I[group(u)=g] / Σ level_weight(u,v)
Load(g) = max(nodes/Smax, bytes/Bmax, omap_keys/Kmax,
              update_rate/Lmax, queue/QueueMax)
Score(v,g) = Locality(v,g) - λ × Load(g)
```

同 group 使用相同 locator key；group 内再切分有界 chunk object，并对节点数、
payload bytes、OMAP key 数和更新率设置硬上限，避免大型 OMAP 对象。先只对新增
节点启用 graph-aware 选择；随后对初始图做离线 greedy partition。第一版不做
在线迁移和边界副本，避免提前引入双读、placement epoch 迁移和副本失效协议。

退出条件：cross-group edge ratio 和 unique distance objects/update 相对 modulo
降低至少 30%，所有数据集的单 query candidates/batch 达到至少 4，并以 8 为目标；
在布局与跨更新微批共同作用后，distance batches/update 最终降至 80 以下。对象
大小、OMAP key 数、OSD 容量与队列保持均衡，共同前缀 Recall 不下降超过 0.5 pp。

## 11. 消融实验与总体验收

| 版本 | 路径角色 | 读路径聚合 | 恢复协议 | 异步/写微批 | 拥塞 | 布局 |
| --- | --- | --- | --- | --- | --- | --- |
| C0 | 传统 compute 对照 | 无 | 单进程 raw 幂等 | 无 | 无 | modulo |
| B0 | 冻结 OSD 基线 | 逐 frontier | 旧 CLS 幂等 | 无 | 无 | modulo |
| M2 | 当前执行控制组 | 逐 frontier | schema v1 + 幂等重放 | 无 | 无 | modulo |
| B1 | OSD 优化 | 有界 frontier + 对象聚合 | schema v1 + 幂等重放 | 无 | 无 | modulo |
| B2 | OSD 优化 | B1 | intent/saga | 无 | 无 | modulo |
| B3 | OSD 优化 | B1 | intent/saga | RemoteExecutor | 无 | modulo |
| B4 | OSD 优化 | B1 | intent/saga | RemoteExecutor | AIMD | modulo |
| B5 | OSD 优化 | B1 | intent/saga | RemoteExecutor | AIMD | graph-aware |

每个版本运行四数据集、并发度 1/4/8/16；B1–B5 与同提交的 M2 控制组做交错
对照，每个正式配置至少三次独立重导入，并额外报告相对冻结 B0 的长期变化。C0
在每个主要里程碑重跑，用来确认集群环境漂移，但不作为 B1–B5 的直接优化分母。
主报告统一使用
`fresh-pool-after-import`；在建立安全且可复现的缓存清理方法前，不宣称 cold-cache
结果。PPT 中各层收益比例只作待验证假设，不能把元数据层 30%–50% 当作预测。

最终综合目标：

- 相对当前 OSD/CLS B0，平均更新延迟降低至少 30%，P99 降低至少 20%，吞吐提高
  至少 30%；同时单独报告相对传统 C0 的端到端收益；
- 阶段 3 先达到对象扇出约束下的分数据集门槛；B5 最终达到 distance
  batches/update < 80、单 query candidates/batch ≥ 4（目标 8）、总 CLS
  calls/update 降低 ≥ 60%、query bytes/update 降低 ≥ 80%；
- patch calls/update ≤ 5，Global Header writes/update 接近 0；
- 所有正式轮次零失败、一致性检查通过，共同 query 前缀 Recall@10 损失 ≤ 0.5 pp；
- 不通过关闭告警、放宽超时、减少检查或只挑选预热轮次获得性能结论。

## 12. 里程碑与交付物

| 里程碑 | 主要交付物 | 退出条件 |
| --- | --- | --- |
| M0 已完成 | 正确性检查、失败分类、幂等重放 | 最新 24/24 轮复验通过 |
| M1 已完成 | C0/B0 严格 A/B、Recall、调用/字节/阶段指标 | 四数据集各三次独立重复 |
| M2 已完成 | PlacementResolver、schema v1、幂等重放、distance-only fan-out | 12/12 轮通过，平均延迟/吞吐变化 < 5% |
| M3 已关闭（负结论） | 自适应 level-0 frontier、按对象 batch distance | 同步扩窗门禁失败，默认关闭 |
| M4 未开始 | intent/saga、恢复器、元数据拆分 | crash injection 通过 |
| M5A 进行中 | 只读异步 RemoteExecutor、multi-query 微批 | 单轮 GIST 性能门禁通过，待重复与并行度消融 |
| M5B 被 M4 阻塞 | 幂等邻接写微批 | M4 通过后验证 calls/patch/P99 |
| M6 拥塞控制 | per-target AIMD 与 deadline flush | queue/P99 达标 |
| M7 图感知布局 | locality group、bounded object layout | distance object fan-out 降低 ≥30% |
| M8 集成评估 | B0–B5 消融、统计报告和图表 | 综合目标验证 |

## 13. Git 实施拆分

每个提交只完成一个可验证的逻辑单元。阶段 2 已完成：

1. `93ee0c7 feat(metrics): track distance object fanout`
2. `6552ede test(quality): compare recall on a shared query prefix`
3. `ed5bf8d refactor(storage): introduce minimal placement resolver`
4. `e40e5f2 feat(protocol): version CLS requests and responses`
5. `3d9f3a3 feat(metrics): expose frontier and dedupe efficiency`
6. `bdfffb8 test(cls): add live idempotent replay probe`

后续建议顺序如下：

1. `53284fc feat(search): batch bounded frontier expansion`（已完成；固定 W 性能门禁失败）
2. `a96b608 feat(search): budget adaptive frontier expansion`（已完成；门禁失败）
3. `2f7c60f fix(search): preserve adaptive frontier termination`（已完成）
4. `feat(cls): fuse same-object frontier expansion`（仅在 Coordinator gate 通过后）
5. `feat(metadata): persist recoverable update intents`
6. `feat(recovery): replay incomplete vector updates`
7. `7640e80 feat(cls): add multi-query distance batches`（已完成）
8. `da6ec8f feat(executor): batch asynchronous distance reads`（已完成；进入并行度消融）
9. `feat(cls): batch idempotent edge patches`
10. `feat(scheduler): adapt per-target concurrency`
11. `feat(layout): place nodes in bounded locality groups`
12. `test(experiments): add c0-b5 ablation suite`

每个阶段先运行单元测试、小数据集正确性、超时/崩溃注入，再运行四数据集正式实验。
性能提交和正确性提交不混合，实验报告必须记录完整 commit、二进制哈希和原始结果
目录；数据集、keyring、日志、构建产物和原始大结果不得提交到 Git。
