# 近存储向量更新优化实施方案

## 1. 修订依据与核心判断

本方案依据开题答辩 PPT 第 14–18 页的三层设计，并以
[2026-09-30 Strict Raw-RADOS/OSD A/B 报告](reports/phase1-strict-raw-vs-osd-report-2026-09-30.md)
作为当前唯一性能基线；PPT 背景数据搬运结论由
[Compute-node 背景实验报告](reports/compute-background-report-2026-09-23.md)
补充。旧的混合 compute/CLS 结果只用于历史追溯，不参与目标设定。

本方案固定两个基线角色：

- **C0（传统存算分离对照）**：全更新链路仅使用原生 librados，候选向量返回
  Coordinator 后算距，CLS 调用严格为 0。
- **B0（近存储优化起点）**：当前 OSD/CLS 实现，候选向量留在 OSD 内算距；后续
  B1–B5 的增益均相对 B0 计算，不能把 C0 当作“优化前 OSD”。

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
- cross-owner edge ratio 为 74.9%–80.0%，并且 Deep100M/SIFT100M 分别触达约
  363/293 个对象；图感知有界布局是突破单次更新聚合上限的必要阶段，不再只是
  可选的末端优化。

因此实施路线分为两条相互约束的主线：

1. **性能主线**：B0 冻结 → 单次更新对象聚合 → 只读异步/跨更新微批 → 邻接写
   聚合 → 拥塞控制 → 图感知有界布局。
2. **正确性主线**：现有幂等基线 → 协议版本化 → intent/saga 与恢复器 →
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

尚需作为 B1 前置补充的不是重跑 B0，而是增加**相同 query 前缀的配对质量检查**。
固定时间窗下 C0/B0 处理数量不同，现有 -0.491 至 +0.681 pp 的 Recall 差值不能
单独证明语义等价；B1 起必须额外在共同前缀上计算 paired Recall delta。

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
  PhysicalLocation Resolve(uint64_t global_id);
  PhysicalLocation SelectForInsert(
      const NeighborSet& neighbors,
      const LoadSnapshot& load);
};
```

第一版 resolver 继续实现现有 modulo 规则。新 CLS 协议统一携带 magic、
schema_version、opcode、request_id；写请求再携带 update_id、expected_version
和 placement_epoch。未知版本必须明确拒绝，不能静默按旧结构解码。

新增观测字段至少包括 `unique_distance_objects/update`、
`candidates_per_unique_distance_object`、frontier window fill、去重前后候选数和
每对象批次大小直方图。现有 `avg_unique_data_objects_per_update_attempt` 混合了
邻接、距离和写路径，不能直接作为距离批次的精确理论下限。

退出条件：modulo 模式的对象映射逐 ID 等价；失败注入仍可幂等重放；新指标在
GIST1M smoke 中可闭合；重构后 B0 延迟和吞吐变化不超过 5%。

## 6. 阶段 3：最高优先级——单次更新的读路径聚合

这是最新数据指向的首个性能改动。现有 `SearchLayer` 虽然调用
`DistanceToMany`，但一次只展开一个 frontier 节点；当邻居按 owner/chunk 分组后，
每个对象通常只剩一个候选。第一版直接复用现有 `get_node_adjacency_batch` 和
`distance_to_local_batch`，先改 Coordinator 调用形态，不立即增加融合 CLS。

### 6.1 有界 frontier 窗口

1. 保留标准 HNSW 最小距离候选作为窗口头，根据当前 lower bound 一次选择最多
   `W=4/8/16/32` 个可安全或可投机展开的 frontier 节点；上层 `GreedySearch`
   仍保持串行，先只优化占主要工作的 level 0。
2. 对窗口内节点去重，按 adjacency 对象分组，一次调用读取同对象多个邻接表。
3. 合并邻居 ID 后先做 visited 去重，再按**距离数据对象**分组；同一对象的一批
   candidate 只携带一次 query。
4. 批内和 completion 合并都使用 `(distance, global_id)` 稳定排序，确保同距离
   tie-break 可复现；记录额外投机展开的节点数，避免用无界额外工作换 RPC 数。
5. 第一版同步提交对象批次，用于隔离“聚合”本身的收益；阶段 5 再并行提交和跨
   update 微批，避免一次同时改变搜索语义、并发和排队。

### 6.2 对象扇出约束下的验收

B0 的距离批次、候选和全部数据对象扇出如下。最后一列是用当前数据估算的单次
update 完美对象合并上限；正式实现以阶段 2 新增的 distance-only fan-out 为准。

| 数据集 | B0 distance batches/update | candidates/batch | data objects/update | 候选/对象上限 | 完美合并批次降幅 |
| --- | ---: | ---: | ---: | ---: | ---: |
| GIST1M | 418 | 1.112 | 65 | 7.19 | 84.5% |
| Text2Image10M | 505 | 1.030 | 155 | 3.36 | 69.3% |
| Deep100M | 590 | 1.009 | 363 | 1.64 | 38.3% |
| SIFT100M | 585 | 1.016 | 293 | 2.03 | 49.8% |

因此阶段 3 不再使用所有数据集统一 `<50 batches/update`、`>=8 candidates/batch`
的不可达门槛。退出条件改为：

- distance batches/update 不高于 `1.2 × unique_distance_objects/update`，并且相对
  B0：GIST1M 降低至少 75%、Text2Image10M 至少 60%、Deep100M 至少 30%、
  SIFT100M 至少 40%；
- candidates/batch 达到测得 `candidates/unique_distance_object` 理论上限的至少
  70%，同时额外距离计算量不超过 B0 的 20%；
- query bytes/update 的降幅至少达到实际 distance-batch 降幅的 80%；按当前估算，
  四数据集的最低目标分别为 60%/48%/24%/32%。Deep100M 的当前对象扇出决定了
  阶段 3 不能设置统一 50% query-byte 降幅；
- distance roundtrip/queue 降低至少 25%；B1 四数据集加权平均延迟相对 B0 降低
  至少 10%，且任何数据集不回退超过 5%，P99 不回退超过 5%；
- 共同 query 前缀的 Recall@10 下降不超过 0.5 pp，零失败且一致性检查通过。

### 6.3 可选的同对象融合

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

退出条件：全部 CLS calls/update 相对 B0 的 501–747 降低至少 60%；patch
calls/update 从约 14–17 降至 5 以下，patch 阶段占完整更新低于 10%；只读异步使
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
| B0 | 当前 OSD 基线 | 逐 frontier | 当前 CLS 幂等 | 无 | 无 | modulo |
| B1 | OSD 优化 | 有界 frontier + 对象聚合 | 当前 CLS 幂等 | 无 | 无 | modulo |
| B2 | OSD 优化 | B1 | intent/saga | 无 | 无 | modulo |
| B3 | OSD 优化 | B1 | intent/saga | RemoteExecutor | 无 | modulo |
| B4 | OSD 优化 | B1 | intent/saga | RemoteExecutor | AIMD | modulo |
| B5 | OSD 优化 | B1 | intent/saga | RemoteExecutor | AIMD | graph-aware |

每个版本运行四数据集、并发度 1/4/8/16；B1–B5 与固定 B0 做同提交或兼容提交的
交错对照，每个正式配置至少三次独立重导入。C0 在每个主要里程碑重跑，用来确认
集群环境漂移，但不作为 B1–B5 的直接优化分母。主报告统一使用
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
| M2 协议与路由 | 最小 PlacementResolver、协议信封、distance-only fan-out | modulo 等价，开销 < 5% |
| M3 读聚合 | level-0 有界 frontier、按对象 batch distance | 达到分数据集扇出门槛 |
| M4 恢复协议 | intent/saga、恢复器、元数据拆分 | crash injection 通过 |
| M5 执行器 | 异步 RemoteExecutor、写微批 | calls/patch/P99 达标 |
| M6 拥塞控制 | per-target AIMD 与 deadline flush | queue/P99 达标 |
| M7 图感知布局 | locality group、bounded object layout | distance object fan-out 降低 ≥30% |
| M8 集成评估 | B0–B5 消融、统计报告和图表 | 综合目标验证 |

## 13. Git 实施拆分

每个提交只完成一个可验证的逻辑单元，建议顺序如下：

1. `feat(metrics): track distance object fanout and batch efficiency`
2. `test(quality): compare recall on a shared query prefix`
3. `refactor(storage): introduce minimal placement resolver`
4. `feat(protocol): version cls requests and responses`
5. `feat(search): collect a bounded level-zero frontier window`
6. `feat(search): coalesce adjacency and distance reads by object`
7. `feat(cls): fuse same-object frontier expansion`
8. `feat(metadata): persist recoverable update intents`
9. `feat(recovery): replay incomplete vector updates`
10. `feat(executor): submit per-object reads asynchronously`
11. `feat(executor): microbatch independent query subrequests`
12. `feat(cls): batch idempotent edge patches`
13. `feat(scheduler): adapt per-target concurrency`
14. `feat(layout): place nodes in bounded locality groups`
15. `test(experiments): add c0-b5 ablation suite`

每个阶段先运行单元测试、小数据集正确性、超时/崩溃注入，再运行四数据集正式实验。
性能提交和正确性提交不混合，实验报告必须记录完整 commit、二进制哈希和原始结果
目录；数据集、keyring、日志、构建产物和原始大结果不得提交到 Git。
