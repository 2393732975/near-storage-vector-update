# 近存储向量更新优化实施方案

## 1. 修订依据与核心判断

本方案依据开题答辩 PPT 第 14–18 页的三层设计，并按 2026-09-22 至
2026-09-23 的最新实测结果重新排序。数据来源为
[阶段 0 OSD 卸载验收报告](reports/phase0-validation-report-2026-09-22.md)和
[Compute-node 背景实验报告](reports/compute-background-report-2026-09-23.md)。

| 数据集 | Compute 平均延迟 | OSD 第 1 轮 | OSD 三轮加权平均 |
| --- | ---: | ---: | ---: |
| GIST1M | 265.227 ms | 274.986 ms | 204.016 ms |
| Text2Image10M | 279.579 ms | 350.135 ms | 262.371 ms |
| Deep100M | 424.387 ms | 418.645 ms | 342.813 ms |
| SIFT100M | 392.776 ms | 397.993 ms | 317.707 ms |

OSD 第 1 轮与 Compute 单轮更接近冷启动对比，但它们仍不是同一提交、同一时间交错
运行的严格 A/B。OSD 后两轮受缓存预热和索引状态变化影响，三轮加权值不能直接
解释为卸载收益。

最新数据支持以下结论：

- 距离阶段占完整更新的约 61%–72%，是第一瓶颈；旧邻接修补占 16%–22%，是
  第二瓶颈。
- OSD 距离阶段中，实际算距仅占约 0.09%–1.01%；主要成本是
  roundtrip/queue（约 66%–90%）及 VectorRef/payload 读取。
- 每次更新有约 312–511 个距离批次，而每批只有 1.01–1.25 个候选；全部 CLS
  调用约 388–647 次。当前“批处理”在实际调用形态上接近逐候选同步 RPC。
- OSD 路径在数百个距离请求中反复携带完整 query。除 GIST 外，应用层 CLS
  request+reply bytes/update 反而比 Compute 高约 4%–24%；卸载主要改变了数据
  移动方向，尚未稳定减少总传输量。
- global meta 仅占约 0.4%–0.8%，且当前正式结果无 meta CAS 重试。元数据改造
  仍是故障恢复和扩展性的基础，但不应再被列为当前性能优化的第一步。
- cross-owner edge ratio 约 74%–80%，说明图感知布局仍有价值；但它不能替代
  先消除细粒度 RPC 和 query 重复传输。

因此实施路线改为两条相互约束的主线：

1. **性能主线**：严格 A/B → 路由抽象 → 单次更新读聚合 → 异步执行与写微批
   → 拥塞控制 → 图感知布局。
2. **正确性主线**：现有幂等基线 → 协议版本化 → intent/saga 与恢复器 →
   元数据扩展。

只读聚合可在完整 saga 前实施；跨 update 的修改型微批必须通过故障恢复门槛后
启用。所有性能结论必须同时满足零失败、图一致性检查通过和 Recall 门槛。

## 2. 目标架构与 Ceph 边界

```text
Update Coordinator
  ├─ Update Protocol：版本、幂等、intent/saga、恢复
  ├─ Windowed Graph Search：批量 frontier、邻居去重
  ├─ Remote Executor
  │    ├─ per-object 读写队列
  │    ├─ query-aware 聚合与异步提交
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

## 3. 阶段 0：正确性与观测基线（已完成）

阶段 0 已完成动态 M、原子 label CAS、失败分类、低频探针、幂等超时重放和离线
一致性检查器。四数据集三轮共完成 47,900 次更新，12/12 轮均为零失败且检查
通过。

现阶段仍需保留的基线约束：

- label 只能指向 ACTIVE 节点；global ID 唯一；payload、VectorRef 和 adjacency
  完整；节点度数满足配置。
- 每轮记录 failures、分类重试、request/reply bytes、调用数、批大小、对象/PG/OSD
  fan-out、跨 owner 边比例及各阶段 P50/P95/P99。
- `timed_noop` 只能低频采样，不能每批调用并污染正式结果。
- 当前幂等机制能处理客户端超时重放，但还不等于进程崩溃后可恢复的跨对象事务。

## 4. 阶段 1：建立可比较的性能与质量基线

优化前先修复实验设计，不用不同日期的结果决定代码取舍。

### 4.1 严格 A/B

同一轮实验必须固定 Git commit、Coordinator/Importer/CLS 二进制、数据切分、池
配置、PG 数、CRUSH 落点和并发度，并交错运行 `compute` 与 `osd`。每个配置至少
三次**重新导入后的独立重复**；冷启动与预热结果分开报告，不把同一索引上连续
三轮当独立样本。每次运行创建唯一 `RUN_ROOT`。

### 4.2 补齐质量和协议指标

- 使用数据集 ground truth 报告 Recall@10；没有 Recall 的性能结果只能用于诊断。
- 分 opcode 记录 calls/update、request/reply bytes、query bytes、候选数、batch
  fill ratio、batch wait、client roundtrip 和 CLS service time。
- 明确区分 VectorRef/payload I/O、OSD 排队、网络/librados 与 Coordinator 调度；
  `roundtrip - CLS internal` 只能称为 roundtrip/queue，不能直接称为网络时间。
- 分别统计 adjacency read、distance、new adjacency、patch、meta 和状态切换调用。

退出条件：复现实有结果量级；每轮零失败且一致性检查通过；Recall@10 可用；同一
配置有至少三次独立样本，并报告均值、标准差和置信区间。

## 5. 阶段 2：协议版本化与位置路由抽象

先把固定 modulo 路由从搜索和导入逻辑中抽离，为后续聚合和布局提供稳定接口：

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

退出条件：modulo 模式的路由与图结果等价；失败注入仍可幂等重放；重构后延迟和
吞吐变化不超过 5%。

## 6. 阶段 3：最高优先级——单次更新的读路径聚合

这是最新数据指向的首个性能改动。将逐候选同步搜索改为窗口化 frontier：

1. 一次弹出多个可扩展候选，并按 adjacency 所在对象分组；
2. 每对象批量读取多个节点的邻接表；
3. 在 Coordinator 去重已访问邻居，并解析它们的目标对象；
4. 按目标对象形成大批距离请求，每个对象批次只携带一次 query；
5. 首版逐对象提交批次并合并结果；阶段 5 再异步并行提交独立对象批次；
6. 批量边界保持确定性排序，确保相同距离的 tie-break 与 baseline 一致。

第二步再增加只读 `expand_frontier_batch`：在一个对象内读取多个 frontier 的邻接，
直接计算其中**同对象**邻居的距离，并返回 `neighbor_id + distance + version`；跨
对象邻居只返回 ID，交由 Coordinator 再分组。不要让 CLS 假设能读取另一个对象。

第一版不把 query 持久写入 OMAP 作为缓存：这会给只读路径增加写放大、清理和
故障语义。应先通过“一次大请求仅发送一次 query”消除重复；只有测量证明仍受
query bytes 限制时，才实验短生命周期、可失效的 query handle。

退出条件：

- distance batches/update 从约 312–511 降至 50 以下；
- candidates/distance batch 从约 1 提升至至少 8，目标 16；
- query bytes/update 降低至少 80%，distance roundtrip/queue 降低至少 50%；
- Recall@10 相对 B0 下降不超过 0.5 个百分点；零失败且一致性检查通过；
- 批等待不能造成 P99 回退，未达到门槛时优先检查搜索依赖和分组窗口，而不是
  盲目增大 RPC 并发。

## 7. 阶段 4：可恢复更新与可扩展元数据

此阶段首先解决正确性和多 Coordinator 扩展，不再预设它能带来 30%–50% 的当前
平均延迟收益。现有 meta 仅占 0.4%–0.8%，优化收益应由实测决定。

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

### 8.1 读批次

单次 update 的 query-aware 合并优先；跨 update 微批可在同一 object/opcode RPC
中携带多个子请求，但每个子请求有独立 query 和候选集合。初始参数扫描范围为
100–500 微秒、256 KiB–1 MiB、256–2048 个候选，并受最老请求 deadline 约束。

跨 update 合并只减少 RPC 数，不必然减少 query bytes；只有同一 update 的候选
被合并到一个子请求时，query 才只传一次。因此两项指标必须分别验收。

### 8.2 写批次

将 edge patch 按对象和版本聚合，协议携带逐项 update_id、expected_version 和
返回码。不同对象仍是独立请求；发生局部冲突只重放对应项目。new adjacency、
label CAS 和生命周期切换不为追求批量而破坏 saga 顺序。

退出条件：全部 CLS calls/update 比 B0 降低至少 60%；patch calls/update 从约
12–16 降至 5 以下，patch 阶段占完整更新低于 10%；P99 不回退；零失败且故障恢复
测试通过。

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

当前 cross-owner edge ratio 约 74%–80%，并且一次更新触达大量对象，布局仍是
后续关键。但优化目标必须是“强关联节点进入同一有界对象”，只映射到同一 PG/OSD
不足以减少 CLS 调用。

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

退出条件：cross-group edge ratio、unique objects/update 和 unique OSDs/update
相对 modulo 降低 20%–40%；对象大小、OSD 容量和队列保持均衡；Recall 不下降。

## 11. 消融实验与总体验收

| 版本 | 读路径聚合 | 恢复协议 | 异步/写微批 | 拥塞 | 布局 |
| --- | --- | --- | --- | --- | --- |
| B0 | 当前实现 | 当前幂等 | 无 | 无 | modulo |
| B1 | frontier + distance batch | 当前幂等 | 无 | 无 | modulo |
| B2 | B1 | intent/saga | 无 | 无 | modulo |
| B3 | B1 | intent/saga | RemoteExecutor | 无 | modulo |
| B4 | B1 | intent/saga | RemoteExecutor | AIMD | modulo |
| B5 | B1 | intent/saga | RemoteExecutor | AIMD | graph-aware |

每个版本运行四数据集、compute/osd 两模式和并发度 1/4/8/16；每个配置至少三次
独立重复，并分别报告 cold/warm。布局、执行层和元数据层在 PPT 中的收益比例只作
待验证假设；尤其不能再把元数据层 30%–50% 当作当前结果支持的预测。

最终综合目标：

- 相对严格 B0，平均更新延迟降低至少 30%，P99 降低至少 20%，吞吐提高至少 30%；
- distance batches/update < 50、candidates/batch ≥ 8、总 CLS calls/update 降低
  ≥ 60%、query bytes/update 降低 ≥ 80%；
- patch calls/update ≤ 5，Global Header writes/update 接近 0；
- 所有正式轮次零失败、一致性检查通过，Recall@10 损失 ≤ 0.5 个百分点；
- 不通过关闭告警、放宽超时、减少检查或只挑选预热轮次获得性能结论。

## 12. 里程碑与交付物

| 里程碑 | 主要交付物 | 退出条件 |
| --- | --- | --- |
| M0 已完成 | 正确性检查、失败分类、稳定 OSD baseline | 12/12 轮通过 |
| M1 测量基线 | 严格 A/B、Recall、细粒度调用/字节指标 | 三次独立重复 |
| M2 协议与路由 | PlacementResolver、版本化协议 | modulo 等价，开销 < 5% |
| M3 读聚合 | windowed frontier、batch distance、融合 CLS | batches/query bytes 达标 |
| M4 恢复协议 | intent/saga、恢复器、元数据拆分 | crash injection 通过 |
| M5 执行器 | 异步 RemoteExecutor、写微批 | calls/patch/P99 达标 |
| M6 拥塞控制 | per-target AIMD 与 deadline flush | queue/P99 达标 |
| M7 图感知布局 | locality group、bounded object layout | fan-out 降低且均衡 |
| M8 集成评估 | B0–B5 消融、统计报告和图表 | 综合目标验证 |

## 13. Git 实施拆分

每个提交只完成一个可验证的逻辑单元，建议顺序如下：

1. `test(experiments): add recall and strict ab runner`
2. `refactor(storage): introduce placement resolver`
3. `feat(protocol): version cls requests and responses`
4. `feat(search): batch frontier adjacency reads`
5. `feat(cls): batch distance candidates per object`
6. `feat(cls): fuse same-object frontier expansion`
7. `feat(metadata): persist recoverable update intents`
8. `feat(recovery): replay incomplete vector updates`
9. `feat(executor): submit per-object requests asynchronously`
10. `feat(cls): batch idempotent edge patches`
11. `feat(scheduler): adapt per-target concurrency`
12. `feat(layout): place nodes in bounded locality groups`
13. `test(experiments): add b0-b5 ablation suite`

每个阶段先运行单元测试、小数据集正确性、超时/崩溃注入，再运行四数据集正式实验。
性能提交和正确性提交不混合，实验报告必须记录完整 commit、二进制哈希和原始结果
目录；数据集、keyring、日志、构建产物和原始大结果不得提交到 Git。
