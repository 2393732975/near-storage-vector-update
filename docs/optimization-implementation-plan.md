# 近存储向量更新优化实施方案

## 1. 设计依据与实施原则

本方案依据开题答辩 PPT 第 14–18 页的三层优化框架，并结合当前仓库的
Coordinator、Ceph CLS、导入器和实验结果制定。PPT 中的三个方向分别是：

1. 图结构感知的数据—存储协同布局；
2. 面向更新负载的两级请求聚合与拥塞感知执行；
3. 面向动态图索引的一致性元数据协议。

三项优化存在依赖关系：布局决定请求 fan-out，聚合依赖稳定的 shard/group
标识，异步批处理又要求写操作可重试、可去重。因此不应同时修改三层，而应按
“正确性基线 → 协议与路由抽象 → 元数据解耦 → 请求聚合 → 拥塞控制 → 图感知
布局 → 集成评估”的顺序实施。

当前实现还存在以下前置问题：节点位置由 `global_id % owners` 固定决定；每个
worker 同步执行 `cls_exec`；并发插入会多次竞争全局 `GlobalMeta`；部分正式实验
存在失败更新；大型 OMAP 对象会给 BlueStore/RocksDB 带来压力。任何性能结论都
必须以零失败更新和图一致性检查通过为前提。

## 2. 目标架构

```text
Update Coordinator
  ├─ Update Protocol：状态机、ID 租约、幂等与失败恢复
  ├─ Graph Search：HNSW 搜索、邻居选择
  ├─ Remote Executor
  │    ├─ 单次 update 内聚合
  │    ├─ 跨 update 微批
  │    └─ 每目标对象/OSD拥塞窗口
  └─ Placement Manager
       ├─ Locality Group 选择
       ├─ group → locator/object 路由
       └─ 容量与负载约束

Ceph/RADOS
  ├─ Global Header：entrypoint、max_level、schema_epoch
  ├─ ID Allocator：批量 ID 租约
  └─ Locality Group Objects
       ├─ payload
       ├─ adjacency + version
       ├─ shard-local meta
       └─ update intent/log
```

需要明确两个 Ceph 执行边界：同一 PG 不代表一次 CLS 能跨多个对象原子执行；
修改型 CLS 必须经 RADOS primary 及其正常副本协议，不能向副本直接并行下发写
操作。请求聚合的基本键必须包含 pool、locator、object 和 opcode，而不能只按
OSD 聚合。

## 3. 阶段 0：建立可靠的正确性和观测基线

### 3.1 修复并发更新语义

首先统一串行与并行路径的行为：

- `apply_edge_patch_batch` 不再固定使用 `M=8`，而是读取索引配置；
- 邻居溢出时采用明确且一致的裁剪策略；
- 失败更新不能遗留 label 指向无效节点；
- 多 Coordinator 场景不能依赖单进程内 label 锁；
- 区分超时、版本冲突、节点缺失和协议错误，不能只累计一个
  `failed_updates`。

增加离线一致性检查器，至少检查：

- label 只指向 `ACTIVE` 节点；
- `global_id` 唯一，payload、VectorRef 和 adjacency 完整；
- 邻接点存在，level 0 度数不超过 `2M`，高层不超过 `M`；
- global/shard count 与实际节点状态一致；
- 不存在长期未完成 intent、重复 patch 和非法状态转换。

验收门槛：四个数据集连续三轮 `failed_updates=0`，一致性检查全部通过。

### 3.2 补齐优化评价指标

在现有阶段耗时基础上增加：

- `unique_objects_per_update`、`unique_pgs_per_update`、
  `unique_osds_per_update`；
- `cross_group_edges_per_update`、`cross_group_edge_ratio`；
- `cls_calls_per_update`、`candidates_per_cls_call`；
- request/reply bytes、batch fill ratio、batch wait time；
- Global Header 与 shard-local meta 写次数；
- patch/meta 冲突数和分类重试数；
- 每目标 inflight、RTT、CLS service time 和 queue estimate。

`timed_noop` 从每批调用一次改成低频采样，例如每目标每秒一次，避免探针本身
改变正式性能。

## 4. 阶段 1：协议版本化与位置路由抽象

### 4.1 位置解析接口

将 Coordinator 和 importer 中的 `owner_for(global_id)`、`chunk_for(global_id)`
统一封装为：

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

第一版 resolver 仍实现现有 modulo 布局，保证重构前后行为相同。后续图感知
布局只替换 resolver 和 importer 的布局阶段，不再修改搜索算法。

### 4.2 协议版本与幂等标识

所有新协议增加统一头部：

```cpp
struct ProtocolHeader {
  uint32_t magic;
  uint16_t schema_version;
  uint16_t opcode;
  uint64_t request_id;
};
```

写请求携带 `update_id`、`expected_version` 和 `placement_epoch`。CLS 需要拒绝
未知 schema，识别已执行的 `update_id`，并对版本冲突返回逐项状态。

验收门槛：modulo 模式的图结果与重构前一致，吞吐变化不超过 5%。

## 5. 阶段 2：面向动态图的一致性元数据协议

当前 `GlobalMeta` 同时保存 entrypoint、max level、元素计数、下一个 ID 和版本，
普通插入也要竞争同一个对象。新协议将其拆为三类状态。

### 5.1 Global Header

```cpp
struct GlobalHeader {
  uint64_t entrypoint;
  uint32_t max_level;
  uint64_t header_epoch;
  uint32_t M;
  uint32_t ef;
  uint32_t dim;
  uint32_t vector_kind;
  uint32_t metric;
  uint32_t schema_version;
};
```

普通插入只读取缓存的 header。只有更高层节点、新 entrypoint 或配置变化时才执行
条件更新。

### 5.2 ID Allocator

将 `next_global_id` 放到独立 allocator 对象。Coordinator 每次原子申请
1024/4096 个 ID，在本地租约耗尽后才再次访问 allocator，将全局 ID 写热点从
每次 update 一次降低为每个租约一次。

### 5.3 Shard-local Meta

```cpp
struct ShardMeta {
  uint64_t local_epoch;
  uint64_t live_count;
  uint64_t stale_count;
  uint64_t committed_updates;
  uint64_t bytes_used;
};
```

节点状态、邻接版本、计数和 intent 均保存在所属 group/shard，不再提升全局
header 版本。

### 5.4 可恢复更新状态机

一次覆盖更新按以下顺序执行：

1. 从本地租约获取新 ID；
2. 读取缓存的 Global Header；
3. 搜索邻居并选择目标 group；
4. 写入状态为 `PREPARED` 的新节点；
5. 写入 shard-local `UpdateIntent`；
6. 使用 `update_id + expected_version` 执行幂等 edge patch；
7. 将新节点切换为 `ACTIVE`；
8. CAS 更新 label：`old_id → new_id`；
9. 将旧节点标记为 `STALE`；
10. 更新 shard-local 计数；
11. 仅在提高 `max_level` 时 CAS Global Header；
12. 将 intent 标记为 `COMMITTED`。

Ceph 不提供这里所需的跨对象事务，因此采用可恢复的 saga/intent，而不是假设
payload、所有邻接和 label 能一次原子提交。崩溃恢复优先向前完成；已经写入的
patch 通过 `update_id` 去重，无法完成的新节点转为 `ABORTED` 并由回收器处理。

验收门槛：普通插入的 `global_header_writes_per_update` 接近 0，CAS 冲突降低至少
80%，并通过各提交阶段的故障注入恢复测试。

## 6. 阶段 3：第一级聚合——单次 update 内批处理

当前代码虽按 `(owner, chunk)` 分组距离请求，但 HNSW 搜索仍逐候选同步读取邻接，
造成多轮小 RPC。将搜索循环改为窗口化 frontier：

1. 一次弹出多个可扩展候选；
2. 按目标 object 分组；
3. 批量读取 adjacency；
4. 去重未访问邻居；
5. 按目标 object 批量算距；
6. 合并结果并更新 frontier。

进一步新增只读融合算子 `expand_frontier_batch`，输入 query、多个 frontier node、
level 和候选限制，在单个目标对象内完成邻接读取、VectorRef/payload 读取及算距，
返回 `neighbor_id + distance + adjacency_version`。这将两次 CLS 链路合并为一次，
但不能跨对象读取。

验收门槛：CLS calls/update 显著下降，candidates/call 上升；Recall@10 相对 baseline
下降不超过 0.5 个百分点，P99 不因批次过大而恶化。

## 7. 阶段 4：第二级聚合——跨 update 微批

将每个 worker 独立、同步的 `CephFacade` 调用改为共享 `RemoteExecutor`：

```text
RemoteExecutor
  ├─ per-target queues
  ├─ batch builder
  ├─ async submitter
  └─ completion dispatcher
```

逻辑请求通过 future/promise 等待结果。不同 update 的 query 不同，因此跨更新
协议必须支持多个子请求：

```cpp
struct DistanceSubRequest {
  uint64_t request_id;
  std::string query_vector;
  std::vector<uint64_t> candidate_ids;
};

struct MultiDistanceRequest {
  std::vector<DistanceSubRequest> requests;
};
```

批次满足任一条件即下发：达到候选数、字节数、子请求数，或最老请求达到
deadline。初始扫描范围可设为：100–500 微秒窗口、256 KiB–1 MiB 请求、
256–2048 个候选。写批次必须返回逐项成功/冲突状态，不能因单项冲突重试整个
批次。

验收门槛：远端调用次数降低 40% 以上，同时更新 P99 不因微批等待显著增加。

## 8. 阶段 5：拥塞感知调度

为每个目标维护：

```cpp
struct TargetState {
  double rtt_ewma;
  double service_time_ewma;
  double queue_time_ewma;
  uint32_t inflight;
  uint32_t cwnd;
  uint32_t batch_limit;
  uint64_t timeout_count;
};
```

第一版采用稳定的 AIMD 控制：RTT/queue 低于目标时缓慢增加 `cwnd`；queue P95、
超时或 slow-op 增加时将窗口减半。低负载时提高批大小；高负载时先收缩并发，
再缩小批次；接近 deadline 的请求立即刷新。

目标 OSD 映射不可得或发生 CRUSH 变化时，调度器先以 target object 为控制粒度，
不能使用陈旧 OSD ID 强行路由。修改请求不实现“多副本并行下发”；只读距离的
副本读取作为独立后续实验，优先使用 Ceph 原生 replica-read 能力。

验收门槛：零 timeout、零失败更新；queue estimate P95 和 update P99 至少降低
20%，吞吐提升不能以严重尾延迟为代价。

## 9. 阶段 6：图结构感知的数据布局

### 9.1 Locality Group 评分

插入搜索得到邻居集合 `N(v)` 后计算：

```text
Locality(v,g)
  = Σ level_weight(u,v) × I[group(u)=g] / Σ level_weight(u,v)

Load(g)
  = max(nodes/Smax, bytes/Bmax, update_rate/Lmax, queue/QueueMax)

Score(v,g) = Locality(v,g) - λ × Load(g)
```

选择得分最高且未超过硬限制的 group；全部 group 达限时创建新 group。容量限制
必须同时约束节点数、payload 字节、OMAP key 数和近期更新率，避免再次产生数
GiB 的大型 OMAP 对象。

### 9.2 对象布局

```text
group.<gid>.data.<chunk>
  payload
  vec/<id>
  node/<id>
  node_version/<id>

group.<gid>.meta
  shard-local meta
  update intent
```

同一 group 使用相同 locator key 映射到相同 PG；强关联且需要融合执行的节点还应
尽量进入同一个有界 chunk 对象，因为同 PG 不会自动减少跨对象 CLS 次数。

实施分两步：先只对在线新节点使用 graph-aware 选择，旧 base 保持固定；验证后
再对初始 HNSW 图执行一到数轮离线 greedy partition。第一版不进行在线迁移和
边界副本，避免过早引入双读、placement epoch、迁移恢复和副本失效协议。

验收门槛：cross-group edge ratio 和 unique OSDs/update 降低 20%–40%；OSD 数据
量、更新率和队列保持均衡；Recall@K 不因布局改变下降。

## 10. 集成实验与消融设计

| 版本 | 元数据协议 | 单次聚合 | 跨更新微批 | 拥塞控制 | 布局 |
| --- | --- | --- | --- | --- | --- |
| B0 | 原始 | 原始 | 无 | 无 | modulo |
| B1 | 新协议 | 原始 | 无 | 无 | modulo |
| B2 | 新协议 | 有 | 无 | 无 | modulo |
| B3 | 新协议 | 有 | 有 | 无 | modulo |
| B4 | 新协议 | 有 | 有 | 有 | modulo |
| B5 | 新协议 | 有 | 有 | 有 | graph-aware |

每个版本运行四个数据集和并发度 1、4、8、16，至少重复三次。固定代码提交、
Ceph/CLS 版本、数据、pool size、PG 数和实际落点；正式性能实验关闭逐请求 noop
探针；每轮结束执行安全清理脚本。只有失败更新为零且一致性检查通过的轮次才纳入
性能统计。

PPT 中布局 20%–40%、执行层 40%–60%、元数据层 30%–50% 的预计收益应作为待验证
假设。最终综合目标为：

- 平均更新延迟降低至少 30%，P99 降低至少 20%；
- 吞吐提升至少 30%；
- 网络字节和 CLS 调用次数显著下降；
- Global Header 写次数接近零；
- 无失败更新、无一致性错误；
- Recall@K 与 baseline 基本一致。

## 11. 里程碑、交付物与建议周期

| 里程碑 | 建议周期 | 主要交付物 | 退出条件 |
| --- | ---: | --- | --- |
| M0 正确性基线 | 1–2 周 | 图检查器、错误分类、稳定 baseline | 三轮零失败 |
| M1 协议与路由 | 1–2 周 | 版本化协议、PlacementResolver | modulo 等价 |
| M2 元数据协议 | 2–3 周 | ID 租约、shard meta、intent 恢复 | 普通更新无全局 CAS |
| M3 单次聚合 | 2 周 | frontier batch、融合 CLS | calls/update 下降 |
| M4 微批与调度 | 2–3 周 | RemoteExecutor、AIMD | P99 与 queue 下降 |
| M5 图感知布局 | 3–4 周 | group 分配、locator/object 布局 | fan-out 下降且均衡 |
| M6 集成评估 | 2 周 | 消融结果、报告、图表 | 综合目标验证 |

## 12. Git 实施拆分

每个提交只完成一个可验证的逻辑单元：

1. `test(correctness): add distributed graph invariant checker`
2. `fix(update): make parallel patch semantics deterministic`
3. `feat(protocol): version CLS requests and add idempotency keys`
4. `refactor(storage): introduce placement resolver`
5. `feat(metadata): lease ids and persist shard-local update state`
6. `feat(cls): add versioned idempotent edge patches`
7. `feat(cls): batch frontier expansion per object`
8. `feat(executor): microbatch requests across updates`
9. `feat(scheduler): adapt per-target concurrency`
10. `feat(layout): place nodes by locality group`
11. `test(experiments): add optimization ablation suite`

每个阶段先运行小数据集正确性和故障注入测试，再运行正式四数据集实验。性能提升
不能替代正确性验收，也不能通过屏蔽 Ceph 健康告警、提高超时或减少检查来实现。
