# Update 阶段耗时分解实验设计

## 目标

本实验用于回答：一次全图在线 update 中，主要阶段分别占多少时间。当前关注的阶段为：

- 查旧点
- 标记 stale
- 全图搜索
- 远端距离计算
- 邻接 patch
- 全局 meta 更新

实验基于现有 `ceph_global_hnsw_baseline` 真实 update 路径实现，不使用模拟路径。因此统计结果包含 `librados cls_exec`、OSD 内 `cls` 执行和 Ceph 对象/OMAP 访问开销。

## 阶段定义

### 查旧点

对应 `lookup_label_batch`。

输入外部 label，查找当前 label 对应的旧 `global_id`。

统计字段：

```json
stage_profile.lookup_old_seconds
stage_profile.lookup_old_pct
stage_profile.lookup_old_ms_per_update
```

### 标记 stale

对应 `mark_node_stale`。

找到旧点后，将旧点的 `VectorRef.flags` 标记为 stale。

统计字段：

```json
stage_profile.mark_stale_seconds
stage_profile.mark_stale_pct
stage_profile.mark_stale_ms_per_update
```

### 全图搜索

对应 HNSW 的 greedy search 和 search layer 控制逻辑，包含邻接表读取、候选队列维护和邻居选择，但为了避免与远端距离计算重复计数，报告中的 `full_graph_search` 是扣除了远端距离计算后的 exclusive 时间。

统计字段：

```json
stage_profile.full_graph_search_exclusive_seconds
stage_profile.full_graph_search_pct
stage_profile.full_graph_search_ms_per_update
```

原始 inclusive 字段：

```json
stage_profile.graph_search_seconds_raw
stage_profile.graph_search_distance_seconds
```

### 远端距离计算

对应 `distance_to_local_batch`。

Coordinator 将候选点按 owner/chunk 分组，调用目标 OSD 的 `cls` 在 OSD 内计算距离。

统计字段：

```json
stage_profile.remote_distance_seconds
stage_profile.remote_distance_pct
stage_profile.remote_distance_ms_per_update
```

需要注意，`remote_distance_seconds` 是 Coordinator 侧观测到的 `cls_exec(distance_to_local_batch)` 端到端时间，不等于纯距离计算时间。它包含：

- Coordinator 到 OSD 的 `cls_exec` 请求路径。
- OSD 调度和执行 `cls` 的开销。
- OMAP 中 `vec/<global_id> -> VectorRef` 的读取。
- 根据 `VectorRef.offset/bytes` 读取 payload 中的向量字节。
- 真正的 L2/IP 距离计算。
- reply 编码、返回和客户端 decode。

因此现在进一步在 `DistanceBatchReply` 中返回 CLS 内部细分：

```json
stage_profile.distance_vector_ref_seconds
stage_profile.distance_payload_read_seconds
stage_profile.distance_compute_seconds
stage_profile.distance_unaccounted_seconds
```

含义如下：

- `distance_vector_ref_seconds`：OSD 内读取 OMAP `VectorRef` 的累计时间。
- `distance_payload_read_seconds`：OSD 内从对象 payload 读取向量字节的累计时间。
- `distance_compute_seconds`：OSD 内真正执行 L2/IP 距离计算的累计时间。
- `distance_unaccounted_seconds`：Coordinator 侧远端距离总耗时扣除上述三项后的剩余部分，主要包括 `cls_exec` 往返、OSD 调度、编码/解码以及未细分的函数开销。

这些字段也会输出对应百分比和每次 update 平均耗时：

```json
stage_profile.distance_vector_ref_pct
stage_profile.distance_payload_read_pct
stage_profile.distance_compute_pct
stage_profile.distance_unaccounted_pct
stage_profile.distance_vector_ref_ms_per_update
stage_profile.distance_payload_read_ms_per_update
stage_profile.distance_compute_ms_per_update
stage_profile.distance_unaccounted_ms_per_update
```

### 邻接 patch

包含两部分：

- patch prepare：读取旧点邻接表，尝试把新点加入旧点邻居列表，必要时裁剪邻居。
- patch apply：调用 `apply_edge_patch_batch` 将修改后的旧点邻接表写回 owner OSD。

为了避免与远端距离计算重复计数，报告中的 `adjacency_patch` 会扣除 patch prepare 期间发生的远端距离计算。

统计字段：

```json
stage_profile.adjacency_patch_exclusive_seconds
stage_profile.adjacency_patch_pct
stage_profile.adjacency_patch_ms_per_update
```

原始字段：

```json
stage_profile.patch_prepare_seconds_raw
stage_profile.patch_apply_seconds_raw
stage_profile.patch_prepare_distance_seconds
```

### 全局 meta 更新

对应 `cas_global_meta`。

插入完成后更新：

- `enterpoint`
- `max_level`
- `cur_element_count`
- `next_global_id`
- `version`

统计字段：

```json
stage_profile.global_meta_update_seconds
stage_profile.global_meta_update_pct
stage_profile.global_meta_update_ms_per_update
```

### 其他

`other_update_seconds` 是端到端 update 时间扣除上述阶段后的剩余部分，主要包括：

- 新向量写入 `store_vector`
- 新点邻接表写入 `set_adjacency_batch`
- 读取全局 meta
- 函数调度和少量本地逻辑

统计字段：

```json
stage_profile.other_update_seconds
stage_profile.other_update_pct
stage_profile.other_update_ms_per_update
```

## 运行方式

### 汇总当前 JSON 打点

`nsvu-update-coordinator` 已在 PPT 的三个观测点上记录原始耗时。对一轮输出运行：

```bash
python3 scripts/summarize-stage-costs.py \
  results/ppt-baseline-<timestamp> \
  --output results/ppt-baseline-<timestamp>/stage-costs.md
```

汇总器使用 `accounted_update_seconds` 作为主路径阶段占比的分母，适用于
`UPDATE_PARALLELISM > 1`。不要直接采用 JSON 旧有的 `*_pct`：它们除以墙钟
`graph_seconds`，而并发 worker 的累计阶段时间会重叠，因而可能大于 100%。

输出包含与答辩 PPT 对应的三类指标：

- 观测点 A：远端距离中的 RTT/排队、CLS OMAP 引用读取、payload 读取与纯算距；
- 观测点 B：扣除距离调用后的邻接 patch 独占时间；
- 观测点 C：`cas_global_meta` 全局元数据更新时间。

当 `failed_updates` 非零时，报告仅用于定位瓶颈；修复失败后再作论文性能结论。

### 单数据集短时间窗

如果当前 Ceph 中已经导入了对应数据集的 base 图，可以直接运行 update 时间窗：

```bash
TIME_LIMIT_SECONDS=300 ./run_dataset_experiment.sh sift100m window30m
```

输出：

```text
results/multids/sift100m/update.window_300s.json
```

### 四数据集完整阶段 profile

如果需要重新导入每个数据集并分别运行固定时间窗：

```bash
./run_stage_profile_suite.sh 300
```

参数 `300` 表示每个数据集 update 窗口为 300 秒。该脚本会顺序执行：

```text
GIST1M -> Text-to-Image10M -> Deep100M -> SIFT100M
```

每个数据集都会先重新 import base，再运行 update profile。

### 生成汇总表和图

```bash
./generate_stage_profile_summary.py 300
```

输出：

```text
results/stage_profile_300s/stage_profile_summary.csv
results/stage_profile_300s/stage_profile_report.md
results/stage_profile_300s/figures/stage_profile_percent.svg
```

## 已完成的 smoke test

已对当前 `SIFT100M` base 状态做了一个 20 秒短窗验证：

```text
results/multids/sift100m/update.window_20s.json
results/stage_profile_20s/stage_profile_report.md
```

结果显示，在该短窗样本中：

- 远端距离计算约占 `77.76%`
- 邻接 patch 约占 `13.91%`
- 全图搜索控制逻辑约占 `4.82%`
- 查旧点、stale 标记和全局 meta 更新占比较小

该 20 秒结果只用于验证计时逻辑是否工作，不建议作为正式结论。正式实验建议使用 300 秒或 1800 秒窗口。

随后又对细化后的 `distance_to_local_batch` 做了一个 `SIFT100M` 30 秒短窗验证：

```text
results/multids/sift100m/update.window_30s.json
```

该短窗中，`remote_distance` 约占 update 总时间 `76.49%`，进一步拆分为：

- `distance_vector_ref`：约 `25.86%`
- `distance_payload_read`：约 `17.50%`
- `distance_compute`：约 `0.10%`
- `distance_unaccounted`：约 `33.02%`

这说明在当前实现中，`远端距离计算` 这个大项主要不是纯 L2/IP 算术开销，而是 OMAP 查引用、payload 读取和 `cls_exec` 往返/调度等系统路径开销。
