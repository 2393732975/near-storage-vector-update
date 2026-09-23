# 更新阶段耗时与数据移动打点

本文定义 `nsvu-update-coordinator` 当前输出的阶段计时口径，并说明如何比较
compute-node 与 OSD/CLS 两条更新路径。正式结论必须同时满足零失败更新和离线
一致性检查通过。

## 两条基线路径

- `--distance-mode compute`：从 Ceph 拉取候选向量，在 Coordinator 本地计算距离。
- `--distance-mode osd`：把 query 和候选 ID 发给目标对象，由 CLS 读取本地向量并
  计算距离。图搜索控制、邻接修改和元数据协议仍由 Coordinator 编排。

两者使用相同的 base 图、更新语义和持久化结构，区别仅在距离阶段是否搬运候选
向量。compute 路径用 `remote_vector_bytes` 和 `remote_vector_calls` 量化数据移动。

## 完整更新主阶段

所有 `*_seconds` 都是 worker 累计工作时间。完整更新按以下互斥阶段分析：

| 阶段 | JSON 字段 | 边界 |
| --- | --- | --- |
| 查找旧点 | `lookup_old_seconds` | label 到旧 `global_id` 的查找 |
| 写入新向量 | `store_vector_seconds` | payload 与 `VectorRef` 持久化 |
| 图搜索控制 | `full_graph_search_exclusive_seconds` | 图搜索总时间扣除距离调用 |
| 远端距离 | `remote_distance_seconds` | Coordinator 观测的距离阶段端到端时间 |
| 写新邻接 | `set_new_adjacency_seconds` | 新节点邻接表写入 |
| 修补旧邻接 | `adjacency_patch_exclusive_seconds` | prepare 与 apply，扣除重复统计的距离调用 |
| 更新元数据 | `global_meta_update_seconds` | global meta 的条件更新 |
| 标记旧向量 | `mark_stale_seconds` | 将旧节点状态改为 stale |

每次成功更新的阶段耗时应计算为：

```text
stage_ms_per_update = sum(stage_seconds) * 1000 / sum(vectors_processed)
```

多轮汇总时，平均更新延迟按 `vectors_processed` 加权。其余未单独打点的成本为
加权平均更新延迟减去上述阶段之和，主要包含 label CAS、调度和客户端逻辑。

不要直接把 JSON 中的 `*_pct` 当作完整更新占比：它们以墙钟
`graph_seconds` 为分母，而并发 worker 的累计时间会重叠，合计可能超过 100%。
`accounted_update_seconds` 及汇总脚本提供的是旧六阶段累计工作时间视图，不包含
已独立观测的写新向量和写新邻接，适合对比路径，但不等同于完整延迟分解。

## 距离阶段内部拆分

OSD/CLS 路径满足：

```text
remote_distance
  = distance_roundtrip_queue
  + distance_vector_ref
  + distance_payload_read
  + distance_compute
  + distance_cls_unaccounted
```

- `distance_vector_ref_seconds`：CLS 内读取 OMAP `VectorRef`。
- `distance_payload_read_seconds`：CLS 内读取对象 payload。
- `distance_compute_seconds`：CLS 内真正执行 L2/IP 算距。
- `distance_cls_unaccounted_seconds`：CLS 内编码、循环等未细分时间。
- `distance_roundtrip_queue_seconds`：客户端端到端时间减去 CLS 总时间，包含网络、
  librados、OSD 排队和线程调度，不能解释为纯网络时间。

开启 `--distance-split-probe` 后，低频 noop 探针还会给出
`distance_network_roundtrip_est_seconds` 和 `distance_osd_queue_est_seconds`。探针是
估算手段，正式性能实验必须记录是否开启及采样间隔。

compute-node 路径满足：

```text
remote_distance
  = distance_fetch_rpc
  + distance_local_compute
  + distance_compute_node_unaccounted
```

其中 `distance_fetch_rpc_seconds` 是候选向量 RPC，
`distance_local_compute_seconds` 是本地算距。路径价值应同时用延迟、吞吐和
`remote_vector_bytes / vectors_processed` 判断，不能只比较纯算距时间。

## 运行和汇总

运行四数据集基线；脚本会为每个模式/数据集重建实验池：

```bash
CEPH_KEYRING=/path/to/keyring \
DATASET_ROOT=/path/to/datasets \
MODES="compute osd" \
DATASETS="gist1m text2image10m deep100m sift100m" \
WINDOW_SECONDS=300 \
UPDATE_PARALLELISM=4 \
scripts/run-ppt-baselines.sh
```

汇总单次实验目录：

```bash
python3 scripts/summarize-stage-costs.py \
  results/ppt-baseline-<timestamp> \
  --output results/ppt-baseline-<timestamp>/stage-costs.md
```

每轮还必须保存 Git commit、CLS 哈希、Ceph 版本、池配置和实际 PG/OSD 落点。
原始 JSON 与生成结果保存在 `results/`，不提交 Git。

## 结果判读规则

- `failed_updates` 必须为 0，检查器必须返回 `status=pass`。
- A/B 必须使用同一代码提交、数据、池落点、并发度和探针配置。
- 至少重复三轮，报告均值、标准差、P50/P95/P99 和每次更新的数据移动量。
- 累积轮次受缓存与索引状态变化影响，不能当成独立冷启动样本。
- `roundtrip/queue` 很高表示系统路径占主导，但不能据此单独归因于网络或 OSD。
