# Compute-Node Distance Baseline

## 目标

构造一个与当前 `Baseline B` 对应的对照系统：

- 向量 payload 仍然存储在 Ceph OSD。
- 邻接表、label、global meta 仍然存储在 Ceph。
- HNSW 更新逻辑仍然由计算节点协调。
- 与 `Baseline B` 的唯一区别：
  - `Baseline B`：候选向量在 OSD/CLS 内完成距离计算。
  - `Compute-node baseline`：候选向量先从 OSD 读取回计算节点，再在计算节点本地完成距离计算。

这个对照组用于量化“数据移动开销”，为后续算子卸载提供实验依据。

## 系统设计

### 存储侧

- `store_vector`：向量 payload 顺序写入 object payload，`VecKey(global_id)` 记录 `VectorRef`
- `get_node_vector_batch`：按 id 批量返回向量字节
- `get_node_adjacency_batch`：按 id 批量返回邻接表
- `set_adjacency_batch`：写入新点邻接表
- `apply_edge_patch_batch`：原子读改写旧点邻接表
- `get_global_meta/cas_global_meta`：维护全局 `enterpoint/max_level/cur_element_count/next_global_id`

### 计算侧

- `import_base` 保持不变：
  - 数据集加载到计算节点
  - 在计算节点构建 base HNSW
  - 将 payload / adjacency / meta 持久化到 Ceph
- `update` 改为 `--distance-mode compute`
  - 搜索阶段遇到候选 id 后，不再调用 `distance_to_local_batch`
  - 改为调用 `get_node_vector_batch`
  - 将候选向量拉回计算节点
  - 在计算节点本地执行 `L2/IP` 距离计算
  - 再继续搜索、选邻居、patch 邻接表、更新 global meta

## 关键测量指标

新增以下指标用于量化数据移动：

- `remote_vector_bytes`
  - 计算节点从 OSD 拉回的向量总字节数
- `remote_vector_seconds`
  - 所有 `get_node_vector_batch` 的总 wall-clock 时间
- `distance_fetch_rpc_seconds`
  - 距离阶段内，取回候选向量的总耗时
- `distance_local_compute_seconds`
  - 计算节点本地执行距离计算的总耗时
- `distance_compute_node_unaccounted_seconds`
  - 距离阶段中未被前两项覆盖的剩余时间

同时保留原有：

- `remote_distance_seconds`
- `remote_distance_calls`
- `remote_candidates_scored`
- `avg_update_latency_ms`
- `throughput_updates_per_sec`

## 实验方法

### 数据集

- `GIST1M`
- `Text-to-Image10M`
- `Deep100M`
- `SIFT100M`

### 实验流程

每个数据集执行：

1. `reset_baseline_objects.sh`
2. `import_base`
3. `update`，时间窗 `300s`
4. 导出 `update.window_300s.json`

### 对照方式

与已有正式结果 `stage_profile_300s_baseline_b_atomic_patch` 对比：

- 吞吐下降多少
- 平均更新延迟放大多少
- `remote_distance ms/update` 放大多少
- 每次 update 额外搬运多少 MB 向量
- 距离阶段中，数据移动时间与本地计算时间的比例

## 预期瓶颈

若系统确实受数据移动支配，则应观察到：

- `distance_fetch_rpc_seconds >> distance_local_compute_seconds`
- `compute` 版本吞吐显著低于 `Baseline B`
- `compute` 版本平均延迟显著高于 `Baseline B`
- 高维或候选访问更多的数据集上，`remote_vector_bytes` 更大、性能下降更明显

## 运行入口

```bash
cd /path/to/near-storage-vector-update
UPDATE_PARALLELISM=4 ./run_compute_node_distance_stage_profile.sh 300
```

tmux 会话：

```bash
tmux attach -t ghnsw-compute-distance
```
