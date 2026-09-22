# 阶段 0：正确性与观测基线

本阶段把性能实验的前提固定为“更新无失败且索引一致”。Coordinator 现在先完成新
节点、邻接和全局元数据，再通过 OSD 端 `cas_label` 原子切换 label，最后将旧节点
标记为 stale。多个 Coordinator 同时覆盖同一 label 时，只有一个 CAS 成功；失败
方将新节点标记为 stale，并记录为 conflict。`apply_edge_patch_batch` 使用索引实际
的 `M`，不再固定为 8。

## 构建与本地验证

```bash
export CEPH_SRC=/home/hhf/ceph/src
export HNSWLIB_INCLUDE=/home/hhf/motivation_test/hnsw_test/include
make -j2
make check
```

协议变更同时涉及 Coordinator 和 CLS。运行更新前必须将本次构建的
`build/libcls_hnsw_global.so` 部署到所有测试 OSD；禁止让新 Coordinator 与旧 CLS
混用。

## 一致性检查

基础数据导入或更新结束后运行：

```bash
build/nsvu-index-checker \
  --keyring "$CEPH_KEYRING" \
  --owners 5 \
  --points-per-object 250000 \
  --output results/index-check.json
```

检查器直接读取 RADOS 对象和 OMAP，不依赖 CLS。它验证 VectorRef、payload 范围、
adjacency、节点 level、度数上限、重复/自环/越界边、节点计数、entrypoint，以及
label 的归属、解码和 ACTIVE 目标。退出码 `0` 表示通过，`1` 表示发现不变量错误，
`2` 表示连接、读取或参数错误。对 100M 节点检查时，节点状态表约占 100 MiB。

## 新增观测字段

更新指标 JSON 新增 `failure_breakdown` 和 `observability`。重点字段包括：

- `label_cas_conflicts` 与 timeout/conflict/not_found/protocol/other 分类；
- 每次 update attempt 的 CLS 调用数、请求/回复字节；
- 距离批次数、平均/最大候选数；
- 每次更新触达的数据对象、PG 和 owner 数；
- 新邻接中的跨 owner 边数量与比例。

`--distance-split-probe` 的 noop 探针现在默认每个目标最多每秒一次，可用
`--distance-probe-interval-ms` 调整。PG 数由 librados 当前 OSDMap 计算；owner 数
是逻辑分片数，不等同于实际 OSD 数，正式实验仍须在 manifest 中记录 PG/OSD 映射。
Coordinator 会先落盘 metrics，再在存在失败更新时以非零状态退出。
在线更新使用 64 位 `update_id`。ID 预留、元数据 finalize 和 edge patch 在其修改
的对象内保存去重标记；向量/邻接创建、label CAS 和 stale 标记也支持安全重放。
因此 RADOS 操作可以在有界超时后按同一请求重试，既不会永久等待，也不会把响应
丢失误判成一个全新的写操作。正式 runner 会在 manifest 和 update metrics 中记录
`OSD_OP_TIMEOUT_SECONDS`（默认 45）与 `OSD_OP_RETRY_LIMIT`（默认 3）。

## 验收门槛

每个数据集连续运行三轮，要求 `failed_updates=0`、检查器 `status=pass`，并保存
metrics、检查报告、Git commit、CLS 哈希和集群映射。当前协议还没有 intent/log；
跨进程崩溃后的完整 saga 恢复仍属于阶段 2；阶段 0 的幂等标记只保证单次运行内的
超时重试安全。检查器会继续暴露强制终止留下的未完成跨对象更新。

正式四数据集验收可通过安全门控 runner 执行；它会为每个数据集重建实验池：

```bash
CEPH_KEYRING=/path/to/keyring DATASET_ROOT=/path/to/datasets \
  scripts/run-phase0-validation.sh --confirm-reset
```
