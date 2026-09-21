# 基于近存储算子卸载的向量高效更新机制

本仓库是一个基于 Ceph RADOS 的研究原型，研究大规模向量索引在持续插入、删除和覆盖场景中的高效更新机制。核心问题是：向量、邻接表和元数据已分布在存储节点上，传统计算侧更新会反复搬运候选向量；如何将合适的算子推近数据侧，同时控制远端调用与一致性成本？

## 研究路线

项目包含两条可对照的 HNSW 更新路径：

1. **Compute-node baseline**：Coordinator 从 Ceph 拉取候选向量，在计算节点计算距离并写回图更新。
2. **OSD-side CLS baseline**：Coordinator 保留全局 HNSW 搜索控制；Ceph CLS 在 OSD 内执行局部向量读取、距离计算、label 查询和原子邻接 patch。

一次更新采用“旧节点标记 stale + 新节点重新插入全局图”的语义，而不是原地覆写向量。该设计能够暴露真实的全图在线更新成本。

现有实验表明：计算侧路径的距离阶段主要耗时在候选向量搬运；距离算子下沉后，纯距离计算已不是主瓶颈，主要成本转移到碎片化 `cls_exec` 的网络往返/OSD 排队、跨 owner 邻接 patch，以及 global meta 的 CAS 竞争。

后续研究围绕三层优化展开：图结构感知的数据布局、两级请求聚合与拥塞感知调度、以及将高频状态下沉到 shard-local 的元数据协议。

## 目录结构

```text
include/nsvu/       CLS 与 Coordinator 共享协议、对象命名和编码
src/cls/            OSD 内 Ceph Object Class 实现
src/coordinator/    全局 HNSW 构建与在线更新编排
src/importer/       本地构建 base HNSW 并导入 Ceph
scripts/            构建与安全门控的实验池初始化脚本
config/             不含真实路径的数据集配置示例
docs/               开题材料、实验设计和研究路线
results/            本地产生的指标；默认不纳入 Git
```

## 构建

依赖与本机 Ceph 版本匹配的开发源码、`librados` 以及 `hnswlib` 头文件。显式设置路径后构建：

```bash
export CEPH_SRC=/path/to/ceph/src
export HNSWLIB_INCLUDE=/path/to/hnswlib/include
make
```

产物位于 `build/`：

- `libcls_hnsw_global.so`：部署到测试 OSD 的 CLS 模块；
- `nsvu-update-coordinator`：build/update Coordinator；
- `nsvu-base-importer`：离线 base 导入器。

## 实验与安全

数据集路径、Ceph 配置和 keyring 必须由运行者通过参数或本机环境提供，绝不可提交。`scripts/initialize-experiment-pools.sh` 会删除并重建实验池，必须显式传入 `--confirm-reset`。

`scripts/run-ppt-baselines.sh` 默认运行 `osd compute` 两条路径和全部四个数据集。
使用空格分隔的 `MODES` 与 `DATASETS` 可以只运行指定子集，例如：

```bash
# PPT 第 4–5 页：传统计算节点数据移动实验
MODES=compute DATASETS="gist1m text2image10m deep100m sift100m" \
  scripts/run-ppt-baselines.sh

# PPT 第 11–12 页：OSD/CLS baseline 瓶颈实验
MODES=osd DATASETS="gist1m text2image10m deep100m sift100m" \
  scripts/run-ppt-baselines.sh
```

运行前仍需设置 `CEPH_KEYRING` 与 `DATASET_ROOT`，并按需设置
`WINDOW_SECONDS`、`UPDATE_PARALLELISM` 和 `RUN_ROOT`。

每次实验至少记录：Git commit、CLS 二进制哈希、Ceph 版本、数据集参数、并发度、pool size/PG 数量以及实际 `ceph pg map` 落点。当前 `size=1` 的池仅用于隔离研究开销，不具备生产级数据冗余。

更多设计细节见 [研究路线](docs/research-roadmap.md)、[阶段剖析方法](docs/stage-profiling.md) 和 [计算侧对照实验](docs/compute-node-baseline.md)。
