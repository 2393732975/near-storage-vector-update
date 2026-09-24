# 实验复现与数据准备手册

本文是本仓库实验的统一运行入口。所有命令都从仓库根目录执行；数据集、Ceph
keyring、日志和原始结果只保存在本机，不提交 Git。实验脚本会删除并重建
`nsvu_*` 池，同一集群任何时刻只能运行一个 runner。

## 1. 实验与入口对应关系

| 实验 | 目的 | 模式 | 入口 |
| --- | --- | --- | --- |
| PPT 第 4–5 页背景实验 | 测量计算节点拉回候选向量的数据移动开销 | `compute` | `scripts/run-ppt-baselines.sh` |
| PPT OSD/CLS baseline | 测量距离算子下沉后的更新性能和阶段开销 | `osd` | `scripts/run-ppt-baselines.sh` |
| 同轮 compute/OSD 对照 | 在同一提交和集群配置下获取两条路径结果 | `compute osd` | `scripts/run-ppt-baselines.sh` |
| 阶段 0 正确性验收 | 三轮 OSD 更新，并在每轮后检查图一致性 | `osd` | `scripts/run-phase0-validation.sh` |
| 阶段耗时汇总 | 汇总已有 `update.json`，不访问集群 | 两者均可 | `scripts/summarize-stage-costs.py` |

前四项会操作 Ceph。阶段耗时汇总是纯离线操作。

## 2. 构建与集群前置条件

准备与 OSD daemon 版本匹配的 Ceph 源码和 hnswlib 头文件：

```bash
export CEPH_SRC=/path/to/ceph/src
export HNSWLIB_INCLUDE=/path/to/hnswlib/include

scripts/build.sh
make CEPH_SRC="$CEPH_SRC" HNSWLIB_INCLUDE="$HNSWLIB_INCLUDE" check
```

构建产物为 importer、Coordinator、离线检查器和
`build/libcls_hnsw_global.so`。协议或 CLS 代码变化后，必须按集群运维流程把同一
CLS 二进制部署到所有 OSD，并逐节点验证 SHA-256；新 Coordinator 不能与旧 CLS
混用。部署和 OSD 重启会改变集群状态，必须另行获得明确授权。

设置运行环境：

```bash
export CEPH_KEYRING=/secure/path/ceph.client.experiment.keyring
export DATASET_ROOT=/data/vector-benchmarks
export EXPECTED_CEPH_FSID="$(ceph --keyring "$CEPH_KEYRING" fsid)"
```

实验前保存并检查：

```bash
git rev-parse HEAD
sha256sum build/nsvu-update-coordinator build/nsvu-base-importer \
  build/libcls_hnsw_global.so
ceph --keyring "$CEPH_KEYRING" versions
ceph --keyring "$CEPH_KEYRING" osd stat
ceph --keyring "$CEPH_KEYRING" pg stat
```

要求所有 OSD 为 `up/in`，所有 PG 为 `active+clean`。不得通过屏蔽健康告警来满足
实验条件。

## 3. 数据集参数与目录

runner 的数据参数固定在脚本中。目录和文件名必须如下：

| 数据集 | base / update 文件 | 数量 / 维度 / 类型 | 距离 | 导入线程 | points/object |
| --- | --- | --- | --- | --- | ---: |
| GIST1M | `gist1m/base.1M.fbin` / `query.public.1K.fbin` | 1M / 960 / f32 | L2 | 32 | 16,000 |
| Text2Image10M | `text2image10m/base.10M.fbin` / `query.public.100K.fbin` | 10M / 200 / f32 | IP | 48 | 60,000 |
| Deep100M | `deep100m/base.100M.fbin` / `query.public.10K.fbin` | 100M / 96 / f32 | L2 | 64 | 160,000 |
| SIFT100M | `sift100m/base.u8bin` / `queries.u8bin` | 100M / 128 / u8 | L2 | 64 | 250,000 |

当前更新上限分别为 1,000、10,000、10,000 和 10,000。固定时间窗通常会在达到
上限前停止。`.fbin` 和 `.u8bin` 都以两个 little-endian `uint32` 开头，依次为
向量数和维度，随后是连续向量元素。

阶段 1 还要求以下 ground-truth 文件。它们以两个 little-endian `uint32` 开头，
依次为 query 数和每条 query 的邻居数，随后是连续 `uint32` base ID：

| 数据集 | ground truth | header |
| --- | --- | ---: |
| GIST1M | `gist1m/gt.public.1K.top1000.ibin` | 1,000 × 1,000 |
| Text2Image10M | `text2image10m/text2image-10M.gt.bin` | 100,000 × 100 |
| Deep100M | `deep100m/deep-100M.gt.bin` | 10,000 × 100 |
| SIFT100M | `sift100m/gt_100.bin` | 10,000 × 100 |

### 3.1 Deep100M 与 Text2Image10M

Deep1B 和 Text-to-Image1B 的文件地址来自
[Big ANN Benchmarks 数据集注册表](https://github.com/harsha-simhadri/big-ann-benchmarks/blob/main/benchmark/datasets.py)。
本项目只取 base 文件前 100M/10M 个向量：

```bash
mkdir -p "$DATASET_ROOT/deep100m" "$DATASET_ROOT/text2image10m"

curl -L --fail --range 0-38400000007 \
  https://storage.yandexcloud.net/yandex-research/ann-datasets/DEEP/base.1B.fbin \
  -o "$DATASET_ROOT/deep100m/base.100M.fbin"
curl -L --fail \
  https://storage.yandexcloud.net/yandex-research/ann-datasets/DEEP/query.public.10K.fbin \
  -o "$DATASET_ROOT/deep100m/query.public.10K.fbin"

curl -L --fail --range 0-8000000007 \
  https://storage.yandexcloud.net/yandex-research/ann-datasets/T2I/base.1B.fbin \
  -o "$DATASET_ROOT/text2image10m/base.10M.fbin"
curl -L --fail \
  https://storage.yandexcloud.net/yandex-research/ann-datasets/T2I/query.public.100K.fbin \
  -o "$DATASET_ROOT/text2image10m/query.public.100K.fbin"
```

服务器必须支持 HTTP Range；下载后一定检查文件大小。截取文件保留原始 1B header，
runner 通过 `--num-vectors` 限制实际读取数量，这是预期行为。

### 3.2 SIFT100M

SIFT 使用 Big ANN Benchmarks 的 `BigANNDataset` 公共文件，截取 base 的前 100M：

```bash
mkdir -p "$DATASET_ROOT/sift100m"

curl -L --fail --range 0-12800000007 \
  https://dl.fbaipublicfiles.com/billion-scale-ann-benchmarks/bigann/base.1B.u8bin \
  -o "$DATASET_ROOT/sift100m/base.u8bin"
curl -L --fail \
  https://dl.fbaipublicfiles.com/billion-scale-ann-benchmarks/bigann/query.public.10K.u8bin \
  -o "$DATASET_ROOT/sift100m/queries.u8bin"
```

也可以使用已准备为 100M header 的可信镜像，但必须记录来源、文件大小和
SHA-256，不能混用不同版本。

### 3.3 GIST1M

GIST1M 的原始 `fvecs` 来源由
[ANN Benchmarks 的 GIST 配置](https://github.com/erikbern/ann-benchmarks/blob/main/ann_benchmarks/datasets.py)
指向 TexMex corpus：

```bash
mkdir -p "$DATASET_ROOT/gist1m"
curl -L --fail \
  ftp://ftp.irisa.fr/local/texmex/corpus/gist.tar.gz \
  -o "$DATASET_ROOT/gist1m/gist.tar.gz"
tar -xzf "$DATASET_ROOT/gist1m/gist.tar.gz" -C "$DATASET_ROOT/gist1m"
```

将 `gist_base.fvecs` 和 `gist_query.fvecs` 转为 runner 使用的 `.fbin`：

```bash
python3 - "$DATASET_ROOT/gist1m" <<'PY'
import struct
import sys
from pathlib import Path

root = Path(sys.argv[1])

def convert(source, target):
    size = source.stat().st_size
    with source.open("rb") as src:
        dim = struct.unpack("<I", src.read(4))[0]
    record_bytes = 4 * (dim + 1)
    assert size % record_bytes == 0
    count = size // record_bytes
    with source.open("rb") as src, target.open("wb") as dst:
        dst.write(struct.pack("<II", count, dim))
        while True:
            block = src.read(record_bytes * 4096)
            if not block:
                break
            assert len(block) % record_bytes == 0
            rows = len(block) // record_bytes
            vectors = bytearray(rows * dim * 4)
            for row in range(rows):
                source_offset = row * record_bytes
                assert struct.unpack_from("<I", block, source_offset)[0] == dim
                target_offset = row * dim * 4
                vectors[target_offset:target_offset + dim * 4] = \
                    block[source_offset + 4:source_offset + record_bytes]
            dst.write(vectors)

convert(root / "gist/gist_base.fvecs", root / "base.1M.fbin")
convert(root / "gist/gist_query.fvecs", root / "query.public.1K.fbin")
PY
```

### 3.4 数据完整性检查

当前四组文件的期望大小为：

| 文件 | 字节数 |
| --- | ---: |
| `gist1m/base.1M.fbin` | 3,840,000,008 |
| `gist1m/query.public.1K.fbin` | 3,840,008 |
| `text2image10m/base.10M.fbin` | 8,000,000,008 |
| `text2image10m/query.public.100K.fbin` | 80,000,008 |
| `deep100m/base.100M.fbin` | 38,400,000,008 |
| `deep100m/query.public.10K.fbin` | 3,840,008 |
| `sift100m/base.u8bin` | 12,800,000,008 |
| `sift100m/queries.u8bin` | 1,280,008 |

执行一次大小和 SHA-256 检查，并把输出保存到实验 manifest；不要把大型数据文件
或校验日志提交仓库：

```bash
find "$DATASET_ROOT" -type f \( -name '*.fbin' -o -name '*.u8bin' \) \
  -exec stat -c '%n %s bytes' {} +
sha256sum \
  "$DATASET_ROOT"/gist1m/{base.1M,query.public.1K}.fbin \
  "$DATASET_ROOT"/text2image10m/{base.10M,query.public.100K}.fbin \
  "$DATASET_ROOT"/deep100m/{base.100M,query.public.10K}.fbin \
  "$DATASET_ROOT"/sift100m/{base,queries}.u8bin
```

读取每个文件的 header：

```bash
python3 - "$DATASET_ROOT" <<'PY'
import struct
import sys
from pathlib import Path

root = Path(sys.argv[1])
names = (
    "gist1m/base.1M.fbin", "gist1m/query.public.1K.fbin",
    "text2image10m/base.10M.fbin", "text2image10m/query.public.100K.fbin",
    "deep100m/base.100M.fbin", "deep100m/query.public.10K.fbin",
    "sift100m/base.u8bin", "sift100m/queries.u8bin",
)
for name in names:
    path = root / name
    with path.open("rb") as stream:
        count, dim = struct.unpack("<II", stream.read(8))
    print(f"{name}: header_count={count} dim={dim} bytes={path.stat().st_size}")
PY
```

PPT baseline runner 不使用 ground truth；阶段 1 runner 要求相同规模、相同 query
的 ground-truth 文件，并按更新提交前搜索结果计算静态 Recall@K。

## 4. 运行 PPT 背景 compute 实验

该实验把候选向量拉回 Coordinator，再在计算节点算距。务必创建从未使用过的
`RUN_ROOT`：

```bash
run_root="$PWD/results/ppt-background-$(date -u +%Y%m%dT%H%M%SZ)"
test ! -e "$run_root"
mkdir -p "$run_root"

nohup env \
  CEPH_KEYRING="$CEPH_KEYRING" \
  DATASET_ROOT="$DATASET_ROOT" \
  MODES=compute \
  DATASETS="gist1m text2image10m deep100m sift100m" \
  WINDOW_SECONDS=300 \
  UPDATE_PARALLELISM=4 \
  RUN_ROOT="$run_root" \
  scripts/run-ppt-baselines.sh \
  >"$run_root/runner.log" 2>&1 &

runner_pid=$!
printf '%s\n' "$runner_pid" >"$run_root/runner.pid"
printf 'PID=%s RUN_ROOT=%s\n' "$runner_pid" "$run_root"
```

## 5. 运行 OSD/CLS baseline

参数与背景实验保持一致，只改变 `MODES`：

```bash
run_root="$PWD/results/ppt-osd-$(date -u +%Y%m%dT%H%M%SZ)"
test ! -e "$run_root"
mkdir -p "$run_root"

nohup env \
  CEPH_KEYRING="$CEPH_KEYRING" \
  DATASET_ROOT="$DATASET_ROOT" \
  MODES=osd \
  DATASETS="gist1m text2image10m deep100m sift100m" \
  WINDOW_SECONDS=300 \
  UPDATE_PARALLELISM=4 \
  RUN_ROOT="$run_root" \
  scripts/run-ppt-baselines.sh \
  >"$run_root/runner.log" 2>&1 &

printf '%s\n' "$!" >"$run_root/runner.pid"
```

## 6. 运行阶段 1 严格 compute/OSD 对照

正式对照使用 `run-phase1-ab.sh`。它会对每个数据集和模式重新建池、重新导入
base，执行三次独立重复；奇数轮按 compute→OSD、偶数轮按 OSD→compute，减少
固定顺序偏差。每次更新后运行一致性检查，并统计提交前 level-0 搜索相对原始
base ground truth 的静态 Recall@10。检查器还会从原始 base 均匀抽样节点，将每条
level-0 图边与确定性的随机对照边比较；图边距离优于对照的比例必须达到 0.60，
用于拦截 internal ID 与 external label 错配这类“结构合法、语义错误”的索引。

```bash
run_root="$PWD/results/phase1-ab-$(date -u +%Y%m%dT%H%M%SZ)"
test ! -e "$run_root"
mkdir -p "$run_root"

nohup env \
  CEPH_KEYRING="$CEPH_KEYRING" \
  DATASET_ROOT="$DATASET_ROOT" \
  DATASETS="gist1m text2image10m deep100m sift100m" \
  REPETITIONS=3 \
  WINDOW_SECONDS=300 \
  UPDATE_PARALLELISM=4 \
  CHECKER_BATCH_SIZE=8192 \
  SEMANTIC_SAMPLES=256 \
  SEMANTIC_MIN_EDGE_WIN_RATE=0.60 \
  OSD_OP_TIMEOUT_SECONDS=45 \
  OSD_OP_RETRY_LIMIT=3 \
  RECALL_K=10 \
  RUN_ROOT="$run_root" \
  scripts/run-phase1-ab.sh --confirm-reset \
  >"$run_root/runner.log" 2>&1 &

printf '%s\n' "$!" >"$run_root/runner.pid"
printf 'PID=%s RUN_ROOT=%s\n' "$!" "$run_root"
```

ground truth 必须位于第 3 节列出的数据集目录。该 Recall 是更新提交前的静态
回归门槛，不是修改后语料库的精确 Recall；精确动态质量评估需要重新计算 ground
truth。每轮虽然使用新池，但导入会预热缓存，因此结果应标记为
`fresh-pool-after-import`，不能声称是受控冷缓存结果。

正常结束后自动生成 `summary.md` 和 `summary.json`。若需要重新汇总：

```bash
python3 scripts/summarize-phase1-ab.py "$run_root" \
  --output "$run_root/summary.md" --json-output "$run_root/summary.json"
```

## 7. 运行阶段 0 正确性验收

阶段 0 固定使用 OSD 模式，每个数据集只导入一次，然后连续更新三轮；每轮结束后
运行离线索引检查器。它比 baseline 更慢，但结果可用于正确性验收：

```bash
run_root="$PWD/results/phase0-validation-$(date -u +%Y%m%dT%H%M%SZ)"
test ! -e "$run_root"

CEPH_KEYRING="$CEPH_KEYRING" \
DATASET_ROOT="$DATASET_ROOT" \
DATASETS="gist1m text2image10m deep100m sift100m" \
ROUNDS=3 \
WINDOW_SECONDS=300 \
UPDATE_PARALLELISM=4 \
CHECKER_BATCH_SIZE=8192 \
OSD_OP_TIMEOUT_SECONDS=45 \
OSD_OP_RETRY_LIMIT=3 \
RUN_ROOT="$run_root" \
scripts/run-phase0-validation.sh --confirm-reset
```

通过条件是每轮 `failed_updates=0`，并且 `index-check.json` 中
`status=pass`、`errors=0`、`warnings=0`。

## 8. 参数说明

| 参数 | baseline | 阶段 0 | 阶段 1 | 建议 |
| --- | ---: | ---: | ---: | --- |
| `DATASETS` | 四数据集 | 四数据集 | 四数据集 | 调试可选子集，正式实验全部运行 |
| `WINDOW_SECONDS` | 300 | 300 | 300 | 正式对照保持一致 |
| `UPDATE_PARALLELISM` | 1 | 4 | 4 | 本项目正式基线显式设为 4 |
| `RUN_ROOT` | 自动时间戳 | 自动时间戳 | 自动时间戳 | 始终使用新目录 |
| `ROUNDS` / `REPETITIONS` | 不适用 | 3 | 3 | 阶段 1 必须是独立重新导入 |
| `CHECKER_BATCH_SIZE` | 不适用 | 8192 | 8192 | 内存不足时降低 |
| `SEMANTIC_SAMPLES` | 不适用 | 不适用 | 256 | 正式实验不得关闭语义抽样 |
| `SEMANTIC_MIN_EDGE_WIN_RATE` | 不适用 | 不适用 | 0.60 | 图边优于随机对照的最低比例 |
| `RECALL_K` | 不适用 | 不适用 | 10 | 当前 ground truth 至少含 top-100 |
| `OSD_OP_TIMEOUT_SECONDS` | 45 | 45 | 45 | 不为掩盖 slow-op 而任意增大 |
| `OSD_OP_RETRY_LIMIT` | 3 | 3 | 3 | 记录在 manifest |

缩短窗口、减少数据集或降低检查规模只能用于 smoke test，不能与正式结果混合。

## 9. 进度、输出与汇总

启动前先确认没有重复 runner：

```bash
pgrep -af 'run-ppt-baselines|run-phase0-validation|run-phase1-ab|nsvu-base-importer|nsvu-update-coordinator'
```

baseline 输出结构：

```text
RUN_ROOT/<mode>/<dataset>/
├── import.json
├── import.progress.json
├── update.json
└── update.progress.json
```

阶段 0 还包含 `manifest.txt`、`suite.log`、`placement.txt`，以及每轮的
`round-N/index-check.json`。阶段 1 的单轮路径为
`rep-N/<mode>/<dataset>/`，正常结束标志是日志中的
`phase-1 strict A/B passed`。检查进度和完成标志：

```bash
ps -p "$(cat "$run_root/runner.pid")" -o pid,stat,etime,cmd
tail -n 50 "$run_root/runner.log"
find "$run_root" -name update.json -o -name index-check.json
```

baseline 正常结束时日志包含 `PPT baseline outputs:`。生成阶段汇总：

```bash
python3 scripts/summarize-stage-costs.py "$run_root" \
  --output "$run_root/stage-costs.md"
```

至少检查 `failed_updates`、吞吐、平均延迟、P99、`remote_vector_bytes`、
`stage_profile` 和失败分类。并发运行时不要直接相加 JSON 的旧 `*_pct`；具体统计
口径见[阶段耗时与数据移动打点](stage-profiling.md)。

## 10. 实验后清理

先预览白名单，再带 FSID 保护执行删除：

```bash
CEPH_KEYRING="$CEPH_KEYRING" \
EXPECTED_CEPH_FSID="$EXPECTED_CEPH_FSID" \
scripts/cleanup-experiment-pools.sh

CEPH_KEYRING="$CEPH_KEYRING" \
EXPECTED_CEPH_FSID="$EXPECTED_CEPH_FSID" \
scripts/cleanup-experiment-pools.sh --confirm-cleanup
```

脚本只删除 `nsvu_meta` 和 `nsvu_owner_0`–`nsvu_owner_4`。退出码 3 表示池已成功
删除、PG 已恢复 `active+clean`，但集群仍有其他健康告警。实验池删除后无法从
Ceph 恢复，必须先确认所有 JSON、报告和日志已落盘；BlueStore 空间回收是异步的。

## 11. 正式实验记录清单

每次正式运行必须记录：

- Git commit，以及 Coordinator、Importer、CLS 的 SHA-256；
- 数据来源、文件大小、header、SHA-256 和实际 `--num-vectors`；
- Ceph daemon/client 版本、FSID、健康状态；
- pool size、PG 数和实际 owner→OSD 落点；
- 模式、数据集、窗口、并发度、超时、重试和探针参数；
- 成功/失败更新数、失败分类、一致性检查结果；
- 吞吐、平均与尾延迟、数据移动量和分阶段耗时；
- 运行期间的 slow-op、OSD 重启、网络异常等干扰因素。

原始工件保留在 `results/`，仓库只提交经过核对的汇总报告。任何零失败但未执行
一致性检查的 baseline，都应明确标注“性能结果”，不能称为完整正确性验收。
