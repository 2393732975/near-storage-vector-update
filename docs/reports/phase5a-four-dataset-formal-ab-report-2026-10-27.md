# 阶段 5A 四数据集正式 A/B 报告（2026-10-27）

## 结论

阶段 5A 完成四数据集、三次重复、控制/优化配对的 24 个 OSD-only case，共
66,413 次成功更新。24/24 次更新均为 `failed_updates=0`、RADOS retry 为 0，
24/24 次 checker 全部 `pass`、0 error、0 warning；12/12 组共同前缀
Recall@10 均通过最多 0.5 pp 退化门槛。因此只读异步执行器通过了正确性和质量
门禁。

性能总门禁未通过。GIST1M、Text2Image10M 和 SIFT100M 的平均延迟分别降低
24.07%、15.53% 和 18.88%，P99 也同步降低；Deep100M 平均延迟降低 6.25%，
但三轮平均 P99 增加 23.58%，超过允许的 5% 回退。Stage 5A 必须保持默认关闭，
不能因 3/4 数据集通过而宣称正式完成。下一步应先实现阶段 6 的 per-target
inflight/queue 观测和限流，再复测 Deep100M，而不是删除异常轮或放宽门槛。

## 实验资格与配置

- Git commit：`51766ffa7a5e578f58c8553927a47a81378f5085`
- Coordinator SHA-256：`387e6aa33d712e818a8b450c37c683731adaf1e60aa6ef65c9a1ed93a8c25479`
- Importer SHA-256：`0fc7919e004a5ba39a40112e0a437c453108aa33a26103838895ee299c19666a`
- Checker SHA-256：`e16394c99f85a8578727e9da813ab294d7d1fda0742e93a867bbd4e885a22760`
- CLS SHA-256：`1721ad9ba8fc633f34b9e2a4aed3a8f0060cb9e0cdba2baecbb74d2eab4f6106`
- Ceph 17.2.9 Quincy；FSID：`c83da116-1e8b-11f1-933d-cd48aa44d216`
- 每个 case 300 s，更新并发度 4，操作超时 45 s，重试上限 3
- 控制组：M2 OSD 路径，关闭 `DISTANCE_MICROBATCH`
- 优化组：等待 250 μs、5 个执行器线程、256 KiB、256 candidates、
  32 subrequests；frontier window 仍为 1
- 每个 dataset/repetition 单独构建一份 HNSW cache，同一 A/B 对加载同一基础图；
  奇数轮 control→optimized，偶数轮 optimized→control
- 缓存口径：`fresh-pool-after-import`，不是受控冷缓存实验
- 原始工件（本机、不提交）：`results/stage5a-full-ab-20261015T080235Z/`

12 份 cache 的 SHA-256 和字节数保存在原始工件顶层 `cache-manifest.txt`。正式
报告落盘前结果目录约 334 GiB，其中绝大部分是可再生成的 HNSW cache。

## 正确性与质量门禁

- 成功更新总数：66,413；失败更新：0；RADOS retry：0
- checker：24/24 pass；errors=0；warnings=0
- 抽样语义边胜率：0.996190–1.000000
- 配对 Recall：12/12 pass；优化组相对控制组差值为 -0.331 至 +0.118 pp

固定时间窗使优化组通常处理更多 query，因此 Recall 只比较每对实验共同覆盖的
最长 query 前缀。每对实验复用相同的 HNSW cache，避免 importer 随机建图污染
质量对照。此前并行度消融中的不确定 Recall 问题在该口径下没有复现。

## 三轮性能结果

误差项为三个样本均值的 95% Student-t 置信区间。变化百分比使用三轮均值计算。

| 数据集 | M2 延迟 (ms) | Stage 5A 延迟 (ms) | 延迟变化 | M2 / Stage 5A 吞吐 | 吞吐变化 |
| --- | ---: | ---: | ---: | ---: | ---: |
| GIST1M | 338.067 ± 1.778 | 256.706 ± 3.158 | **-24.07%** | 11.824 / 15.570 | **+31.68%** |
| Text2Image10M | 409.550 ± 7.697 | 345.957 ± 8.972 | **-15.53%** | 9.761 / 11.553 | **+18.36%** |
| Deep100M | 511.790 ± 29.470 | 479.816 ± 63.197 | **-6.25%** | 7.809 / 8.345 | **+6.86%** |
| SIFT100M | 525.934 ± 121.294 | 426.638 ± 41.912 | **-18.88%** | 7.646 / 9.375 | **+22.61%** |

| 数据集 | M2 P99 (ms) | Stage 5A P99 (ms) | P99 变化 | Recall 差值范围 | 门禁 |
| --- | ---: | ---: | ---: | ---: | --- |
| GIST1M | 622.998 ± 41.342 | 491.216 ± 21.365 | **-21.15%** | -0.100–+0.080 pp | pass |
| Text2Image10M | 740.556 ± 44.382 | 629.741 ± 42.599 | **-14.96%** | -0.331–-0.049 pp | pass |
| Deep100M | 944.340 ± 414.625 | 1167.051 ± 533.488 | **+23.58%** | -0.068–+0.056 pp | **fail** |
| SIFT100M | 1327.163 ± 1272.200 | 907.419 ± 278.289 | **-31.63%** | -0.145–+0.118 pp | pass |

## 与阶段 2 M2 结果的关系

同提交配对控制组才是 Stage 5A 的直接性能分母。阶段 2 的历史 M2 结果只用于检查
环境漂移：当前控制组平均延迟相对阶段 2 分别变化 -3.59%、+1.55%、-2.21% 和
+4.20%；吞吐变化为 +3.48%、-1.55%、+1.85% 和 -4.12%。四个数据集均在 ±5%
内，说明控制路径没有出现足以解释 Stage 5A 收益的系统性漂移。

阶段 2 的 Deep100M P99 本身也曾在第三轮达到 2163.79 ms，表明大数据集尾部长期
存在波动；但本轮是同基础图配对实验，优化组 P99 仍必须按预设门槛记为失败，不能
用历史波动豁免。

## 阶段开销与收益来源

下表按三轮成功更新数加权，单位为 ms/update；调用数按 update attempt 汇总。

| 数据集 | 版本 | 距离阶段 | RTT/排队 | payload | 邻接 patch | CLS calls/update | subrequests/RPC |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| GIST1M | M2 | 232.60 | 198.04 | 23.89 | 59.65 | 498.69 | — |
| GIST1M | Stage 5A | 145.87 | 115.09 | 20.79 | 60.61 | 455.78 | 1.079 |
| Text2Image10M | M2 | 284.30 | 180.25 | 89.35 | 69.37 | 615.52 | — |
| Text2Image10M | Stage 5A | 214.01 | 106.41 | 92.86 | 73.86 | 592.04 | 1.045 |
| Deep100M | M2 | 358.86 | 208.91 | 124.07 | 74.68 | 748.46 | — |
| Deep100M | Stage 5A | 322.79 | 156.91 | 139.68 | 89.80 | 735.41 | 1.018 |
| SIFT100M | M2 | 361.68 | 212.02 | 124.49 | 84.21 | 717.53 | — |
| SIFT100M | Stage 5A | 280.16 | 132.57 | 122.23 | 83.58 | 696.02 | 1.026 |

四个数据集的 RTT/排队均下降，说明对象级异步提交有效；但物理 CLS 调用仅减少
1.74%–8.60%，`subrequests/RPC` 只有 1.018–1.079，仍不是充分的跨 update 合批。
Deep100M 每次更新触达约 363 个距离对象，优化后 payload 读取增加 12.58%，邻接
patch 增加 20.25%，异步收益被后续对象访问和写竞争部分抵消。

## Deep100M 尾延迟分析

| 重复 | 平均延迟变化 | P50 变化 | P95 变化 | P99 变化 |
| ---: | ---: | ---: | ---: | ---: |
| 1 | -8.47% | -13.01% | +0.06% | +12.51% |
| 2 | -3.14% | -8.81% | +8.74% | +3.08% |
| 3 | -7.26% | -14.16% | +10.32% | **+61.92%** |

第三轮不是整体回退：P50、平均延迟和吞吐仍改善，但分布尾部显著拉长。三轮均为
零失败、零重试，配对 Recall 全部通过。第三轮优化组的平均 executor queue 为
2.720 ms/subrequest，和第二轮的 2.721 ms 基本相同；multi-query CLS 平均往返
0.857 ms，也没有高于第二轮的 0.915 ms。因此现有平均指标不能定位个别慢 update。

最合理但尚未被充分观测证实的解释是：5 个执行线程同时向约 363 个对象发起细粒度
请求，在没有 per-target inflight/cwnd 的情况下产生瞬时排队或写路径竞争。当前
指标只记录全局平均值，缺少 per-target queue P95/P99、inflight 峰值和逐 update
阶段尾延迟，不能进一步归因到网络、某个 OSD 或 BlueStore。该缺口正是阶段 6
需要先补齐的内容。

## 具体代码改动

| 代码位置 | 改动及作用 |
| --- | --- |
| `include/nsvu/protocol.hpp` | 增加 `distance_multi_query_batch` 的多 query 子请求和逐项回复协议。 |
| `src/cls/hnsw_cls.cc` | 在单个对象内复用 VectorRef/payload 读取，同时保持每个 query 独立算距和返回状态。 |
| `src/coordinator/update_coordinator.cc` | 增加共享 `DistanceReadExecutor`、future/promise、对象队列、5 线程异步提交和微批指标；不改变 frontier 依赖顺序。 |
| `src/importer/base_index_importer.cc` | 增加 HNSW cache `build/load`，保证每对控制组和优化组使用完全相同的基础图。 |
| `scripts/compare-paired-recall.py` | 对共同 query 前缀执行 Recall 门禁，并支持固定轨迹逐字段比较。 |
| `scripts/run-phase1-ab.sh` | 接入执行器参数、cache、OSD-only、正确性检查和运行时访问路径门禁。 |
| `scripts/run-stage5a-full-ab.sh` | 自动运行 24 个 case、交替顺序、校验二进制/cache 哈希并生成四数据集门禁汇总。 |

核心实现提交为 `7640e80`、`da6ec8f`、`8b2cbaa`、`4f02a30` 和 `51766ff`。
本阶段只改变只读距离路径；label CAS、生命周期、邻接 patch 和全局元数据仍使用
M2 路径，没有绕过阶段 4 的 intent/saga 门禁。

## 阶段决策与收尾

1. Stage 5A 正确性通过、性能部分通过，但总体门禁失败；默认开关继续关闭。
2. 不继续扩大等待窗口或执行器线程数。先进入阶段 6 的只读 per-target
   inflight/queue 观测与上限控制，重点约束 Deep100M P95/P99。
3. 阶段 6 小规模门禁通过后，先用冻结 cache 做 Deep100M 三轮配对复测；Deep
   P99 不回退后，再重跑四数据集正式门禁。
4. 修改型 edge patch 微批仍被阶段 4 阻塞；本结果不能视为完整阶段 5 完成。

本报告初次落盘时，六个 `nsvu_*` 实验池和 HNSW cache 仍保留。报告提交后使用
FSID 保护的白名单脚本清理实验池，并只删除可由 manifest 重建的 cache；原始
JSON、日志、checker、paired-quality、`cache-manifest.txt` 和 summary 将保留。
