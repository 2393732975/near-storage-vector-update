# 阶段 5A 只读执行器可行性与重复验证报告（2026-10-12）

## 结论

阶段 5A 的 GIST1M 单轮参数扫描和 250 μs 三轮独立重复均已完成。正式重复中，
250 μs 只读执行器相对同提交 M2 控制组将平均延迟从 452.44 ms 降至
358.91 ms（-20.67%），P99 从 760.50 ms 降至 631.77 ms（-16.93%），吞吐
从 8.786 提高到 11.061 update/s（+25.89%）。6/6 轮均为零失败，结构和语义
检查全部通过；三轮共同 query 前缀 Recall@10 门禁也全部通过。因此“并发重叠
独立对象读取”已经通过重复性门禁，可以进入执行器线程数和更新并发度消融。

仍不能宣称跨 update 合批成功：单轮扫描的 `subrequests/RPC` 为 1.108–1.116，
三轮正式配置均值也只有 1.112。单轮扫描中 88.82%–89.71% 的 multi-query RPC
仍只携带一个子请求，候选读取复用率仅
0.035%–0.101%。距离 CLS 服务时间和 payload 读取基本不变，主要变化是
RTT/排队从 238.61 降至 123.38–133.28 ms/update，说明收益主要来自 5 个执行
线程重叠独立对象等待。

250 μs 是单轮扫描中唯一满足全部门禁的候选值。后续三轮重复的配对 Recall 差值
分别为 +0.917、+0.458 和 +0.747 pp，均未触发最大 0.5 pp 退化门槛。由于目前只
验证了 GIST1M、更新并发度 4 和执行器线程 5，默认开关继续保持关闭；必须完成
线程数/更新并发度消融后才能决定是否进入四数据集正式实验。

## 实现范围

- `include/nsvu/protocol.hpp`：新增 multi-query 子请求、逐项回复和
  `kDistanceMultiQueryBatch` opcode。
- `src/cls/hnsw_cls.cc`：同一对象内一次读取 VectorRef/payload，对重叠 candidate
  去重读取，再按每个 query 独立计算距离和返回状态。
- `src/coordinator/update_coordinator.cc`：新增共享 `DistanceReadExecutor`、按
  owner/chunk 的队列、future/promise、等待窗口和 5 线程提交；同一 frontier 的
  不同对象先全部提交再等待结果，搜索顺序不变。
- `scripts/run-phase1-ab.sh`：增加开关、容量、线程参数、manifest 记录和
  multi-query 实际调用门禁。

实现提交为 `7640e80`、`da6ec8f`，门禁文档提交为 `35f7f1f`。本阶段只改只读
距离路径；邻接 patch、label CAS、节点生命周期和元数据写仍走 M2 原路径，没有
绕过尚未实施的阶段 4 intent/saga 门禁。

## 单轮可行性扫描配置

- Git commit：`35f7f1f1a1233f2afe9364efa9fc6eec835f2f47`
- Coordinator SHA-256：`561986c974b0d0a2a81388cb948417405e3ae98e5b2cad053d5642856d059e5e`
- CLS SHA-256：`1721ad9ba8fc633f34b9e2a4aed3a8f0060cb9e0cdba2baecbb74d2eab4f6106`
- Ceph 17.2.9 Quincy；FSID：`c83da116-1e8b-11f1-933d-cd48aa44d216`
- GIST1M、OSD-only、每组独立建池和重导入、30 s、更新并发度 4
- W1、adaptive 关闭；执行器线程 5；上限 256 KiB、256 candidates、32 subrequests
- 控制组关闭 `DISTANCE_MICROBATCH`；实验组等待 50/100/250/500 μs
- `nsvu_meta` 固定到 OSD 0；owner 0–4 分别固定到 OSD 0–4；pool size/min_size 为 1
- 缓存口径为 `fresh-pool-after-import`，不是受控冷缓存
- 原始工件（本机、不提交）：
  `results/stage5-read-feasibility-20261012T024626Z/`

## 三轮正式重复验证

正式重复运行于提交 `4a06031e3e6097cea707e93cd7020229cb73f8c7`。该提交只修复
实验池创建时 PG autoscaler 的竞态：创建池时原子设置 `pg_num_min=1`、
`pg_num_max=1` 和 `autoscale_mode=off`，并在放置前校验。Coordinator 与 CLS
二进制哈希仍分别为 `561986c974b0d0a2a81388cb948417405e3ae98e5b2cad053d5642856d059e5e`
和 `1721ad9ba8fc633f34b9e2a4aed3a8f0060cb9e0cdba2baecbb74d2eab4f6106`。

三轮均重新建池和重导入 GIST1M，OSD-only、30 s、更新并发度 4、执行器线程 5；
实验组使用 250 μs 等待窗口，控制组关闭 microbatch。奇数轮 control→optimized，
偶数轮 optimized→control。每个池固定为 1 PG、size/min_size=1，meta/owner 的实际
落点分别固定到 OSD 0/0–4。原始工件位于
`results/stage5-read-repeat-20261012T040134Z/`，未提交仓库。

下表误差项为三个独立样本均值的 95% Student-t 置信区间：

| 指标 | M2 control | wait 250 μs | 相对变化 |
| --- | ---: | ---: | ---: |
| 成功更新数/30 s | 240.33 ± 1.43 | 301.33 ± 6.25 | +25.38% |
| 平均延迟 | 452.44 ± 4.53 ms | 358.91 ± 6.57 ms | **-20.67%** |
| P99 | 760.50 ± 54.87 ms | 631.77 ± 92.82 ms | **-16.93%** |
| 吞吐 | 8.786 ± 0.094/s | 11.061 ± 0.216/s | **+25.89%** |
| CLS calls/update | 547.00 ± 12.00 | 498.80 ± 4.03 | -8.81% |
| distance batches/update | 459.73 ± 11.09 | 413.03 ± 2.87 | -10.16% |

6/6 轮 `failed_updates=0`，checker 全部 `pass`、0 error、0 warning，语义边胜率
为 0.993519–0.997232。固定时间窗使实验组处理更多 query，因此质量判定使用每轮
控制组长度决定的共同前缀：

| 重复 | 共同 query 数 | M2 Recall@10 | wait 250 μs Recall@10 | 差值 | 门禁 |
| ---: | ---: | ---: | ---: | ---: | --- |
| 1 | 240 | 0.3792 | 0.3883 | +0.917 pp | pass |
| 2 | 240 | 0.3729 | 0.3775 | +0.458 pp | pass |
| 3 | 241 | 0.3793 | 0.3867 | +0.747 pp | pass |

三轮平均距离阶段由 336.73 降到 233.24 ms/update（-30.73%），其中
roundtrip/queue 由 231.01 降到 126.45 ms/update（-45.26%）；payload 读取由
94.13 变为 94.72 ms/update（+0.63%），没有实质改善。邻接 patch 则从 63.93
升到 67.88 ms/update（+6.17%）。这进一步说明收益来自客户端对独立对象等待的
异步重叠，而非 OSD 内部读取或算距变快；距离阶段缩短后，邻接修补的重要性正在
上升。

优化组三轮的 `subrequests/RPC` 为 1.113、1.114、1.110，均值 1.112；执行器
排队约 1.84 ms/subrequest。物理 distance batches/update 只下降 10.16%，未达到
完整阶段 5 的调用数目标。因此重复验证通过的是 Stage 5A 只读执行器性能门禁，
不是完整两级微批或修改型批处理门禁。

## 单轮等待窗口扫描

| 配置 | 更新数 | 平均延迟 | 相对 M2 | P99 | 相对 M2 | 吞吐 | 相对 M2 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| M2 control | 235 | 462.14 ms | — | 733.69 ms | — | 8.602/s | — |
| wait 50 μs | 305 | 354.99 ms | -23.19% | 653.49 ms | -10.93% | 11.183/s | +30.01% |
| wait 100 μs | 300 | 360.73 ms | -21.94% | 652.05 ms | -11.13% | 11.013/s | +28.03% |
| wait 250 μs | 303 | 358.54 ms | -22.42% | 607.27 ms | -17.23% | 11.060/s | +28.58% |
| wait 500 μs | 294 | 368.08 ms | -20.35% | 626.06 ms | -14.67% | 10.778/s | +25.30% |

所有轮次均为 `failed_updates=0`、checker `pass`、0 error、0 warning；语义边胜率
为 0.994531–0.997209。固定时间窗使各配置处理的 query 数不同，因此质量门禁使用
控制组覆盖的共同 235-query 前缀：

| 配置 | M2 Recall@10 | 实验 Recall@10 | 差值 | 0.5 pp 门禁 |
| --- | ---: | ---: | ---: | --- |
| wait 50 μs | 0.4021 | 0.3770 | -2.511 pp | fail |
| wait 100 μs | 0.4021 | 0.3872 | -1.489 pp | fail |
| wait 250 μs | 0.4021 | 0.3974 | -0.468 pp | pass |
| wait 500 μs | 0.4021 | 0.3957 | -0.638 pp | fail |

等待窗口不改变 HNSW frontier 语义，Recall 差异更可能来自 4 个并发更新的执行
交错、独立重导入后的图演化和单次样本波动；不能把 50/100/500 μs 的单轮失败
直接解释为确定性的算法质量退化，也不能放宽门槛将其记为通过。

## 调用形态与收益来源

| 配置 | CLS calls/update | distance batches/update | subrequests/RPC | 单子请求 RPC | 执行器排队 | 距离 RTT/排队 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| M2 control | 545.86 | 458.52 | — | — | — | 238.61 ms |
| wait 50 μs | 507.96 | 421.11 | 1.108 | 89.71% | 1.735 ms/subreq | 123.38 ms |
| wait 100 μs | 510.10 | 423.44 | 1.109 | 89.52% | 1.825 ms/subreq | 126.33 ms |
| wait 250 μs | 504.23 | 417.74 | 1.111 | 89.32% | 1.814 ms/subreq | 127.19 ms |
| wait 500 μs | 504.27 | 417.70 | 1.116 | 88.82% | 1.881 ms/subreq | 133.28 ms |

物理 distance batch 只下降 7.65%–8.90%，全部 CLS calls/update 只下降
6.55%–7.63%，远未达到完整阶段 5 的 60% 目标。每个逻辑子请求仍只有约
1.105–1.109 个候选，query 数据为 1719–1758 KiB/update，没有减少。与此同时，
距离 CLS 内部计算约 106–109 ms/update、payload 读取约 94–97 ms/update，均与
控制组接近；显著下降的是客户端等待。因此当前成果是“异步执行器可行”，不是
“微批已经解决小 RPC”。

## 后续决策

1. 三次独立重导入和共同前缀 Recall 复核已经完成，250 μs 通过重复性门禁。
2. 固定 250 μs 等待窗口，扫描执行器线程 `1/2/5/10`；再固定最佳线程数，扫描更新
   并发度 `1/2/4/8`，分离异步重叠与跨 update 合批收益。
3. 在并行度门禁通过前，`DISTANCE_MICROBATCH` 保持默认关闭，不启动四
   数据集正式实验。
4. 阶段 4 intent/saga、恢复器和元数据拆分仍未实施；跨 update 修改型微批继续
   禁止。只读执行器验证不代表完整阶段 5 已完成。
5. 若提高并发后 `subrequests/RPC` 仍接近 1，应推进阶段 7 布局来降低对象扇出，
   而不是继续扩大等待窗口。

## 集群收尾

单轮扫描和三轮重复使用的实验池均已清理。确认报告和原始工件落盘后，使用 FSID
`c83da116-1e8b-11f1-933d-cd48aa44d216` 保护的白名单脚本删除 `nsvu_meta` 与
`nsvu_owner_0`–`4`。最终没有 `nsvu_*` 残留，只保留 `.mgr`、`rbd`；5/5 OSD
`up/in`，33/33 PG `active+clean`。

清理脚本返回码 3，因为集群仍报告既有的 `devicehealth` 模块磁盘 I/O 错误；这不
表示池删除失败。未屏蔽告警，也未为消除告警而强制压缩或重启 OSD；BlueStore
物理空间由后台异步回收。
