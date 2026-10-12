# 阶段 5A 只读执行器可行性报告（2026-10-12）

## 结论

阶段 5A 的 GIST1M 单轮可行性扫描完成。相对同提交 M2 控制组，50–500 μs
四个只读异步配置的平均延迟降低 20.35%–23.19%，P99 降低
10.93%–17.23%，吞吐提高 25.30%–30.01%。5/5 轮均为零失败，结构和语义
检查全部通过。因此“并发重叠独立对象读取”具有继续验证的价值。

本轮不能宣称跨 update 合批成功：`subrequests/RPC` 只有 1.108–1.116，
88.82%–89.71% 的 multi-query RPC 仍只携带一个子请求，候选读取复用率仅
0.035%–0.101%。距离 CLS 服务时间和 payload 读取基本不变，主要变化是
RTT/排队从 238.61 降至 123.38–133.28 ms/update，说明收益主要来自 5 个执行
线程重叠独立对象等待。

250 μs 是唯一满足本轮全部门禁的候选值：平均延迟降低 22.42%，P99 降低
17.23%，共同 235-query 前缀 Recall@10 下降 0.468 pp，未超过 0.5 pp 门槛。
但每种配置只有一次 30 s 样本，默认开关继续保持关闭；必须完成独立重复和并行度
消融后才能进入四数据集正式实验。

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

## 实验配置

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

## 延迟、吞吐与质量

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

1. 以 250 μs 为候选值，与同提交 M2 做至少三次独立重导入重复；优先使用固定
   更新数或可复现调度补充配对质量验证。
2. 固定候选等待窗口，扫描执行器线程 `1/2/5/10`；再固定最佳线程数，扫描更新
   并发度 `1/2/4/8`，分离异步重叠与跨 update 合批收益。
3. 在重复和并行度门禁通过前，`DISTANCE_MICROBATCH` 保持默认关闭，不启动四
   数据集正式实验。
4. 阶段 4 intent/saga、恢复器和元数据拆分仍未实施；跨 update 修改型微批继续
   禁止。只读执行器验证不代表完整阶段 5 已完成。
5. 若提高并发后 `subrequests/RPC` 仍接近 1，应推进阶段 7 布局来降低对象扇出，
   而不是继续扩大等待窗口。

## 集群收尾

报告和原始工件确认落盘后，使用带 FSID 保护的白名单脚本删除 `nsvu_meta` 与
`nsvu_owner_0`–`4`。最终仅保留 `.mgr`、`rbd`，5/5 OSD `up/in`，33/33 PG
`active+clean`。集群仍报告既有的 `devicehealth` 模块磁盘 I/O 错误；未屏蔽告警，
也未为消除告警而压缩或重启 OSD。BlueStore 物理空间由后台异步回收。
