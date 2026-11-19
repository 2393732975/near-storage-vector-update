# 故障 osd.1 数据迁移与停用记录（2026-11-19）

用户明确要求迁移数据并停用故障 SSD。本次已完成迁移及持久停用，最终验收时间为
2026-11-19 03:47:21 UTC。当前 osd.0、osd.2、osd.3、osd.4 为 `up/in`；
osd.1 保留在 OSDMap 中，为 `down/out`，对应服务 `inactive/disabled`，无运行容器。
39 个 PG 全部 `active+clean`，8 个池迁移前后的完整原生导出文件 SHA-256 一致。

## 1. 目标、授权和迁移前状态

- FSID：`c83da116-1e8b-11f1-933d-cd48aa44d216`。
- 目标：n118 的 osd.1，`/dev/sdd`，序列号 `S45PNC1RC02813`。
- 操作时源码提交：`741127c`；实际 OSD 版本 17.2.8。
- 迁移前 5/5 OSD `up/in`，39 个 PG clean，无运行中的实验进程。
- 目标承载 17 个三副本 RBD PG，以及 `nsvu_owner_1` 的单副本 PG `1673.0`。
- RBD 与 `.mgr` 保持 size=3/min_size=2；六个实验池保持 size=1/min_size=1。

本次授权覆盖停用故障盘所需的 Ceph 数据迁移，包括受影响 RBD 副本的自动恢复。
没有删除、重建实验池或 RBD 池，没有降低副本数，没有 purge/destroy/zap。
原 OSD 身份、认证、CRUSH 条目、LVM 和磁盘内容保留，供后续受控换盘。

## 2. 执行时间线

| UTC 时间 | 已执行操作与结果 |
|---|---|
| 03:30 起 | 保存 OSDMap、池定义、PG 状态、设备身份、服务状态 |
| 03:35:13 | 8 个池原生 `rados export` 备份全部完成，保存文件大小和 SHA-256 |
| 03:35:42 | `ceph osd out 1` 成功，保留源服务在线供回填 |
| 03:36:47 | 32 PG clean、7 PG 恢复中；单副本 owner1 已迁到 osd.4 |
| 03:37:46 | 39 PG 全部 clean，降级/错放/找不到的对象均为 0 |
| 03:40:08 | 迁移后 8 个池再次原生导出完成 |
| 03:41:21 | 8 个池全部文件大小及 SHA-256 与迁移前一致 |
| 03:41:24 | 全部门槛通过后请求停止 osd.1 |
| 03:44:30 | 确认服务 inactive、开机自启 disabled、MainPID=0、无运行容器 |
| 03:47:21 | 完成服务、数据、放置及安全检查的最终验收 |

迁移使用标准 `out` 路径收敛，未使用 CRUSH reweight 回退，也未调整恢复限速或
健康告警配置。停用只针对 osd.1，n118 的 osd.0 和 mon.n118 保持 active。

停止时，日志记录收到 SIGTERM、BlueStore 卸载约 26.6 s，以及容器退出码 137。
服务停止后留下 systemd failed 状态；在确认 MainPID=0、无运行容器且服务已 disabled
后，仅清除了该单元的终止失败状态，未重启服务、清 SMART 或屏蔽健康检查。
Cephadm 缓存刷新后确认状态为 stopped，临时 failed-daemon 提示消退。原始停止
日志与状态均已保存；退出码本身不足以断言发生了 OOM 或某种具体终止原因。

## 3. 数据验证

迁移前后均导出 `.mgr`、`rbd` 和六个 `nsvu_*` 池的全部当前 HEAD 对象。原生格式
包含对象数据、namespace/locator、xattrs、OMAP header 和条目。8 个导出文件逐个
SHA-256 完全一致，连同导出内容规模一起验证：

| 数据范围 | 对象数 | payload bytes |
|---|---:|---:|
| rbd | 1000 | 4,194,304,000 |
| .mgr | 3 | 8,389,152 |
| 六个实验池 | 6 | 0 |
| 合计 | 1009 | 4,202,693,152 |

RBD 前后原生文件 SHA-256 均为
`6241e24261f00343bc151642b78a91555e1ca7398f17f1a5157332df65aa427b`。
离线格式解析验证了备份的完整终结、对象身份和数据规模；指纹工具另通过 10 项
截断、结构破坏、对象重排及 payload 修改检查。当前 PG clone 数为 0，因此
HEAD 导出覆盖本次实际对象内容；没有将其称为通用的快照克隆备份方案。

停止前后，PG 统计均验证 1009 个逻辑对象、3015 个活动对象副本，missing、unfound、
degraded、misplaced、scrub error 均为 0。`ok-to-stop` 返回 true，
`safe-to-destroy` 明确包含 osd.1。没有对各物理副本逐扇区读取，也没有执行恢复演练。

## 4. 迁移后的实验布局与剩余问题

| 实验池 | 迁移前 acting OSD | 迁移后 acting OSD |
|---|---:|---:|
| nsvu_meta | 0 | 0 |
| nsvu_owner_0 | 0 | 0 |
| nsvu_owner_1 | 1 | 4 |
| nsvu_owner_2 | 2 | 4 |
| nsvu_owner_3 | 3 | 3 |
| nsvu_owner_4 | 4 | 4 |

owner2 的变化来自原 `1→2` upmap 在移出 osd.1 后不再适用于新的 CRUSH 基础放置。
保留原 upmap 记录，没有为了维持旧布局将 PG 强行指回故障 OSD。所有池的 ID、
size、min_size、PG 数和 CRUSH rule 均未改变。

目前运行的是四个 OSD，仍覆盖四台主机；原始在用 OSD 容量约 892 GiB。
原“五 owner 分别对应五 OSD”的性能口径已不成立，旧实验继续暂停。应在换盘恢复
布局并重新认证后继续，或另行设计四 OSD 对照，不能混合两种布局的实验数字。

最终健康状态仍为 `HEALTH_ERR`，原因保留为既有 `devicehealth` 模块错误和六个
实验池单副本告警。没有新增 PG/数据降级或失败 daemon 告警。SSD 本体仍待维修或
更换；本次完成的是数据迁移与停止使用该设备。如果换盘需要关闭整个 n118，须
另外评估 osd.0 和 MON 的影响，不能直接套用单盘停用的检查结果。

完整工件：`results/osd1-evacuation-20261119T033032Z/`。入口为 `completion.json`、
`data-verification.json`、`service-deactivation-proof.json` 和 `actions.jsonl`；
`pool-backups/` 与 `pool-after/` 保存前后备份，`native-fingerprints-before/`
保存原生格式检查结果。原始数据、备份和临时工具不提交 Git，全部索引 cache 保留。
