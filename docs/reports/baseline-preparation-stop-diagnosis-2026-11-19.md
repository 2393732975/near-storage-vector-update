# 高召回基线准备任务自动停止诊断（2026-11-19）

本次停止的直接原因已经确认：重置实验池期间，osd.1 新增 BlueStore 慢操作告警，
超出了恢复流程仅允许历史 osd.2 告警的范围，脚本按保护规则停止。实际日志显示
同期有大量 OMAP 删除、RocksDB flush/compaction 和 KV 持久化长尾；进一步只读
检查确认 osd.1 对应 SSD 当前 SMART 总体健康检查失败。具体硬件异常机制，以及
硬件异常对本次延迟的贡献程度，尚未完全定位。

## 1. 停止发生在哪里

运行目录为 `results/high-recall-preparation-resume-20261118T075859Z/`。
所有时间均为 UTC。

| 时间（2026-11-18） | 可复核事件 |
|---|---|
| 07:58:59 | 启动恢复任务，核验并复用已完成索引和 GIST 结果 |
| 08:04:08.937 | 开始重置六个实验池 |
| 08:04:32–08:05:07 | osd.1 记录多组 KV 提交慢操作 |
| 08:05:14.766 | 重置命令完成，退出码为 0 |
| 08:05:15.872 | 重置后的健康检查发现告警扩展至 osd.1，任务停止 |

[事件保护逻辑](../../scripts/baseline_cluster_guard.py) 从健康详情提取受影响 OSD，
仅接受已诊断的 `{2}`；新增 osd.1 后抛出
`slow-operation warning expanded beyond diagnosed osd.2`。
[准备任务](../../scripts/run-high-recall-preparation.py) 的 `reset()` 在重置后立即
执行健康检查；`import_case()` 只有在它返回后才启动 importer。

因此，这次只完成了池重置，**没有启动实际 Deep 导入**，也没有新增 Deep import、
checker 或正式 Compute/OSD 测量结果。四份本地图 cache 和 GIST 12 次完整测试
仍然保留。原实验池内不完整的 Deep 数据已随这次重置删除，原本地日志仍在。

## 2. 慢操作的实测证据

远端 n118 的 osd.1 日志共找到 34 条慢操作记录，全部集中在上述重置窗口。
它们是不同计时点的日志，不能计为 34 个互相独立的慢 I/O。

| 日志计时点 | 条数 | 实测耗时 |
|---|---:|---|
| `kv_commit` | 4 | 6.249、9.211、10.654、10.542 s |
| `kv_sync` | 4 | 与上述四次基本相同 |
| `_txc_committed_kv` | 26 | 5.242–12.296 s |

使用 `ceph tell osd.1 config get` 核验的运行时参数为：
`bluestore_log_op_age=5 s`、`bluestore_slow_ops_warn_threshold=1`、
`bluestore_slow_ops_warn_lifetime=86400 s`。这些日志超过实际阈值，告警不是
仅由旧 osd.2 提示残留导致。`_txc_committed_kv` 表示从事务开始到 KV 持久提交
完成的累计时长，不能当作纯磁盘、纯 RocksDB 或网络耗时。

08:03–08:08 的日志还包含 6 次 flush 和 11 次 compaction 的开始/结束事件。
一个 OMAP memtable 的 4,480,028 条记录全部是删除记录；另两批分别含
4,160,026 和 5,280,033 条删除记录。`num_deletes` 是数据库删除记录数，不是
向量数。`p-0/p-1/p-2` 是 OMAP 列族分片名称，不是实验池编号。

08:04:43 和 08:05:07，RocksDB 两次记录 WAL 总量超过 1 GiB 上限并触发 flush。
这与清除旧池产生大量元数据删除的路径一致；后台整理持续到约 08:06:23，晚于
脚本停止时间。池删除命令返回、PG 为 `active+clean`，都不能证明底层回收已经静默。

本机 Ceph 源码中，`PG::do_delete_work()` 按批删除对象并重新排队；BlueStore
对象删除会清理 OMAP。这些实现帮助解释日志，但本机源码没有 Git 版本身份，
不能据此声称与运行中的 17.2.8 二进制逐字一致。

## 3. 对应 SSD 存在独立的健康异常

通过 Ceph 设备清单、OSD block 链接和 `lsblk` 三处核对，osd.1 的数据设备为
n118 的 `/dev/sdd`，型号 `SAMSUNG MZ7LH480HAHQ-00005`，序列号
`S45PNC1RC02813`。未发现独立的 `block.db` 或 `block.wal` 链接。

2026-11-19 只读读取 SMART，文本与 JSON 两种输出一致：

| 指标 | osd.1：`/dev/sdd` | 同机同型号对照：osd.0 `/dev/sdc` |
|---|---|---|
| SMART 总体健康 | `FAILED` / `passed=false` | `passed=true` |
| 属性 202 `Exception_Mode_Status` | value=1，threshold=10，`when_failed=now`，raw=16 | value=100，raw=0，未越阈值 |
| smartctl 退出码 | 24 | 0 |

这是设备报告的实际健康失败，应优先排查或更换异常 SSD。属性 202 是厂商属性；
现有证据不能把 raw=16 擅自解释为坏块、供电保护故障或某种确定的固件问题。
本次重分配扇区、不可纠正错误及 CRC 计数均为 0，也不能用这些为 0 的计数抵消
总体健康失败。首次只读取证未执行 SMART 自检任务、写入测试、设备重置或维修操作；
后续按用户修复请求完成的短自检见第 5 节。

SMART 是诊断当时读取的状态，没有故障发生瞬间的 SMART 快照。因此可以确认
“对应设备当前异常”，尚不能证明它独自造成了 11 月 18 日的全部长延迟。
历史温度记录在故障附近约 28–30°C；读取的内核日志窗口没有匹配到 I/O 错误，
也不足以排除设备内部延迟、共享 I/O 竞争或调度影响。

## 4. 结论与后续边界

有证据支持的链条是：**重置旧池 → 大量 OMAP 删除和数据库后台整理 → 同期 KV
提交出现秒级长尾 → 新增 osd.1 慢告警 → 实验保护停止**。SSD 健康失败是进一步
确认的重要设备异常，硬件层的具体故障机制仍需运维或厂商诊断。

告警保留一天，解释了后续空闲时仍可见慢操作提示，不表示磁盘持续卡住；截至本次
查询，旧 osd.2 告警已经消退，osd.1 告警仍在。5/5 OSD 在线、39/39 PG clean
只说明服务/PG 状态，不能代替设备健康和写入延迟资格。

本次诊断保持任务停止。下一步先处理失败健康的 SSD，再验证清理后的后台静默、
受控写入延迟及无新增慢事件，之后才恢复准备任务。不能把此次池清理异常当作
高召回 HNSW 算法、CLS 卸载或正式更新性能的新瓶颈结论。

原始证据位于 `results/slow-stop-diagnostic-20261119T025528Z/`：
`diagnosis.json` 保存计数与证据 SHA-256，`manifest.json` 保存只读命令和退出码；
`osd1-slow-history.out`、`osd1-stop-window.out`、`smart-sdd-json.out`、
`smart-sdc-control-json.out` 及设备映射文件支持上述结论。原始结果不提交 Git。
远端读取使用已有 Cephadm 管理身份和已有主机密钥校验；临时身份文件已删除，
未改变认证、Ceph 阈值、健康检查、OSD 服务或池数据。

## 5. 软件侧排查与授权短自检

用户随后要求判断软硬件故障并修复。2026-11-19 03:11–03:13 UTC 对原序列号
复核后，执行了以下操作，证据目录为
`results/ssd-repair-qualification-20261119T031143Z/`：

1. `smartctl -H -P ignore -j /dev/sdd` 关闭型号数据库预设，仍返回
   `smart_status.passed=false`，没有协议读取失败或回退判断警告。这排除了仅因
   数据库属性名称解释而产生健康失败的说法。
2. 执行一次后台、非 captive 的 `smartctl -t short -j /dev/sdd`。该诊断不覆盖
   用户数据；[smartctl 官方手册](https://github.com/smartmontools/smartmontools/blob/main/src/smartctl.8.in)
   允许在正常系统运行时进行这种测试，但它可能影响运行期间的性能。
3. 自检最终返回 `Completed: unknown failure`，记录 `passed=false`；设备状态
   同时显示 `completed with error (unknown test element)`，剩余 90% 是失败时
   未完成比例，不代表测试仍在运行。不能把日志中的 LBA=0 解读为已定位坏扇区 0。
4. 自检后总体健康仍失败，属性 202 仍为 value=1、threshold=10、raw=16。
   设备没有修复；本次自检也不能证明是某个确定的电容、NAND 或控制器部件损坏。

现在可确定问题在 SSD 设备内部，应按设备故障处理；现有证据尚不能将物理硬件
损坏与盘内固件状态异常完全分开。尚未找到与本盘型号 `MZ7LH480HAHQ-00005`、
固件 `HXT7904Q` 及此故障表现明确匹配、可核验适配的官方无损软件修复方案。

[Samsung DC Toolkit 2.1 官方指南](https://download.semiconductor.samsung.com/resources/user-manual/Samsung_DCToolkit_V2.1_User_Guide.pdf)
第 8 页列出 PM/SM883 诊断支持，第 17 页将 SMART 列为支持、Firmware Update
列为 N/A。因此不能认为“下载任意新版工具就能修复”，也不能将同系列或其他 OEM
镜像直接刷入此盘。[Dell HE59 发布页](https://www.dell.com/support/home/en-us/drivers/driversdetails?driverid=93P7P)
列的是 `...0D3` 型号，不是本盘的 `...-00005`，不能作为适配升级依据。

## 6. 已完成的数据保护与待执行维修

迁移前的停用检查 `ceph osd ok-to-stop 1` 返回 `ok_to_stop=false`：单副本
`nsvu_owner_1` 的 PG `1673.0` 会离线；osd.1 另外承载 17 个 RBD PG，停止时
这些 PG 会降级。`safe-to-destroy` 结果也没有将 osd.1 列为可销毁。

已经将 `nsvu_owner_1` 导出到本地维修目录。当前该池只有一个空初始化对象，
stored=0；导出文件 475 bytes，SHA-256 为
`136a3083b12fc59fd5b4014898a847006416484bd114e3302a080739105d85ef`。
这只是该实验池的备份，不是 RBD 数据全量备份，也没有验证在替代池上的恢复。
没有更改池布局、OSD 成员关系或 RBD 数据，没有停止或重启服务。

后续需要原供应商提供适配的固件/故障诊断结论，或安排现场更换对应 SSD。
在拔盘、刷固件或销毁 OSD 之前，先处理单副本 PG 的迁移/恢复安排，并等待
相关 RBD 数据恢复到满足维护条件，再复核停止和销毁检查。换盘后还需恢复
owner/OSD 实验映射、检查布局和小规模存储资格，才能继续正式准备任务。

本次完成了软件侧复核、短自检和限定实验池备份，**没有声称修复 SSD**。
具体操作与原始结果可由 `repair-assessment.json`、`manifest.json`、
`smart-without-presets.out`、`smart-after-short-*.out` 及 `owner1-backup.json`
复核。

用户后续明确授权迁移并停用；2026-11-19 03:47:21 UTC 已完成数据迁移、
前后内容校验及 osd.1 持久停用，停盘阻塞已解除。设备本体仍待维修或更换。
当前四 OSD 状态及完整验收见
[数据迁移与停用记录](osd1-data-evacuation-report-2026-11-19.md)。
