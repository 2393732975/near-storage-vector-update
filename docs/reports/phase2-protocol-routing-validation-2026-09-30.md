# 阶段 2 协议与路由抽象验收报告（2026-09-30）

## 结论

阶段 2 已通过真实 Ceph 集群验收。保持原 modulo 放置语义的
`PlacementResolver`、CLS schema v1、共同 query 前缀 Recall 门禁和新增聚合观测
字段均正常工作。GIST1M smoke 的两条路径均为 0 更新失败，离线索引检查均为
`pass`、0 error、0 warning。重构后的 OSD 路径相对阶段 1 B0 没有性能退化；可以
进入阶段 3 的有界 frontier 聚合。

## 部署与运行环境

- smoke Git commit：`05e120224fb430a7555f7c4304d5218e1b6842db`
- 重放探针 commit：`bdfffb8`（探针在 smoke 完成后加入，不改变被测 CLS）
- Coordinator SHA-256：`13d0492cfb75c4222d7553d5e764e3b53b967ac076f8c7879a859e7443bcc6ce`
- Importer SHA-256：`53aff15742c953c9404d1c19b5562dd3562dfe1d3f4520dfcc4d6e05ceda1eb2`
- Checker SHA-256：`e16394c99f85a8578727e9da813ab294d7d1fda0742e93a867bbd4e885a22760`
- 新 CLS SHA-256：`a615bc434ba0ada2cb0808066c56d5299b66582aa1bf6c43ad72a94ae2b78e55`
- 旧 CLS SHA-256：`64157add9fe0ed7fbd4baba3fdd6e5729f303fbd9b54abaf4c0450f174297bc5`
- Ceph FSID：`c83da116-1e8b-11f1-933d-cd48aa44d216`
- 客户端 Ceph 17.2.9、集群 daemon Ceph 17.2.8 Quincy；5 个 OSD，单副本实验池，
  一池一 OSD 固定落点
- GIST1M；300 s；并发度 4；每模式独立重建池和重新导入
- 原始工件（本机、不提交）：`results/phase2-gist-smoke-20260930T081551Z/`

新库先原子替换到每个 OSD 的持久化 `rados-classes` 目录，旧文件以
`.pre-stage2-20260930` 后缀保留。随后按 osd.2、osd.3、osd.0、osd.1、osd.4
逐个重启；每次都等待 5/5 OSD `up/in` 且全部 PG `active+clean` 后才继续。重启后
容器内五份库的哈希均为新 CLS 哈希。

## GIST1M 严格 smoke

| 模式 | 成功更新 | 失败 | 平均延迟 | P99 | 吞吐 | 静态 Recall@10 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| compute/raw RADOS | 2,421 | 0 | 446.359 ms | 788.738 ms | 8.955/s | 0.3710 |
| OSD/CLS | 3,197 | 0 | 337.920 ms | 614.397 ms | 11.831/s | 0.3661 |

compute 的 CLS 调用严格为 0、Raw RADOS 调用为 2,310,404；OSD 的 Raw RADOS
调用为 0、CLS 调用为 1,595,022。两条路径的结构和语义检查均通过，语义边胜率
分别为 0.994460 和 0.997140。

固定时间窗处理的更新数不同，因此正式质量门禁使用共同的前 1,000 个 query：
compute Recall@10 为 0.3691，OSD 为 0.3661，差值 -0.0030，小于最大允许退化
0.005，门禁通过。

## 性能退出条件

阶段 1 正式 B0 的 GIST1M OSD 三轮均值为 345.387 ms、11.601 updates/s。本轮为
337.920 ms、11.831 updates/s，即延迟变化 -2.16%、吞吐变化 +1.98%。compute
相对 C0 三轮均值的延迟变化约 +1.17%、吞吐变化约 -1.17%。两条路径的变化绝对值
均小于 5%，且 B0 方向略有改善。单次 smoke 不能替代三次重复置信区间，但足以作为
无明显重构回归的阶段退出门禁。

## 新指标闭合

| 指标 | compute | OSD |
| --- | ---: | ---: |
| schema version | 1 | 1 |
| distance batches | 1,033,424 | 1,331,865 |
| 直方图桶次数之和 | 1,033,424 | 1,331,865 |
| 实际评分候选数 | 1,145,025 | 1,479,628 |
| 直方图 `size × count` | 1,145,025 | 1,479,628 |
| 平均唯一距离对象/update | 64.678 | 64.618 |
| 候选/唯一距离对象 | 7.312 | 7.162 |
| candidates/batch | 1.108 | 1.111 |
| 去重保留率 | 0.8018 | 0.7803 |
| frontier window fill | 1.000 | 1.000 |

批次直方图同时在“批次数”和“候选数”两个维度闭合；去重后候选数小于去重前。
`frontier_window_fill=1` 是阶段 2 保持旧串行搜索语义的预期结果，也为阶段 3 的
W=4/8/16/32 聚合建立了直接基线。平均每次更新虽只访问约 65 个唯一距离对象，当前
仍产生约 417 个距离批次，说明阶段 3 的对象内合并空间确实存在。

## 幂等重放

`nsvu-cls-replay-probe` 模拟服务端已执行但客户端丢失响应：同一个编码后的
reserve 请求原样执行两次，再对 finalize 做相同操作。最终一次验证中：

- reserve 前 `next_global_id=1003199`，重放后为 1003200，只增加 1；
- finalize 前 `cur_element_count=1003198`，重放后为 1003199，只增加 1；
- 两个 `*_replay_advanced_meta` 字段均为 `false`；schema version 为 1。

探针会消耗测试元数据 ID，必须显式传入 `--confirm-mutation`，且只能在即将清理的
实验池使用。第一次开发态探针误把写 CLS 的 output 当作可靠回复进行解码，调用已
成功但客户端返回 `EINVAL`；该次只留下一个未 finalize reservation。正式探针改为
与 Coordinator 相同的“读持久 marker 恢复结果”语义后通过。两次探针都发生在
smoke 和 checker 完成之后，不影响性能、Recall 或一致性结果；实验池不再复用。

## 集群收尾

报告和原始工件确认落盘后，已使用带 FSID 保护的固定白名单清理脚本删除
`nsvu_meta`、`nsvu_owner_0`–`4`。删除结束时 pool/PG 数短暂处于过渡状态；最终
复核只剩 `.mgr`、`rbd`，5/5 OSD `up/in`，33/33 PG `active+clean`，没有
`nsvu_*` 残留。清理脚本返回码 3 仅表示集群仍有既有 `devicehealth` 模块磁盘 I/O
错误，不是池删除失败；本次收尾未屏蔽告警、强制压缩或额外重启 OSD。BlueStore
物理空间由后台异步回收。
