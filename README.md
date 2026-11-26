# 电商交易实时数仓

围绕电商交易域（下单 → 支付 → 退款），用 **Kafka + Flink SQL + Doris** 搭一条秒级实时链路，用 **Spark + Hive** 保留 T+1 离线链路，两条链路计算同一套指标并互相校对，输出差异率与数据质量记录。

## 链路

```text
MySQL binlog ──J1──► Kafka(ods_*) ──J2──► Doris dwd_trade_order_detail
                           │                        │
                           └──────J3───────────────┴──► DWS 3 张
埋点 ──► Kafka(ods_traffic) ──J4/J5──► 埋点 DWD / 流量 DWS
                                          │
                                    J6（Doris SQL）──► ADS 3 张

MySQL ──Spark JDBC──► Hive ods→dwd→dws→ads ──┐
                                             ├─► 对账（5 个交易指标）──► dw.ads_reconcile_diff
Doris 实时 DWD/DWS ──────────────────────────┘
```

## 快速开始

完整步骤、预期输出、排障命令见 [docs/07-操作步骤.md](docs/07-操作步骤.md)。

```bash
# 0. 一次性：建 MySQL 业务库、Kafka topic、Doris 表、Hive 表，起 Flink
sed 's/__CDC_PASSWORD__/你的CDC密码/g' sql/01_init_trade.sql | mysql -uroot -p
bash scripts/start_flink.sh
bash scripts/init_kafka_topics.sh && bash scripts/init_doris.sh
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
/home/hzh/apps/spark/bin/spark-sql -f sql/09_init_hive.sql

# 1. 造数 + 装载维表
bash scripts/run_gen_master.sh 'MySQL口令'
bash scripts/run_gen_history.sh 'MySQL口令'
bash scripts/load_dim_shop.sh 'MySQL口令' && bash scripts/load_dim_product.sh 'MySQL口令'

# 2. 启动实时链路（J1 起 4 个作业，J2~J5 各 1 个）
bash scripts/run_j1.sh 'CDC密码'
bash scripts/run_j2.sh && bash scripts/run_j3.sh && bash scripts/run_j4.sh && bash scripts/run_j5.sh

# 3. 造实时数据（另开终端）
MYSQL_PASSWORD='MySQL口令' bash scripts/run_gen_realtime.sh
bash scripts/run_gen_traffic.sh --qps 200

# 4. 刷新 ADS
bash scripts/run_j6.sh --loop 60

# 5. 离线 T+1 + 对账（每天，紧跟执行）
export MYSQL_PASSWORD='MySQL口令'
bash scripts/run_offline_t1.sh --dt 2026-11-25
bash scripts/run_reconcile.sh  --dt 2026-11-25
```

## 目录

| 目录 | 内容 |
|---|---|
| `generator/` | 造数：维表、30 天历史、埋点、实时交易状态机 |
| `flink/sql/` | J1~J5 实时作业（Flink SQL） |
| `spark/` | `offline_t1.py` 离线跑批、`reconcile.py` 对账 |
| `sql/` | 01 MySQL、02~08/10 Doris、09 Hive、`j6_ads_refresh.sql` |
| `scripts/` | 提交、冒烟、维表装载、环境与排障脚本 |
| `docs/` | 01~08 文档 |

## 文档

- [01-项目主页](docs/01-项目主页.md)：目标、边界、验收标准
- [02-指标体系](docs/02-指标体系.md)、[03-数据模型](docs/03-数据模型.md)
- [04-架构设计](docs/04-架构设计.md)、[05-数据源与造数](docs/05-数据源与造数.md)
- [06-数据质量与对账](docs/06-数据质量与对账.md)
- [07-操作步骤](docs/07-操作步骤.md)：按 7 个阶段从零跑起来
- [08-问题索引](docs/08-问题索引.md)：踩坑记录与决策编号

## 当前状态

- 已跑通：4 个生成器、J1~J6、Doris 14 张表、Hive 10 张表、离线 T+1、5 个交易指标对账（正反向验证均通过）
- 未完成：2000 QPS 压测、端到端延迟实测、看板、DolphinScheduler 编排、批巡检 C01~C05

> 单机环境（WSL2 + 本机，16 GB 量级），用于学习与演示，不是生产部署方案。
