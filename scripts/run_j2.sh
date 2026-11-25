#!/usr/bin/env bash
# 用途：把 J2（ods_order → Doris DWD）的 Flink SQL 提交到本地集群
# 对应文档：docs/04-架构设计.md 4.2
# 执行：bash scripts/run_j2.sh
# 依赖：Flink 已启动、J1 作业在跑（ods_order 有数据）、Doris 已建 dw.dwd_trade_order_detail
#       （建表用 bash scripts/init_doris.sh）
# 说明：本作业不连 MySQL，Doris root 免密，所以不需要密码参数
set -euo pipefail

cd /mnt/d/projects/ecommerce-realtime-dw

export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk-amd64}
export HADOOP_HOME=${HADOOP_HOME:-/home/hzh/apps/hadoop}
export HADOOP_CLASSPATH=$($HADOOP_HOME/bin/hadoop classpath)
FLINK_HOME=${FLINK_HOME:-/home/hzh/apps/flink}

"$FLINK_HOME/bin/sql-client.sh" -f flink/sql/j2_dwd_trade.sql
