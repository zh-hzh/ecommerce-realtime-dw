#!/usr/bin/env bash
# 用途：把 J3（交易事件 → Doris 三张 DWS 聚合表）的 Flink SQL 提交到本地集群
# 对应文档：docs/04-架构设计.md 4.2 J3
# 执行：bash scripts/run_j3.sh
# 依赖：Flink 已启动、J1/J2 在跑（Kafka 有交易事件、DWD 有订单行）、
#       Doris 已建三张 DWS 表与 dim_product（bash scripts/init_doris.sh），
#       商品维表已装载（bash scripts/load_dim_product.sh <mysql_root_password>）
# 说明：本作业不连 MySQL，Doris root 免密，所以不需要密码参数；
#       重新提交前先停掉旧版 J3，并换掉 sink label 前缀（Doris label 全局唯一）
set -euo pipefail

cd /mnt/d/projects/ecommerce-realtime-dw

export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk-amd64}
export HADOOP_HOME=${HADOOP_HOME:-/home/hzh/apps/hadoop}
export HADOOP_CLASSPATH=$($HADOOP_HOME/bin/hadoop classpath)
FLINK_HOME=${FLINK_HOME:-/home/hzh/apps/flink}

"$FLINK_HOME/bin/sql-client.sh" -f flink/sql/j3_dws_trade.sql
