#!/usr/bin/env bash
# 用途：把 J5（ods_traffic → dws_traffic_1min + dws_user_1d）的 Flink SQL 提交到本地集群
# 对应文档：docs/04-架构设计.md 4.2 J5
# 执行：bash scripts/run_j5.sh
# 依赖：Flink 已启动且至少有 3 个空闲槽位（J4 已降到并行度 3）；
#       Doris 已建两张流量 DWS 表（bash scripts/init_doris.sh）；
#       Kafka topic ods_traffic 存在
# 说明：不连 MySQL、不需要密码。重新提交前先停掉旧版 J5，并换掉 sink label 前缀。
set -euo pipefail

cd /mnt/d/projects/ecommerce-realtime-dw

export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk-amd64}
export HADOOP_HOME=${HADOOP_HOME:-/home/hzh/apps/hadoop}
export HADOOP_CLASSPATH=$($HADOOP_HOME/bin/hadoop classpath)
FLINK_HOME=${FLINK_HOME:-/home/hzh/apps/flink}

"$FLINK_HOME/bin/sql-client.sh" -f flink/sql/j5_dws_traffic.sql
