#!/usr/bin/env bash
# 用途：把 J4（埋点流 → dwd_traffic_detail + dwd_dirty_record）的 Flink SQL 提交到本地集群
# 对应文档：docs/04-架构设计.md 4.2 J4
# 执行：bash scripts/run_j4.sh
# 依赖：Flink 已启动；Doris 已建两张表（bash scripts/init_doris.sh）；
#       Kafka topic ods_traffic 存在（bash scripts/init_kafka_topics.sh）
# 说明：本作业不连 MySQL、不需要密码；埋点数据由 scripts/run_gen_traffic.sh 写入。
#       重新提交前先停掉旧版 J4，并换掉 sink label 前缀（Doris label 全局唯一）
set -euo pipefail

cd /mnt/d/projects/ecommerce-realtime-dw

export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk-amd64}
export HADOOP_HOME=${HADOOP_HOME:-/home/hzh/apps/hadoop}
export HADOOP_CLASSPATH=$($HADOOP_HOME/bin/hadoop classpath)
FLINK_HOME=${FLINK_HOME:-/home/hzh/apps/flink}

"$FLINK_HOME/bin/sql-client.sh" -f flink/sql/j4_dwd_traffic.sql
