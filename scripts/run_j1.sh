#!/usr/bin/env bash
# 用途：把 J1 的 Flink SQL 提交到本地集群；密码用占位符替换，不写进仓库
# 对应文档：docs/04-架构设计.md 4.2
# 执行：bash scripts/run_j1.sh <CDC密码>
# 依赖：Flink 已启动（scripts/start_flink.sh），Kafka 5 个 topic 已建
set -euo pipefail

cd /mnt/d/projects/ecommerce-realtime-dw

if [ $# -lt 1 ]; then
  echo "用法: $0 <CDC密码>" >&2
  exit 1
fi

export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk-amd64}
export HADOOP_HOME=${HADOOP_HOME:-/home/hzh/apps/hadoop}
export HADOOP_CLASSPATH=$($HADOOP_HOME/bin/hadoop classpath)
FLINK_HOME=${FLINK_HOME:-/home/hzh/apps/flink}

# 临时文件承载带密码的 SQL，提交后立即删除
TMP_SQL=$(mktemp /tmp/j1_cdc2kafka.XXXXXX.sql)
trap 'rm -f "$TMP_SQL"' EXIT

sed "s/__CDC_PASSWORD__/$1/g" flink/sql/j1_cdc2kafka.sql > "$TMP_SQL"

"$FLINK_HOME/bin/sql-client.sh" -f "$TMP_SQL"
