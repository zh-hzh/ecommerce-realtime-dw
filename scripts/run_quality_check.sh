#!/usr/bin/env bash
# 是什么：批巡检 C01~C05 的入口，把 spark/quality_check.py 提交到本地 Spark
# 干什么：设置 Spark / Java / Hadoop 环境；结果写回 Doris dw.dwd_dirty_record
# 对应文档：docs/06-数据质量与对账.md 四节
# 执行：bash scripts/run_quality_check.sh [--dt 2026-11-25]   缺省 dt = 昨天
# 依赖：Hive metastore 9083、Doris 9030、pymysql；不需要 MySQL 口令
# 退出码：0 全通过；1 任一规则 FAIL（C04 失败会提前退出），DS 用它阻断下游
set -euo pipefail

cd /mnt/d/projects/ecommerce-realtime-dw

export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-11-openjdk-amd64}
export HADOOP_HOME=${HADOOP_HOME:-/home/hzh/apps/hadoop}
export HADOOP_CONF_DIR=${HADOOP_CONF_DIR:-$HADOOP_HOME/etc/hadoop}
SPARK_HOME=${SPARK_HOME:-/home/hzh/apps/spark}

[ -x "$SPARK_HOME/bin/spark-submit" ] || { echo "找不到 spark-submit：$SPARK_HOME" >&2; exit 1; }

exec "$SPARK_HOME/bin/spark-submit" \
  --master "local[${SPARK_CORES:-2}]" \
  --conf spark.driver.extraJavaOptions=-Duser.timezone=Asia/Shanghai \
  --conf spark.executor.extraJavaOptions=-Duser.timezone=Asia/Shanghai \
  --conf spark.sql.session.timeZone=Asia/Shanghai \
  --conf spark.sql.shuffle.partitions=4 \
  --conf spark.ui.showConsoleProgress=false \
  spark/quality_check.py "$@"
