#!/usr/bin/env bash
# 是什么：离线 T+1 对账入口，Hive ADS（离线） vs Doris（实时）
# 干什么：设置 Spark / Java / Hadoop 环境，调用 spark/reconcile.py，结果写 dw.ads_reconcile_diff
# 对应文档：docs/06-数据质量与对账.md 六 / 七
# 执行：bash scripts/run_reconcile.sh [--dt 2026-11-25]   缺省 dt = 昨天
# 前置：该 dt 的离线分区已产出（bash scripts/run_offline_t1.sh --dt ...）
# 幂等：同一 (stat_date, metric) 覆盖写
set -euo pipefail

cd /mnt/d/projects/ecommerce-realtime-dw

export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-11-openjdk-amd64}
export HADOOP_HOME=${HADOOP_HOME:-/home/hzh/apps/hadoop}
export HADOOP_CONF_DIR=${HADOOP_CONF_DIR:-$HADOOP_HOME/etc/hadoop}
SPARK_HOME=${SPARK_HOME:-/home/hzh/apps/spark}
MYSQL_JAR=${MYSQL_JAR:-/home/hzh/apps/apache-hive-3.1.3-bin/lib/mysql-connector-j-8.0.33.jar}

[ -x "$SPARK_HOME/bin/spark-submit" ] || { echo "找不到 spark-submit：$SPARK_HOME" >&2; exit 1; }
[ -f "$MYSQL_JAR" ] || { echo "找不到 MySQL JDBC 驱动：$MYSQL_JAR" >&2; exit 1; }

exec "$SPARK_HOME/bin/spark-submit" \
  --master "local[${SPARK_CORES:-2}]" \
  --jars "$MYSQL_JAR" \
  --conf spark.driver.extraJavaOptions=-Duser.timezone=Asia/Shanghai \
  --conf spark.executor.extraJavaOptions=-Duser.timezone=Asia/Shanghai \
  --conf spark.sql.session.timeZone=Asia/Shanghai \
  --conf spark.ui.showConsoleProgress=false \
  spark/reconcile.py "$@"
