#!/usr/bin/env bash
# 是什么：离线 T+1 跑批入口，把 MySQL 当天数据抽到 Hive 并算出 DWD/DWS/ADS
# 干什么：设置 Spark / Java / Hadoop 环境，调用 spark/offline_t1.py
# 对应文档：docs/04-架构设计.md 4.3、docs/06-数据质量与对账.md 六
# 执行：bash scripts/run_offline_t1.sh [--dt 2026-11-25]   缺省 dt = 昨天
# 依赖：MYSQL_PASSWORD 环境变量（或把 --password 透传下去）；mysql-connector jar 见 MYSQL_JAR
# 幂等：每个 dt 分区 INSERT OVERWRITE，可重复跑
set -euo pipefail

cd /mnt/d/projects/ecommerce-realtime-dw

export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-11-openjdk-amd64}
export HADOOP_HOME=${HADOOP_HOME:-/home/hzh/apps/hadoop}
export HADOOP_CONF_DIR=${HADOOP_CONF_DIR:-$HADOOP_HOME/etc/hadoop}
SPARK_HOME=${SPARK_HOME:-/home/hzh/apps/spark}
MYSQL_JAR=${MYSQL_JAR:-/home/hzh/apps/apache-hive-3.1.3-bin/lib/mysql-connector-j-8.0.33.jar}

[ -x "$SPARK_HOME/bin/spark-submit" ] || { echo "找不到 spark-submit：$SPARK_HOME" >&2; exit 1; }
[ -f "$MYSQL_JAR" ] || { echo "找不到 MySQL JDBC 驱动：$MYSQL_JAR" >&2; exit 1; }
if [ -z "${MYSQL_PASSWORD:-}" ] && [[ " $* " != *" --password "* ]]; then
  echo "缺少 MySQL 口令：export MYSQL_PASSWORD=... 或传 --password" >&2
  exit 1
fi

exec "$SPARK_HOME/bin/spark-submit" \
  --master "local[${SPARK_CORES:-2}]" \
  --jars "$MYSQL_JAR" \
  --conf spark.driver.extraJavaOptions=-Duser.timezone=Asia/Shanghai \
  --conf spark.executor.extraJavaOptions=-Duser.timezone=Asia/Shanghai \
  --conf spark.sql.session.timeZone=Asia/Shanghai \
  --conf spark.sql.shuffle.partitions=4 \
  --conf spark.ui.showConsoleProgress=false \
  spark/offline_t1.py "$@"
