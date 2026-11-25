#!/usr/bin/env bash
# 用途：带 Hadoop classpath 启动 Flink 单机集群（1 JM + 1 TM）
# 背景：不设 HADOOP_CLASSPATH 时，JobManager 初始化会报
#       ClassNotFoundException: AbstractDelegationTokenIdentifier，进程直接退出
# 执行：bash scripts/start_flink.sh
set -euo pipefail

export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk-amd64}
export HADOOP_HOME=${HADOOP_HOME:-/home/hzh/apps/hadoop}
export HADOOP_CONF_DIR=${HADOOP_CONF_DIR:-$HADOOP_HOME/etc/hadoop}
export HADOOP_CLASSPATH=$($HADOOP_HOME/bin/hadoop classpath)
FLINK_HOME=${FLINK_HOME:-/home/hzh/apps/flink}

"$FLINK_HOME/bin/stop-cluster.sh" >/dev/null 2>&1 || true
sleep 2
"$FLINK_HOME/bin/start-cluster.sh"
sleep 10

echo "--- 进程 ---"
jps | grep -Ei "StandaloneSession|TaskManagerRunner" || echo "未找到 Flink 进程"
echo "--- REST ---"
curl -s --max-time 5 http://127.0.0.1:8081/overview || echo "8081 无响应"
