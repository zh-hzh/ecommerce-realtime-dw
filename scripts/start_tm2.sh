#!/usr/bin/env bash
# 是什么：再起一个 TaskManager（4 槽位），给 J3/J5 这类"槽位不够"的作业腾位置
# 干什么：复制一份 conf（只改 numberOfTaskSlots=4，内存沿用默认 1728m）后启动 TM；
#         加 --keepalive 时占住会话（WSL 会在 wsl.exe 退出时回收非 systemd 进程）
# 对应文档：docs/07-操作步骤.md 阶段 4、docs/08-问题索引.md D25/E38
# 执行：bash scripts/start_tm2.sh [--keepalive]
set -euo pipefail
FLINK_HOME=${FLINK_HOME:-/home/hzh/apps/flink}
CONF=/tmp/flink-tm2-conf
export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk-amd64}
export HADOOP_HOME=${HADOOP_HOME:-/home/hzh/apps/hadoop}
export HADOOP_CLASSPATH=$($HADOOP_HOME/bin/hadoop classpath)

rm -rf "$CONF"; cp -r "$FLINK_HOME/conf" "$CONF"
sed -i 's/^  numberOfTaskSlots: .*/  numberOfTaskSlots: 4/' "$CONF/config.yaml"
echo "TM2 槽位：$(grep -E '^  numberOfTaskSlots' "$CONF/config.yaml")"

FLINK_CONF_DIR="$CONF" "$FLINK_HOME/bin/taskmanager.sh" start
sleep 12
curl -s localhost:8081/taskmanagers | python3 -c "
import sys,json
d=json.load(sys.stdin)
print('TM:', [(t['id'][:13], t['slotsNumber'], t['freeSlots']) for t in d['taskmanagers']])
"

if [ "${1:-}" = "--keepalive" ]; then
  echo "TM2 会话保持中"
  exec sleep infinity
fi
echo "注意：不加 --keepalive 时，本命令退出后 TM2 会被 WSL 回收"
