#!/usr/bin/env bash
# 是什么：启动 DolphinScheduler standalone（单进程，元数据在 MySQL 的 dolphinscheduler 库）
# 干什么：把 MYSQL_PASSWORD 转成 DS_DB_PASSWORD 传给 DS 进程，等 12345 端口就绪
# 对应文档：docs/04-架构设计.md 4.3、docs/07-操作步骤.md 阶段 9
# 执行：MYSQL_PASSWORD=xxx bash scripts/start_ds.sh
# 依赖：MySQL 3306（dolphinscheduler 库已初始化 3.4.3）、Java 11
# 说明：口令只走环境变量。standalone 的 application.yaml 里写的是 ${DS_DB_PASSWORD:}，
#       不落盘；进程退出后 DS 就被回收，常驻要用 ds_keepalive.sh（同 Flink 的 WSL 限制）
set -euo pipefail

DS_HOME=${DS_HOME:-/home/hzh/apps/dolphinscheduler}
JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-11-openjdk-amd64}
export JAVA_HOME

[ -x "$DS_HOME/bin/dolphinscheduler-daemon.sh" ] || { echo "找不到 DS：$DS_HOME" >&2; exit 1; }
if [ -z "${DS_DB_PASSWORD:-}" ]; then
  [ -n "${MYSQL_PASSWORD:-}" ] || { echo "缺少 MySQL 口令：export MYSQL_PASSWORD=..." >&2; exit 1; }
  export DS_DB_PASSWORD="$MYSQL_PASSWORD"
fi

"$DS_HOME/bin/dolphinscheduler-daemon.sh" stop standalone-server >/dev/null 2>&1 || true
"$DS_HOME/bin/dolphinscheduler-daemon.sh" start standalone-server

echo "等待 12345 就绪（最多 180 秒）…"
for i in $(seq 1 90); do
  if curl -fsS -o /dev/null "http://127.0.0.1:12345/dolphinscheduler/ui/" 2>/dev/null \
     || curl -fsS -o /dev/null "http://127.0.0.1:12345/dolphinscheduler/" 2>/dev/null; then
    echo "DS 已就绪：http://127.0.0.1:12345/dolphinscheduler/ui/（admin / dolphinscheduler123）"
    exit 0
  fi
  sleep 2
done
echo "180 秒内没起来，看日志：$DS_HOME/standalone-server/logs/" >&2
exit 1
