#!/usr/bin/env bash
# 用途：启停本机 Grafana（tar 包安装，不依赖 systemd，不需要 sudo）
# 对应文档：docs/07-操作步骤.md 阶段 8
# 执行：bash scripts/grafana_start.sh start|stop|restart|status
# 依赖：/home/hzh/apps/grafana 已解压（见 docs/07 阶段 8）；Doris 9030 可连
# 说明：
#   1. 配置固定在仓库 grafana/grafana.ini，数据目录在安装目录 data/ 下，不落仓库。
#   2. 本机演示用 nohup 常驻，WSL 重启后需要重新 start；要开机自启再改成 systemd（需 sudo）。
#   3. 日志：/home/hzh/apps/grafana/log/grafana.log
set -uo pipefail

GRAFANA_HOME=${GRAFANA_HOME:-/home/hzh/apps/grafana}
REPO=/mnt/d/projects/ecommerce-realtime-dw
CONF="$REPO/grafana/grafana.ini"
PIDFILE="$GRAFANA_HOME/run/grafana.pid"
LOGFILE="$GRAFANA_HOME/log/grafana.log"
URL=http://127.0.0.1:3000/api/health

[ -x "$GRAFANA_HOME/bin/grafana" ] || { echo "未找到 $GRAFANA_HOME/bin/grafana，先按 docs/07 阶段 8 安装"; exit 1; }

running() { [ -f "$PIDFILE" ] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; }

case "${1:-status}" in
  start)
    if running; then echo "已在运行：pid=$(cat "$PIDFILE")"; exit 0; fi
    mkdir -p "$GRAFANA_HOME/run" "$GRAFANA_HOME/log"
    cd "$GRAFANA_HOME" || exit 1
    nohup ./bin/grafana server --config "$CONF" --homepath "$GRAFANA_HOME" >> "$LOGFILE" 2>&1 &
    echo $! > "$PIDFILE"
    for _ in $(seq 1 40); do
      if curl -sf --max-time 2 "$URL" >/dev/null 2>&1; then
        echo "启动成功：pid=$(cat "$PIDFILE")  $URL"
        curl -s "$URL"; echo
        exit 0
      fi
      sleep 1
    done
    echo "启动超时，看日志：$LOGFILE"
    tail -20 "$LOGFILE"
    exit 1
    ;;
  stop)
    if ! running; then echo "未在运行"; rm -f "$PIDFILE"; exit 0; fi
    pid=$(cat "$PIDFILE")
    kill "$pid" 2>/dev/null
    for _ in $(seq 1 15); do kill -0 "$pid" 2>/dev/null || break; sleep 1; done
    kill -9 "$pid" 2>/dev/null
    rm -f "$PIDFILE"
    echo "已停止"
    ;;
  restart)
    bash "$0" stop
    sleep 1
    bash "$0" start
    ;;
  status)
    if running; then
      echo "运行中：pid=$(cat "$PIDFILE")"
    else
      echo "未运行"
    fi
    curl -s --max-time 3 "$URL" || echo "（健康检查不可达）"
    echo
    ;;
  *)
    echo "用法：bash scripts/grafana_start.sh start|stop|restart|status"; exit 1
    ;;
esac
