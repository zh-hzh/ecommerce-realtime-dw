#!/usr/bin/env bash
# 是什么：启动 DS 并占住 WSL 会话，防止 wsl.exe 退出时把 JVM 一并回收
# 干什么：调 start_ds.sh，然后 sleep infinity
# 对应文档：docs/07-操作步骤.md 阶段 9
# 执行：MYSQL_PASSWORD=xxx bash scripts/ds_keepalive.sh（后台跑）
set -euo pipefail

bash /mnt/d/projects/ecommerce-realtime-dw/scripts/start_ds.sh
sleep 20
echo "DolphinScheduler 会话保持中，Ctrl-C 或停止本任务即结束"
exec sleep infinity
