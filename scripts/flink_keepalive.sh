#!/usr/bin/env bash
# 用途：启动 Flink 并保持 WSL 会话常驻，防止 wsl.exe 退出时把 JM/TM 一并回收
# 背景：从工具/脚本里调用 wsl 时，命令结束后整个会话被销毁，Flink 守护进程也跟着没了；
#       这里启动完集群不退出，用 sleep 占住会话
# 执行：以后台方式运行 bash scripts/flink_keepalive.sh
set -euo pipefail

bash /mnt/d/projects/ecommerce-realtime-dw/scripts/start_flink.sh
sleep 20
echo "Flink 会话保持中，Ctrl-C 或停止本任务即结束"
exec sleep infinity
