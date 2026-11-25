#!/usr/bin/env bash
# 用途：查看 Flink 集群与作业状态（WSL 内执行，避免 Windows curl 互操作问题）
# 执行：bash scripts/check_flink.sh
set -euo pipefail

echo "--- 集群 ---"
curl -s --max-time 5 http://127.0.0.1:8081/overview || echo "8081 无响应"
echo
echo "--- 作业 ---"
curl -s --max-time 5 http://127.0.0.1:8081/jobs/overview || echo "作业接口无响应"
echo
