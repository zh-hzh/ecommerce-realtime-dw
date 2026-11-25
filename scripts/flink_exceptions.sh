#!/usr/bin/env bash
# 用途：打印最近一个 Flink 作业的异常栈，排错用
# 执行：bash scripts/flink_exceptions.sh [jobId]
set -euo pipefail

JID=${1:-}
if [ -z "$JID" ]; then
  JID=$(curl -s --max-time 5 http://127.0.0.1:8081/jobs/overview \
        | sed -n 's/.*"jid":"\([^"]*\)".*/\1/p' | head -n 1)
fi

echo "jobId=$JID"
curl -s --max-time 5 "http://127.0.0.1:8081/jobs/$JID/exceptions"
echo
