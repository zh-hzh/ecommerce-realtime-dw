#!/usr/bin/env bash
# 用途：打印失败作业里与 cdc / debezium 相关的调用栈，判断走的是哪条代码路径
# 执行：bash scripts/cdc_stack.sh [jobId]
set -euo pipefail

JID=${1:-}
if [ -z "$JID" ]; then
  JID=$(curl -s --max-time 5 http://127.0.0.1:8081/jobs/overview \
        | sed -n 's/.*"jid":"\([^"]*\)".*/\1/p' | head -n 1)
fi

curl -s --max-time 5 "http://127.0.0.1:8081/jobs/$JID/exceptions" | python3 -c '
import json, sys
s = json.load(sys.stdin).get("root-exception", "")
lines = [l.strip() for l in s.split("\n")
         if "flink.cdc" in l or "debezium" in l.lower()]
print("\n".join(lines[:25]))
'
