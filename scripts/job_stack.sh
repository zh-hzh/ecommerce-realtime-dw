#!/usr/bin/env bash
# 用途：打印失败作业 root-exception 的前 N 行调用栈
# 执行：bash scripts/job_stack.sh [jobId] [行数]
set -euo pipefail

JID=${1:-}
LINES=${2:-80}
if [ -z "$JID" ]; then
  JID=$(curl -s --max-time 5 http://127.0.0.1:8081/jobs/overview \
        | sed -n 's/.*"jid":"\([^"]*\)".*/\1/p' | head -n 1)
fi

curl -s --max-time 5 "http://127.0.0.1:8081/jobs/$JID/exceptions" | python3 -c '
import json, sys
n = int(sys.argv[1])
s = json.load(sys.stdin).get("root-exception", "")
print("\n".join(s.split("\n")[:n]))
' "$LINES"
