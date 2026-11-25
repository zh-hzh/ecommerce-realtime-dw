#!/usr/bin/env bash
# 用途：解析最近一个 Flink 作业的异常，只打印 Caused by 链，便于快速定位根因
# 执行：bash scripts/last_job_error.sh [jobId]
set -euo pipefail

JID=${1:-}
if [ -z "$JID" ]; then
  JID=$(curl -s --max-time 5 http://127.0.0.1:8081/jobs/overview \
        | sed -n 's/.*"jid":"\([^"]*\)".*/\1/p' | head -n 1)
fi

echo "jobId=$JID"
curl -s --max-time 5 "http://127.0.0.1:8081/jobs/$JID/exceptions" | python3 -c '
import json, re, sys
d = json.load(sys.stdin)
s = d.get("root-exception", "")
causes = re.findall(r"Caused by: [^\n]+", s)
print("\n".join(causes[:8]) if causes else s[:3000])
'
