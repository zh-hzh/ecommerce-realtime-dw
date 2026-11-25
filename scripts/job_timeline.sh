#!/usr/bin/env bash
# 用途：打印当前时间、作业开始时间与状态，判断报错是否来自旧作业
# 执行：bash scripts/job_timeline.sh
set -euo pipefail

NOW=$(($(date +%s) * 1000))
echo "now_ms=$NOW ($(date '+%F %T'))"
curl -s --max-time 5 http://127.0.0.1:8081/jobs/overview | python3 -c '
import json, sys, datetime
now = int(sys.argv[1])
for j in json.load(sys.stdin).get("jobs", []):
    st = datetime.datetime.fromtimestamp(j["start-time"] / 1000).strftime("%F %T")
    age = (now - j["start-time"]) / 1000
    print("{} {} state={} start={} age={:.0f}s".format(
        j["jid"], j["name"], j["state"], st, age))
' "$NOW"
