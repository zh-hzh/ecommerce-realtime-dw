#!/usr/bin/env bash
# =============================================================================
# 用途：J4 端到端冒烟测试。直接往 Kafka ods_traffic 写 4 条埋点，验证：
#   1) 合法事件经 J4 落到 dwd_traffic_detail，event_time 与发送值一致
#   2) 同一 event_id 发两次，DWD 只有 1 行（MoW 主键去重，Q08）
#   3) 非法 event_type 落到 dwd_dirty_record 且 rule_id=Q05
#   4) event_time 早于 30 天落到 dwd_dirty_record 且 rule_id=Q04
#   5) 清理测试行后两张表无残留，J4 仍 RUNNING
# 对应文档：docs/04-架构设计.md 4.2 J4、docs/06-数据质量与对账.md 三节
# 执行：bash scripts/smoke_j4.sh
# 依赖：J4 RUNNING；Doris 已建表；Kafka 127.0.0.1:9092 可写
# 注意：断言按 event_id 过滤，生成器同时在跑也不会互相干扰；
#       清理只删本次冒烟用的 3 个 event_id。
# =============================================================================
set -u
KAFKA_HOME=${KAFKA_HOME:-/home/hzh/apps/kafka}
DORIS="mysql -h127.0.0.1 -P9030 -uroot -N"
JOB_NAME="J4-dwd-traffic"
RUN_ID=$(date +%s)
EV_OK="smoke-traffic-ok-${RUN_ID}"
EV_BAD_TYPE="smoke-traffic-badtype-${RUN_ID}"
EV_BAD_TIME="smoke-traffic-badtime-${RUN_ID}"
TMP=$(mktemp /tmp/smoke_j4.XXXXXX.json)
trap 'rm -f "${TMP}"' EXIT

echo "=== 0. 前置检查：表与作业 ==="
CNT=$($DORIS -e "SELECT COUNT(*) FROM dw.dwd_traffic_detail;" 2>/dev/null)
if [ -z "${CNT}" ]; then
  echo "  [FAIL] dw.dwd_traffic_detail 不存在，先跑 bash scripts/init_doris.sh"
  exit 1
fi
STATE=$(curl -s "http://127.0.0.1:8081/jobs/overview" | python3 -c '
import json, sys
jobs = [j for j in json.load(sys.stdin)["jobs"] if j["name"] == sys.argv[1]]
print("%s" % max(jobs, key=lambda j: j["start-time"])["state"] if jobs else "NONE")
' "${JOB_NAME}" 2>/dev/null)
if [ "${STATE}" != "RUNNING" ]; then
  echo "  [FAIL] ${JOB_NAME} 未在运行（当前：${STATE}），先跑 bash scripts/run_j4.sh"
  exit 1
fi
echo "  dwd_traffic_detail 现有 ${CNT} 行；${JOB_NAME}：${STATE}"

# 生成 4 行消息：合法事件 ×2（重复）+ 非法枚举 + 越界时间早 40 天
IFS=$'\t' read -r OK_MS OK_TIME PAST_MS TODAY <<EOF
$(python3 - "${EV_OK}" "${EV_BAD_TYPE}" "${EV_BAD_TIME}" "${TMP}" <<'PY'
import json, sys, time
from datetime import datetime

ev_ok, ev_bad_type, ev_bad_time, out_path = sys.argv[1:5]
now_ms = int(time.time() * 1000)
past_ms = now_ms - 40 * 24 * 3600 * 1000

ok = {"event_id": ev_ok, "user_id": 1, "product_id": 1,
      "event_type": "page_view", "event_time": now_ms, "session_id": "s-smoke"}
bad_type = {"event_id": ev_bad_type, "user_id": 1, "product_id": 1,
            "event_type": "click_buy", "event_time": now_ms, "session_id": "s-smoke"}
bad_time = {"event_id": ev_bad_time, "user_id": 1, "product_id": 1,
            "event_type": "add_cart", "event_time": past_ms, "session_id": "s-smoke"}

with open(out_path, "w", encoding="utf-8") as fh:
    for ev in (ok, ok, bad_type, bad_time):
        fh.write(json.dumps(ev, ensure_ascii=False) + "\n")

print("\t".join([
    str(now_ms),
    datetime.fromtimestamp(now_ms / 1000).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
    str(past_ms),
    datetime.fromtimestamp(now_ms / 1000).strftime("%Y-%m-%d"),
]))
PY
)
EOF

echo "=== 1. 写入 4 条消息（1 条合法重复 2 次 + Q05 + Q04）==="
echo "  合法事件：${EV_OK}，event_time=${OK_TIME}"
"${KAFKA_HOME}/bin/kafka-console-producer.sh" \
  --bootstrap-server 127.0.0.1:9092 --topic ods_traffic < "${TMP}" 2>/dev/null
if [ $? -ne 0 ]; then
  echo "  [FAIL] 写 Kafka 失败"
  exit 1
fi
echo "  已写入"

doris_q() { $DORIS -e "$1" 2>/dev/null; }

# wait_for <说明> <期望值> <SQL>：每 2 秒查一次 Doris，最多 90 秒
wait_for() {
  local desc="$1" expect="$2" sql="$3" actual="" i
  for i in $(seq 1 45); do
    actual=$(doris_q "$sql")
    if [ "$actual" = "$expect" ]; then
      echo "  [OK] ${desc}：${actual}（第 $((i * 2)) 秒）"
      return 0
    fi
    sleep 2
  done
  echo "  [FAIL] ${desc}：期望 '${expect}'，实际 '${actual}'"
  return 1
}

FAIL=0

echo "=== 2. 等合法事件进 DWD（含 event_time 一致性）==="
wait_for "dwd_traffic_detail 合法行" "1	${OK_TIME}" \
  "SELECT COUNT(*), MAX(event_time) FROM dw.dwd_traffic_detail WHERE event_id = '${EV_OK}';" || FAIL=1

echo "=== 3. 等非法枚举进脏数据表（Q05）==="
wait_for "Q05 脏数据" "1" \
  "SELECT COUNT(*) FROM dw.dwd_dirty_record WHERE rule_id = 'Q05' AND biz_key = '${EV_BAD_TYPE}';" || FAIL=1

echo "=== 4. 等越界时间进脏数据表（Q04）==="
wait_for "Q04 脏数据" "1" \
  "SELECT COUNT(*) FROM dw.dwd_dirty_record WHERE rule_id = 'Q04' AND biz_key = '${EV_BAD_TIME}';" || FAIL=1

echo "=== 5. 清理测试数据 ==="
# 两张表都按 dt 分区，DELETE 必须带分区列，否则 Doris 直接拒绝
$DORIS -e "DELETE FROM dw.dwd_traffic_detail WHERE dt = '${TODAY}' AND event_id IN ('${EV_OK}', '${EV_BAD_TYPE}', '${EV_BAD_TIME}');"
$DORIS -e "DELETE FROM dw.dwd_dirty_record WHERE dt = '${TODAY}' AND biz_key IN ('${EV_OK}', '${EV_BAD_TYPE}', '${EV_BAD_TIME}');"
# Doris 的 DELETE 是异步生效的，轮询到 0 行而不是固定 sleep
LEFT=1
for i in $(seq 1 15); do
  LEFT=$($DORIS -e "SELECT (SELECT COUNT(*) FROM dw.dwd_traffic_detail WHERE dt = '${TODAY}' AND event_id IN ('${EV_OK}', '${EV_BAD_TYPE}', '${EV_BAD_TIME}'))
                         + (SELECT COUNT(*) FROM dw.dwd_dirty_record WHERE dt = '${TODAY}' AND biz_key IN ('${EV_OK}', '${EV_BAD_TYPE}', '${EV_BAD_TIME}'));" 2>/dev/null)
  [ "${LEFT}" = "0" ] && break
  sleep 2
done
echo "  Doris 残留行数：${LEFT}"
if [ "${LEFT}" != "0" ]; then
  echo "  [FAIL] 清理后仍有残留"
  FAIL=1
fi

echo "=== 6. 作业状态 ==="
STATE=$(curl -s "http://127.0.0.1:8081/jobs/overview" | python3 -c '
import json, sys
jobs = [j for j in json.load(sys.stdin)["jobs"] if j["name"] == sys.argv[1]]
cur = max(jobs, key=lambda j: j["start-time"])
print("%s %s" % (cur["state"], cur["jid"]))
' "${JOB_NAME}" 2>/dev/null)
echo "  ${STATE}"
if [ "${STATE%% *}" != "RUNNING" ]; then
  echo "  [FAIL] 当前 J4 作业不在 RUNNING"
  FAIL=1
fi

if [ "$FAIL" -ne 0 ]; then
  echo "冒烟失败，看 scripts/last_job_error.sh 与 docs/08 记录"
  exit 1
fi
echo "J4 冒烟通过"
