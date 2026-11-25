#!/usr/bin/env bash
# =============================================================================
# 用途：J5 端到端冒烟测试。往 Kafka ods_traffic 写 6 条埋点，验证：
#   1) 分钟表 pv 按 event_id 去重（同一 page_view 发两次只算 1）
#   2) 分钟表 cart_cnt / submit_cnt 按事件类型分别计数
#   3) 分钟表 uv 是 bitmap 去重后的浏览用户数（2 个浏览用户，重复发不加人）
#   4) 日表三个用户集合各自独立（uv / cart_uv / submit_uv 用 bitmap_contains 验证）
#   5) 测试结束后 J5 仍 RUNNING
# 对应文档：docs/04-架构设计.md 4.2 J5、docs/03-数据模型.md 八节、
#           docs/02-指标体系.md 四 / 五节
# 执行：bash scripts/smoke_j5.sh
# 依赖：J5 RUNNING；Doris 已建表；Kafka 127.0.0.1:9092 可写
# 注意：
#   - 分钟表用事件时间窗口，水位线越过窗口结束时间才出结果。冒烟把事件放在“当前分钟”，
#     再写一条事件时间 +2 分钟的“推水位”事件（在 Q04 的 +5 分钟容差内），让窗口几秒内触发。
#   - 冒烟数据不删除：这些事件在 DWD 里也有，留着才能保持 DWD 与 DWS 同口径；
#     测试用户固定 900001~900003，重复跑不会继续放大日 UV。
#   - 推水位事件会把水位线推到 2 分钟后，跑完 2 分钟内的真实事件会被当成迟到丢弃，
#     所以请在埋点生成器停止时跑。
# =============================================================================
set -u
KAFKA_HOME=${KAFKA_HOME:-/home/hzh/apps/kafka}
DORIS="mysql -h127.0.0.1 -P9030 -uroot -N"
JOB_NAME="J5-dws-traffic"
RUN_ID=$(date +%s)
TMP=$(mktemp /tmp/smoke_j5.XXXXXX.json)
trap 'rm -f "${TMP}" "${TMP}.push"' EXIT

U1=900001
U2=900002
U3=900003

IFS=$'\t' read -r MIN_TS TODAY <<EOF
$(python3 - "${RUN_ID}" "${TMP}" "${U1}" "${U2}" "${U3}" <<'PY'
import json, sys, time
from datetime import datetime

run_id, out_path, u1, u2, u3 = sys.argv[1:6]
now = datetime.now()
now_ms = int(time.time() * 1000)
base = now.replace(second=0, microsecond=0)
min_ms = int(base.timestamp() * 1000)

def ev(eid, user, etype, ts):
    return {"event_id": eid, "user_id": user, "product_id": 1,
            "event_type": etype, "event_time": ts, "session_id": "s-smoke-j5"}

rows = [
    ev(f"smoke-j5-pv-a-{run_id}", u1, "page_view", min_ms),
    ev(f"smoke-j5-pv-a-{run_id}", u1, "page_view", min_ms),          # 重发，pv 不应 +1
    ev(f"smoke-j5-pv-b-{run_id}", u2, "page_view", min_ms),
    ev(f"smoke-j5-pv-b-{run_id}", u2, "page_view", min_ms),          # 重发
    ev(f"smoke-j5-cart-{run_id}", u2, "add_cart", min_ms),
    ev(f"smoke-j5-submit-{run_id}", u3, "order_submit", min_ms),
]
push = ev(f"smoke-j5-push-{run_id}", u2, "page_view", now_ms + 120000)

with open(out_path, "w", encoding="utf-8") as fh:
    for r in rows:
        fh.write(json.dumps(r, ensure_ascii=False) + "\n")
with open(out_path + ".push", "w", encoding="utf-8") as fh:
    fh.write(json.dumps(push, ensure_ascii=False) + "\n")

print("\t".join([base.strftime("%Y-%m-%d %H:%M:00"), now.strftime("%Y-%m-%d")]))
PY
)
EOF

echo "=== 0. 前置检查：表与作业 ==="
CNT=$($DORIS -e "SELECT COUNT(*) FROM dw.dws_traffic_1min;" 2>/dev/null)
if [ -z "${CNT}" ]; then
  echo "  [FAIL] dw.dws_traffic_1min 不存在，先跑 bash scripts/init_doris.sh"
  exit 1
fi
STATE=$(curl -s "http://127.0.0.1:8081/jobs/overview" -o /tmp/smoke_overview.json; python3 -c '
import json, sys
jobs = [j for j in json.load(open("/tmp/smoke_overview.json"))["jobs"] if j["name"] == sys.argv[1]]
print(max(jobs, key=lambda j: j["start-time"])["state"] if jobs else "NONE")
' "${JOB_NAME}" 2>/dev/null)
if [ "${STATE}" != "RUNNING" ]; then
  echo "  [FAIL] ${JOB_NAME} 未在运行（当前：${STATE}），先跑 bash scripts/run_j5.sh"
  exit 1
fi
echo "  dws_traffic_1min 现有 ${CNT} 行；${JOB_NAME}：${STATE}"

doris_q() { $DORIS -e "$1" 2>/dev/null; }

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

echo "=== 1. 写 6 条测试事件（事件时间 ${MIN_TS}，含 2 组重复）==="
"${KAFKA_HOME}/bin/kafka-console-producer.sh" \
  --bootstrap-server 127.0.0.1:9092 --topic ods_traffic < "${TMP}" 2>/dev/null
if [ $? -ne 0 ]; then
  echo "  [FAIL] 写 Kafka 失败"
  exit 1
fi
echo "  已写入；等 10 秒确保被消费后再推水位"
sleep 10

echo "=== 2. 写推水位事件（事件时间 +2 分钟）==="
"${KAFKA_HOME}/bin/kafka-console-producer.sh" \
  --bootstrap-server 127.0.0.1:9092 --topic ods_traffic < "${TMP}.push" 2>/dev/null
echo "  已写入"

echo "=== 3. 等分钟表出结果（pv=2 去重 / 加购 1 / 下单 1 / uv=2）==="
wait_for "分钟指标与 UV" "2	1	1	2" \
  "SELECT SUM(pv), SUM(cart_cnt), SUM(submit_cnt), bitmap_union_count(uv) FROM dw.dws_traffic_1min
   WHERE dt = '${TODAY}' AND stat_minute = '${MIN_TS}';" || FAIL=1

echo "=== 4. 日表三个用户集合（bitmap_contains：1 表示在集合里）==="
wait_for "日表用户集合" "1	1	0	1	1	0" \
  "SELECT bitmap_contains(bitmap_union(uv), ${U1}),
          bitmap_contains(bitmap_union(uv), ${U2}),
          bitmap_contains(bitmap_union(uv), ${U3}),
          bitmap_contains(bitmap_union(cart_uv), ${U2}),
          bitmap_contains(bitmap_union(submit_uv), ${U3}),
          bitmap_contains(bitmap_union(submit_uv), ${U2})
   FROM dw.dws_user_1d WHERE dt = '${TODAY}';" || FAIL=1

echo "=== 5. 作业状态 ==="
STATE=$(curl -s "http://127.0.0.1:8081/jobs/overview" -o /tmp/smoke_overview.json; python3 -c '
import json, sys
jobs = [j for j in json.load(open("/tmp/smoke_overview.json"))["jobs"] if j["name"] == sys.argv[1]]
cur = max(jobs, key=lambda j: j["start-time"])
print("%s %s" % (cur["state"], cur["jid"]))
' "${JOB_NAME}" 2>/dev/null)
echo "  ${STATE}"
if [ "${STATE%% *}" != "RUNNING" ]; then
  echo "  [FAIL] 当前 J5 作业不在 RUNNING"
  FAIL=1
fi

if [ "$FAIL" -ne 0 ]; then
  echo "冒烟失败，看 scripts/last_job_error.sh 与 docs/08 记录"
  exit 1
fi
echo "J5 冒烟通过"
