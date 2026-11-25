#!/usr/bin/env bash
# =============================================================================
# 用途：J6 端到端冒烟测试。造一单 + 一笔支付 + 一组埋点，等 J2/J3/J5 落到 DWS 后
#       跑一次 ADS 刷新，验证三张 ADS 表与上游口径一致：
#   1) ads_gmv_realtime：测试订单分钟的 order_cnt/order_amt，
#      支付分钟的 pay_cnt/pay_amt/pay_user_cnt/net_amt/客单价
#   2) ads_topn_product：两件商品按支付金额排名（30.00 第 1，20.00 第 2）
#   3) ads_conversion_funnel：7 个计数与 DWS/DWD 重算值逐字段相等
#   4) 清理后 ADS 无测试残留（topN 每天先删后写，漏斗重算）
# 对应文档：docs/04-架构设计.md 4.2 J6、docs/03-数据模型.md 5.5、
#           docs/02-指标体系.md 六节
# 执行：bash scripts/smoke_j6.sh <mysql_root_password>
# 依赖：J2/J3/J5 RUNNING；Doris 已建 ADS 表（bash scripts/init_doris.sh）；
#       dim_product 已装载；Kafka 127.0.0.1:9092 可写
# 注意：
#   - 埋点断言用"uv 含指定用户 + 三项计数下限"而不是精确值：上一轮冒烟推水位的
#     事件时间正好 +2 分钟，连续重跑时可能落进本轮分钟，让 pv 多 1。
#   - 清理按"测试单分钟/小时 + 测试商品"删除，会连带删掉同一时段的真实数据；
#     冒烟请在埋点生成器停止、无业务写入时跑（与 smoke_j3 一致）。
#   - 测试订单号按运行时间生成（900000000 + 秒数后 8 位）：J3 的商品粒度支付指标
#     是"明细 ⋈ 1 小时内支付"，若反复用同一个订单号，新一轮支付会把上一轮还留在
#     状态里的明细再算一遍，金额翻倍（实测 20/30 变 40/60）。
#   - 清理后 DWD 里会留一行 is_deleted=1 的软删记录（J2 处理 MySQL DELETE 的方式），
#     J6 的 pay_user_cnt 已按 is_deleted=0 过滤。
# =============================================================================
set -u
PW="${1:?用法: $0 <mysql_root_password>}"
MYSQL="mysql -h127.0.0.1 -P3306 -uroot -p${PW}"
DORIS="mysql -h127.0.0.1 -P9030 -uroot -N"
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
TODAY=$(date +%F)
ORDER_ID=$(( 900000000 + $(date +%s) % 100000000 ))
USER_ID=900201
SHOP_ID=1
RUN_ID=$(date +%s)
TMP=$(mktemp /tmp/smoke_j6.XXXXXX.json)
trap 'rm -f "${TMP}" "${TMP}.push"' EXIT

echo "=== 0. 前置检查：ADS 表、维表与作业 ==="
CNT=$($DORIS -e "SELECT COUNT(*) FROM dw.ads_gmv_realtime;" 2>/dev/null)
if [ -z "${CNT}" ]; then
  echo "  [FAIL] dw.ads_gmv_realtime 不存在，先跑 bash scripts/init_doris.sh"
  exit 1
fi
DIM_P=$($DORIS -e "SELECT COUNT(*) FROM dw.dim_product;" 2>/dev/null)
if [ -z "${DIM_P}" ] || [ "${DIM_P}" -eq 0 ]; then
  echo "  [FAIL] dw.dim_product 为空，先跑 bash scripts/load_dim_product.sh <mysql_root_password>"
  exit 1
fi
for JOB in J2-dwd-trade J3-dws-trade J5-dws-traffic; do
  STATE=$(curl -s "http://127.0.0.1:8081/jobs/overview" -o /tmp/smoke_j6_overview.json; python3 -c '
import json, sys
jobs = [j for j in json.load(open("/tmp/smoke_j6_overview.json"))["jobs"] if j["name"] == sys.argv[1]]
print(max(jobs, key=lambda j: j["start-time"])["state"] if jobs else "NONE")
' "${JOB}" 2>/dev/null)
  if [ "${STATE}" != "RUNNING" ]; then
    echo "  [FAIL] ${JOB} 未在运行（当前：${STATE}）"
    exit 1
  fi
done
echo "  ADS 表就绪；dim_product ${DIM_P} 行；J2/J3/J5 RUNNING"

# 基线：今天已存在的支付用户数，从 DWD 直接算（ADS 里的值可能还没刷新）
BASE_PAYU=$($DORIS -e "SELECT COUNT(DISTINCT user_id) FROM dw.dwd_trade_order_detail
  WHERE pay_time >= '${TODAY}' AND pay_time < DATE_ADD('${TODAY}', INTERVAL 1 DAY)
    AND COALESCE(is_deleted, 0) = 0;" 2>/dev/null)
BASE_PAYU=${BASE_PAYU:-0}
echo "  基线：今日支付用户数 ${BASE_PAYU}"

# 取两个上架商品作为明细商品（与 smoke_j3 同一取法）
read -r P1 P2 <<<"$($MYSQL -N -B -e "
  SELECT MIN(product_id), MAX(product_id) FROM (
    SELECT product_id FROM trade.product_info WHERE status = 1 ORDER BY product_id LIMIT 2
  ) t;" 2>/dev/null)"
echo "  测试商品：${P1} / ${P2}"

doris_q() { $DORIS -e "$1" 2>/dev/null; }

# wait_for <说明> <期望值> <SQL>
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

# assert_eq <说明> <期望值> <实际值>
assert_eq() {
  if [ "$2" = "$3" ]; then
    echo "  [OK] $1：$3"
    return 0
  fi
  echo "  [FAIL] $1：期望 '$2'，实际 '$3'"
  return 1
}

FAIL=0

echo "=== 1. 插入测试单 ${ORDER_ID}（50.00 = 20.00 + 30.00）==="
$MYSQL 2>/dev/null <<SQL
INSERT INTO trade.order_info
  (order_id, user_id, shop_id, order_amount, item_cnt, order_status, is_deleted, create_time)
VALUES
  (${ORDER_ID}, ${USER_ID}, ${SHOP_ID}, 50.00, 3, 100, 0, NOW(3));
INSERT INTO trade.order_detail (order_id, product_id, num, price, amount, create_time)
VALUES
  (${ORDER_ID}, ${P1}, 2, 10.00, 20.00, NOW(3)),
  (${ORDER_ID}, ${P2}, 1, 30.00, 30.00, NOW(3));
SQL

M1=$($MYSQL -N -B -e "SELECT DATE_FORMAT(create_time, '%Y-%m-%d %H:%i:00') FROM trade.order_info WHERE order_id = ${ORDER_ID};" 2>/dev/null)
D1=${M1%% *}
H1="${M1:0:13}:00:00"
echo "  下单分钟：${M1}"

echo "=== 2. 等 DWS 拿到下单指标 ==="
wait_for "dws_trade_1min 下单" "1	50.00" \
  "SELECT COALESCE(SUM(order_cnt), 0), COALESCE(SUM(order_amt), 0) FROM dw.dws_trade_1min
   WHERE dt = '${D1}' AND stat_minute = '${M1}';" || FAIL=1

echo "=== 3. 插入支付 50.00，等 DWS 与商品粒度支付指标 ==="
$MYSQL 2>/dev/null <<SQL
INSERT INTO trade.payment_info
  (pay_id, order_id, pay_amount, pay_channel, pay_status, pay_time)
VALUES
  (${ORDER_ID}, ${ORDER_ID}, 50.00, 'alipay', 1, NOW(3));
SQL
wait_for "DWD 补齐支付字段" "50.00" \
  "SELECT COALESCE(pay_amount, 0) FROM dw.dwd_trade_order_detail WHERE order_id = ${ORDER_ID};" || FAIL=1

M2=$($MYSQL -N -B -e "SELECT DATE_FORMAT(pay_time, '%Y-%m-%d %H:%i:00') FROM trade.payment_info WHERE pay_id = ${ORDER_ID};" 2>/dev/null)
H2="${M2:0:13}:00:00"
wait_for "dws_trade_1min 支付" "1	50.00" \
  "SELECT COALESCE(SUM(pay_cnt), 0), COALESCE(SUM(pay_amt), 0) FROM dw.dws_trade_1min
   WHERE dt = '${D1}' AND stat_minute = '${M2}';" || FAIL=1
wait_for "dws_trade_product_1h 商品 ${P1}" "20.00" \
  "SELECT COALESCE(SUM(pay_amt), 0) FROM dw.dws_trade_product_1h
   WHERE dt = '${D1}' AND product_id = ${P1};" || FAIL=1
wait_for "dws_trade_product_1h 商品 ${P2}" "30.00" \
  "SELECT COALESCE(SUM(pay_amt), 0) FROM dw.dws_trade_product_1h
   WHERE dt = '${D1}' AND product_id = ${P2};" || FAIL=1

echo "=== 4. 写 4 条埋点（当前分钟，含 2 个浏览用户）+ 推水位 ==="
IFS=$'\t' read -r MIN_TS TODAY <<EOF
$(python3 - "${RUN_ID}" "${TMP}" <<'PY'
import json, sys, time
from datetime import datetime

run_id, out_path = sys.argv[1:3]
now = datetime.now()
now_ms = int(time.time() * 1000)
base = now.replace(second=0, microsecond=0)
min_ms = int(base.timestamp() * 1000)

def ev(eid, user, etype, ts):
    return {"event_id": eid, "user_id": user, "product_id": 1,
            "event_type": etype, "event_time": ts, "session_id": "s-smoke-j6"}

rows = [
    ev(f"smoke-j6-pv-a-{run_id}", 900301, "page_view", min_ms),
    ev(f"smoke-j6-pv-b-{run_id}", 900302, "page_view", min_ms),
    ev(f"smoke-j6-cart-{run_id}", 900302, "add_cart", min_ms),
    ev(f"smoke-j6-submit-{run_id}", 900303, "order_submit", min_ms),
]
push = ev(f"smoke-j6-push-{run_id}", 900302, "page_view", now_ms + 120000)

with open(out_path, "w", encoding="utf-8") as fh:
    for r in rows:
        fh.write(json.dumps(r, ensure_ascii=False) + "\n")
with open(out_path + ".push", "w", encoding="utf-8") as fh:
    fh.write(json.dumps(push, ensure_ascii=False) + "\n")

print("\t".join([base.strftime("%Y-%m-%d %H:%M:00"), now.strftime("%Y-%m-%d")]))
PY
)
EOF
"${KAFKA_HOME:-/home/hzh/apps/kafka}/bin/kafka-console-producer.sh" \
  --bootstrap-server 127.0.0.1:9092 --topic ods_traffic < "${TMP}" 2>/dev/null
sleep 10
"${KAFKA_HOME:-/home/hzh/apps/kafka}/bin/kafka-console-producer.sh" \
  --bootstrap-server 127.0.0.1:9092 --topic ods_traffic < "${TMP}.push" 2>/dev/null
wait_for "冒烟分钟 uv 含两个浏览用户" "1	1" \
  "SELECT bitmap_contains(bitmap_union(uv), 900301), bitmap_contains(bitmap_union(uv), 900302)
   FROM dw.dws_traffic_1min WHERE dt = '${TODAY}' AND stat_minute = '${MIN_TS}';" || FAIL=1
assert_eq "冒烟分钟三项计数达到下限（pv≥2 / 加购≥1 / 下单≥1）" "1	1	1" \
  "$(doris_q "SELECT pv >= 2, cart_cnt >= 1, submit_cnt >= 1 FROM dw.dws_traffic_1min
              WHERE dt = '${TODAY}' AND stat_minute = '${MIN_TS}';")" || FAIL=1

echo "=== 5. 刷新 ADS（dt=${D1}）==="
bash "${SCRIPT_DIR}/run_j6.sh" --dt "${D1}" || FAIL=1

echo "=== 6. 断言 ads_gmv_realtime ==="
assert_eq "下单分钟 order_cnt/order_amt" "1	50.00" \
  "$(doris_q "SELECT order_cnt, order_amt FROM dw.ads_gmv_realtime WHERE dt = '${D1}' AND stat_minute = '${M1}';")" || FAIL=1
if [ "${M1}" = "${M2}" ]; then RATE="1.0000"; else RATE="0.0000"; fi
assert_eq "支付分钟 pay_cnt/pay_amt/pay_user_cnt/net_amt/avg/pay_rate" \
  "1	50.00	1	50.00	50.00	${RATE}" \
  "$(doris_q "SELECT pay_cnt, pay_amt, pay_user_cnt, net_amt, avg_user_amt, pay_rate
              FROM dw.ads_gmv_realtime WHERE dt = '${D1}' AND stat_minute = '${M2}';")" || FAIL=1

echo "=== 7. 断言 ads_topn_product（30.00 第 1，20.00 第 2）==="
assert_eq "商品 ${P2} 排名" "1	30.00" \
  "$(doris_q "SELECT rank_no, pay_amt FROM dw.ads_topn_product WHERE dt = '${D1}' AND product_id = ${P2};")" || FAIL=1
assert_eq "商品 ${P1} 排名" "2	20.00" \
  "$(doris_q "SELECT rank_no, pay_amt FROM dw.ads_topn_product WHERE dt = '${D1}' AND product_id = ${P1};")" || FAIL=1

echo "=== 8. 断言 ads_conversion_funnel 与 DWS/DWD 重算值一致 ==="
ADS_F=$(doris_q "SELECT CONCAT_WS('\t', pv, cart_cnt, submit_cnt, uv, cart_uv, submit_uv, pay_user_cnt)
                 FROM dw.ads_conversion_funnel WHERE dt = '${TODAY}';")
SRC_F=$(doris_q "SELECT CONCAT_WS('\t',
  (SELECT COALESCE(SUM(pv), 0) FROM dw.dws_traffic_1min WHERE dt = '${TODAY}'),
  (SELECT COALESCE(SUM(cart_cnt), 0) FROM dw.dws_traffic_1min WHERE dt = '${TODAY}'),
  (SELECT COALESCE(SUM(submit_cnt), 0) FROM dw.dws_traffic_1min WHERE dt = '${TODAY}'),
  (SELECT COALESCE(bitmap_union_count(uv), 0) FROM dw.dws_user_1d WHERE dt = '${TODAY}'),
  (SELECT COALESCE(bitmap_union_count(cart_uv), 0) FROM dw.dws_user_1d WHERE dt = '${TODAY}'),
  (SELECT COALESCE(bitmap_union_count(submit_uv), 0) FROM dw.dws_user_1d WHERE dt = '${TODAY}'),
  (SELECT COUNT(DISTINCT user_id) FROM dw.dwd_trade_order_detail
    WHERE pay_time >= '${TODAY}' AND pay_time < DATE_ADD('${TODAY}', INTERVAL 1 DAY)
      AND COALESCE(is_deleted, 0) = 0));")
assert_eq "漏斗 7 个计数" "${SRC_F}" "${ADS_F}" || FAIL=1

echo "=== 9. 清理测试数据（先软删，等 DWD 标记，再删 MySQL 行）==="
$MYSQL 2>/dev/null -e "UPDATE trade.order_info SET is_deleted = 1 WHERE order_id = ${ORDER_ID};"
wait_for "DWD 软删标记" "1" \
  "SELECT COALESCE(is_deleted, 0) FROM dw.dwd_trade_order_detail WHERE order_id = ${ORDER_ID};" || FAIL=1
$MYSQL 2>/dev/null -e "DELETE FROM trade.refund_info WHERE order_id = ${ORDER_ID};
                       DELETE FROM trade.payment_info WHERE order_id = ${ORDER_ID};
                       DELETE FROM trade.order_detail WHERE order_id = ${ORDER_ID};
                       DELETE FROM trade.order_info WHERE order_id = ${ORDER_ID};"
sleep 5
$DORIS -e "DELETE FROM dw.dws_trade_1min WHERE dt = '${D1}' AND stat_minute IN ('${M1}', '${M2}');" 2>/dev/null
$DORIS -e "DELETE FROM dw.dws_trade_shop_1h WHERE dt = '${D1}' AND stat_hour IN ('${H1}', '${H2}') AND shop_id = ${SHOP_ID};" 2>/dev/null
$DORIS -e "DELETE FROM dw.dws_trade_product_1h WHERE dt = '${D1}' AND product_id IN (${P1}, ${P2});" 2>/dev/null
$DORIS -e "DELETE FROM dw.ads_gmv_realtime WHERE dt = '${D1}' AND stat_minute IN ('${M1}', '${M2}');" 2>/dev/null
bash "${SCRIPT_DIR}/run_j6.sh" --dt "${D1}" >/dev/null || FAIL=1

LEFT=$(doris_q "SELECT
  (SELECT COUNT(*) FROM dw.ads_gmv_realtime WHERE dt = '${D1}' AND stat_minute IN ('${M1}', '${M2}'))
+ (SELECT COUNT(*) FROM dw.ads_topn_product WHERE dt = '${D1}' AND product_id IN (${P1}, ${P2}));")
assert_eq "ADS 测试残留（分钟行 + TOPN 行）" "0" "${LEFT}" || FAIL=1
PAYU=$(doris_q "SELECT COALESCE(pay_user_cnt, 0) FROM dw.ads_conversion_funnel WHERE dt = '${TODAY}';")
assert_eq "ADS 支付用户数回到基线" "${BASE_PAYU}" "${PAYU}" || FAIL=1

echo "=== 10. 作业状态 ==="
for JOB in J2-dwd-trade J3-dws-trade; do
  STATE=$(curl -s "http://127.0.0.1:8081/jobs/overview" -o /tmp/smoke_j6_overview.json; python3 -c '
import json, sys
jobs = [j for j in json.load(open("/tmp/smoke_j6_overview.json"))["jobs"] if j["name"] == sys.argv[1]]
cur = max(jobs, key=lambda j: j["start-time"])
print("%s %s" % (cur["state"], cur["jid"]))
' "${JOB}" 2>/dev/null)
  echo "  ${JOB}：${STATE}"
  if [ "${STATE%% *}" != "RUNNING" ]; then
    echo "  [FAIL] ${JOB} 不在 RUNNING"
    FAIL=1
  fi
done

if [ "$FAIL" -ne 0 ]; then
  echo "冒烟失败，看 scripts/last_job_error.sh 与 docs/08 记录"
  exit 1
fi
echo "J6 冒烟通过"
