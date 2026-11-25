#!/usr/bin/env bash
# =============================================================================
# 用途：J3 端到端冒烟测试。往 MySQL 造一单（两件商品）+ 一笔支付 + 一笔成功退款，
#       确认它们经 CDC → Kafka → J3 后落到三张 DWS 表，且数值与源端一致。
#   1) 插入订单 900000011（shop_id=1）与两条明细：20.00 + 30.00 = 50.00
#   2) 等 dwd_trade_order_detail 出现该单（J2 已合并，省市非空）
#   3) 等 dws_trade_1min 该分钟 order_cnt=1、order_amt=50.00
#   4) 等 dws_trade_shop_1h 该小时 shop_id=1 的 order_cnt/order_amt，
#      并断言 province/city 与 MySQL 店铺表一致（dim_shop Lookup 生效）
#   5) 插入支付 50.00，等 DWD 与 1min/店铺小时级的 pay_cnt/pay_amt
#   6) 插入退款（先 2处理中 再改 1成功）10.00，等 1min/店铺小时级的退款指标
#   7) 等 dws_trade_product_1h 两个商品的 order_amt/pay_amt（明细 ⋈ 支付 interval join）
#   8) 清理 Doris 测试行 + MySQL 测试数据，确认作业仍 RUNNING
# 对应文档：docs/04-架构设计.md 4.2 J3、docs/08-问题索引.md
# 执行：bash scripts/smoke_j3.sh <mysql_root_password>
# 依赖：J1/J2/J3 作业 RUNNING；Doris 已建 DWS 表；dim_shop / dim_product 已装载
# 注意：清理阶段按"测试单所在的分钟/小时 + 测试商品"删除，若同一时段还有其它
#       数据会被一起删掉；冒烟请在无业务写入时跑。
# =============================================================================
set -u
PW="${1:?用法: $0 <mysql_root_password>}"
MYSQL="mysql -h127.0.0.1 -P3306 -uroot -p${PW}"
DORIS="mysql -h127.0.0.1 -P9030 -uroot -N"
ORDER_ID=900000011
SHOP_ID=1
JOB_NAME="J3-dws-trade"

echo "=== 0. 前置检查：维表与作业 ==="
DIM_P=$($DORIS -e "SELECT COUNT(*) FROM dw.dim_product;" 2>/dev/null)
if [ -z "${DIM_P}" ] || [ "${DIM_P}" -eq 0 ]; then
  echo "  [FAIL] dw.dim_product 为空，先跑 bash scripts/load_dim_product.sh <mysql_root_password>"
  exit 1
fi
STATE=$(curl -s "http://127.0.0.1:8081/jobs/overview" | python3 -c '
import json, sys
jobs = [j for j in json.load(sys.stdin)["jobs"] if j["name"] == sys.argv[1]]
print("%s" % max(jobs, key=lambda j: j["start-time"])["state"] if jobs else "NONE")
' "${JOB_NAME}" 2>/dev/null)
if [ "${STATE}" != "RUNNING" ]; then
  echo "  [FAIL] ${JOB_NAME} 未在运行（当前：${STATE}），先跑 bash scripts/run_j3.sh"
  exit 1
fi
echo "  dim_product 行数：${DIM_P}；${JOB_NAME}：${STATE}"

# 取两个上架商品作为明细商品；省市以 MySQL 为准，用于断言 dim_shop Lookup
read -r P1 P2 <<<"$($MYSQL -N -B -e "
  SELECT MIN(product_id), MAX(product_id) FROM (
    SELECT product_id FROM trade.product_info WHERE status = 1 ORDER BY product_id LIMIT 2
  ) t;" 2>/dev/null)"
REGION=$($MYSQL -N -B -e "SELECT province, city FROM trade.shop_info WHERE shop_id = ${SHOP_ID};" 2>/dev/null)
PROV=${REGION%%$'\t'*}
CITY=${REGION##*$'\t'}
echo "  测试商品：${P1} / ${P2}；店铺 ${SHOP_ID} 省市：${PROV} ${CITY}"

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

echo "=== 1. 插入测试单 ${ORDER_ID}（50.00 = 20.00 + 30.00）==="
$MYSQL 2>/dev/null <<SQL
INSERT INTO trade.order_info
  (order_id, user_id, shop_id, order_amount, item_cnt, order_status, is_deleted, create_time)
VALUES
  (${ORDER_ID}, 1, ${SHOP_ID}, 50.00, 3, 100, 0, NOW(3));
INSERT INTO trade.order_detail (order_id, product_id, num, price, amount, create_time)
VALUES
  (${ORDER_ID}, ${P1}, 2, 10.00, 20.00, NOW(3)),
  (${ORDER_ID}, ${P2}, 1, 30.00, 30.00, NOW(3));
SQL

M1=$($MYSQL -N -B -e "SELECT DATE_FORMAT(create_time, '%Y-%m-%d %H:%i:00') FROM trade.order_info WHERE order_id = ${ORDER_ID};" 2>/dev/null)
D1=${M1%% *}
H1="${M1:0:13}:00:00"
echo "  下单分钟：${M1}"

echo "=== 2. 等 J2 把该单合并进 DWD（省市非空）==="
wait_for "DWD 收到订单" "1	1" \
  "SELECT COUNT(*), COUNT(province) FROM dw.dwd_trade_order_detail WHERE order_id = ${ORDER_ID};" || FAIL=1

echo "=== 3. 等分钟级下单指标 ==="
wait_for "dws_trade_1min 下单" "1	50.00" \
  "SELECT COALESCE(SUM(order_cnt), 0), COALESCE(SUM(order_amt), 0) FROM dw.dws_trade_1min
   WHERE dt = '${D1}' AND stat_minute = '${M1}';" || FAIL=1

echo "=== 4. 等店铺小时级下单指标（含省市）==="
wait_for "dws_trade_shop_1h 下单" "1	50.00	${PROV}	${CITY}" \
  "SELECT COALESCE(SUM(order_cnt), 0), COALESCE(SUM(order_amt), 0), MAX(province), MAX(city)
   FROM dw.dws_trade_shop_1h
   WHERE dt = '${D1}' AND stat_hour = '${H1}' AND shop_id = ${SHOP_ID};" || FAIL=1

echo "=== 5. 插入支付 50.00，等 DWD 与分钟/店铺支付指标 ==="
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
wait_for "dws_trade_shop_1h 支付" "1	50.00	${PROV}	${CITY}" \
  "SELECT COALESCE(SUM(pay_cnt), 0), COALESCE(SUM(pay_amt), 0), MAX(province), MAX(city)
   FROM dw.dws_trade_shop_1h
   WHERE dt = '${D1}' AND stat_hour = '${H2}' AND shop_id = ${SHOP_ID};" || FAIL=1

echo "=== 6. 插入退款 10.00（2处理中 → 1成功），等退款指标 ==="
$MYSQL 2>/dev/null <<SQL
INSERT INTO trade.refund_info
  (refund_id, order_id, refund_amount, refund_status, refund_reason, refund_time)
VALUES
  (${ORDER_ID}, ${ORDER_ID}, 10.00, 2, '冒烟测试', NOW(3));
SQL
sleep 2
$MYSQL 2>/dev/null -e "UPDATE trade.refund_info SET refund_status = 1 WHERE refund_id = ${ORDER_ID};"

M3=$($MYSQL -N -B -e "SELECT DATE_FORMAT(refund_time, '%Y-%m-%d %H:%i:00') FROM trade.refund_info WHERE refund_id = ${ORDER_ID};" 2>/dev/null)
H3="${M3:0:13}:00:00"
wait_for "dws_trade_1min 退款" "1	10.00" \
  "SELECT COALESCE(SUM(refund_cnt), 0), COALESCE(SUM(refund_amt), 0) FROM dw.dws_trade_1min
   WHERE dt = '${D1}' AND stat_minute = '${M3}';" || FAIL=1
wait_for "dws_trade_shop_1h 退款" "1	10.00" \
  "SELECT COALESCE(SUM(refund_cnt), 0), COALESCE(SUM(refund_amt), 0) FROM dw.dws_trade_shop_1h
   WHERE dt = '${D1}' AND stat_hour = '${H3}' AND shop_id = ${SHOP_ID};" || FAIL=1

echo "=== 7. 等商品小时级指标（明细 ⋈ 支付）==="
wait_for "dws_trade_product_1h 商品 ${P1}" "20.00	20.00" \
  "SELECT COALESCE(SUM(order_amt), 0), COALESCE(SUM(pay_amt), 0) FROM dw.dws_trade_product_1h
   WHERE dt = '${D1}' AND product_id = ${P1};" || FAIL=1
wait_for "dws_trade_product_1h 商品 ${P2}" "30.00	30.00" \
  "SELECT COALESCE(SUM(order_amt), 0), COALESCE(SUM(pay_amt), 0) FROM dw.dws_trade_product_1h
   WHERE dt = '${D1}' AND product_id = ${P2};" || FAIL=1

echo "=== 8. 清理测试数据 ==="
$DORIS -e "DELETE FROM dw.dws_trade_1min WHERE dt = '${D1}' AND stat_minute IN ('${M1}', '${M2}', '${M3}');" 2>/dev/null
$DORIS -e "DELETE FROM dw.dws_trade_shop_1h WHERE dt = '${D1}' AND stat_hour IN ('${H1}', '${H2}', '${H3}') AND shop_id = ${SHOP_ID};" 2>/dev/null
$DORIS -e "DELETE FROM dw.dws_trade_product_1h WHERE dt = '${D1}' AND product_id IN (${P1}, ${P2});" 2>/dev/null
$MYSQL 2>/dev/null -e "DELETE FROM trade.refund_info WHERE order_id = ${ORDER_ID};
                       DELETE FROM trade.payment_info WHERE order_id = ${ORDER_ID};
                       DELETE FROM trade.order_detail WHERE order_id = ${ORDER_ID};
                       DELETE FROM trade.order_info WHERE order_id = ${ORDER_ID};"
sleep 5
LEFT=$($DORIS -e "SELECT (SELECT COUNT(*) FROM dw.dws_trade_1min WHERE dt = '${D1}' AND stat_minute IN ('${M1}', '${M2}', '${M3}'))
                       + (SELECT COUNT(*) FROM dw.dws_trade_shop_1h WHERE dt = '${D1}' AND stat_hour IN ('${H1}', '${H2}', '${H3}') AND shop_id = ${SHOP_ID})
                       + (SELECT COUNT(*) FROM dw.dws_trade_product_1h WHERE dt = '${D1}' AND product_id IN (${P1}, ${P2}));" 2>/dev/null)
echo "  Doris 残留行数：${LEFT}"

echo "=== 9. 作业状态 ==="
STATE=$(curl -s "http://127.0.0.1:8081/jobs/overview" | python3 -c '
import json, sys
jobs = [j for j in json.load(sys.stdin)["jobs"] if j["name"] == sys.argv[1]]
cur = max(jobs, key=lambda j: j["start-time"])
print("%s %s" % (cur["state"], cur["jid"]))
' "${JOB_NAME}" 2>/dev/null)
echo "  ${STATE}"
if [ "${STATE%% *}" != "RUNNING" ]; then
  echo "  [FAIL] 当前 J3 作业不在 RUNNING"
  FAIL=1
fi

if [ "$FAIL" -ne 0 ]; then
  echo "冒烟失败，看 scripts/last_job_error.sh 与 docs/08 记录"
  exit 1
fi
echo "J3 冒烟通过"
