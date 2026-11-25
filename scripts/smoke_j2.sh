#!/usr/bin/env bash
# =============================================================================
# 用途：J2 端到端冒烟测试。往 MySQL 插一条测试单，确认它经 CDC → Kafka → Flink 后
#       能在 Doris 的 dw.dwd_trade_order_detail 查到，并验证更新与逻辑删除同步。
#   1) 插入测试单（order_id 900000001，shop_id=1，超出历史造数范围）
#   2) 轮询 Doris，等到这一行出现：order_status=100 且 province/city 与 MySQL shop_info 一致
#      （province/city 来自 dim_shop Lookup Join，能对上说明维表关联生效）
#   3) UPDATE 订单状态/金额，轮询 Doris 等到 200 / 123.45（验证幂等更新）
#   4) 软删（is_deleted=1），轮询 Doris 等到 is_deleted=1（验证逻辑删除同步）
#   5) 清理：删 Doris 测试行 + 删 MySQL 测试单 + 确认作业仍在 RUNNING
# 对应文档：docs/04-架构设计.md 4.2、docs/08-问题索引.md 第七节
# 执行：bash scripts/smoke_j2.sh <mysql_root_password>
# 依赖：J1、J2 作业 RUNNING；Doris 已建表并装载维表
#       （scripts/init_doris.sh + scripts/load_dim_shop.sh）
# =============================================================================
set -u
PW="${1:?用法: $0 <mysql_root_password>}"
MYSQL="mysql -h127.0.0.1 -P3306 -uroot -p${PW}"
DORIS="mysql -h127.0.0.1 -P9030 -uroot -N"
ORDER_ID=900000001
SHOP_ID=1
JOB_NAME="J2-dwd-trade"

echo "=== 0. 前置检查：dim_shop 是否已装载 ==="
DIM_CNT=$($DORIS -e "SELECT COUNT(*) FROM dw.dim_shop;" 2>/dev/null)
if [ -z "${DIM_CNT}" ] || [ "${DIM_CNT}" -eq 0 ]; then
  echo "  [FAIL] dw.dim_shop 为空，先跑 bash scripts/load_dim_shop.sh <mysql_root_password>"
  exit 1
fi
# 测试单的店铺省份/城市以 MySQL 为准，用于断言 Lookup Join 结果
REGION=$($MYSQL -N -B -e "SELECT province, city FROM trade.shop_info WHERE shop_id = ${SHOP_ID};" 2>/dev/null)
echo "  dim_shop 行数：${DIM_CNT}；shop_id=${SHOP_ID} 期望省市：${REGION//$'\t'/ /}"

doris_query() {  # 打印测试单在当前 DWD 里的 (状态, 金额, 删除标记, 省, 市)；没有行时输出空
  $DORIS -e "SELECT order_status, order_amount, is_deleted, province, city
             FROM dw.dwd_trade_order_detail WHERE order_id = ${ORDER_ID};" 2>/dev/null
}

# wait_for <说明> <期望值>：每 2 秒查一次 Doris，最多 60 秒
wait_for() {
  local desc="$1" expect="$2" actual="" i
  for i in $(seq 1 30); do
    actual=$(doris_query)
    if [ "$actual" = "$expect" ]; then
      echo "  [OK] ${desc}：${actual}（第 $((i * 2)) 秒）"
      return 0
    fi
    sleep 2
  done
  echo "  [FAIL] ${desc}：期望 '${expect}'，实际 '${actual}'"
  return 1
}

echo "=== 1. 插入测试单 ${ORDER_ID} ==="
$MYSQL 2>/dev/null <<SQL
INSERT INTO trade.order_info
  (order_id, user_id, shop_id, order_amount, item_cnt, order_status, is_deleted, create_time)
VALUES
  (${ORDER_ID}, 1, ${SHOP_ID}, 99.90, 1, 100, 0, NOW(3));
SQL

echo "=== 2. 等 Doris 出现该行（order_status=100，且省市来自维表）==="
FAIL=0
wait_for "DWD 收到下单事件，省市关联成功" $'100\t99.90\t0\t'"${REGION}" || FAIL=1

echo "=== 3. UPDATE 状态=200、金额=123.45，等 Doris 更新（验证 sequence 列）==="
$MYSQL 2>/dev/null -e "UPDATE trade.order_info
  SET order_status = 200, order_amount = 123.45 WHERE order_id = ${ORDER_ID};"
wait_for "DWD 更新到已支付" $'200\t123.45\t0\t'"${REGION}" || FAIL=1

echo "=== 4. 软删 is_deleted=1，等 Doris 同步（验证逻辑删除）==="
$MYSQL 2>/dev/null -e "UPDATE trade.order_info
  SET is_deleted = 1 WHERE order_id = ${ORDER_ID};"
wait_for "DWD 逻辑删除标记" $'200\t123.45\t1\t'"${REGION}" || FAIL=1

echo "=== 5. 清理测试数据 ==="
$DORIS -e "DELETE FROM dw.dwd_trade_order_detail WHERE order_id = ${ORDER_ID};" 2>/dev/null
$MYSQL 2>/dev/null -e "DELETE FROM trade.order_info WHERE order_id = ${ORDER_ID};"
sleep 5
LEFT=$($DORIS -e "SELECT COUNT(*) FROM dw.dwd_trade_order_detail WHERE order_id = ${ORDER_ID};" 2>/dev/null)
echo "  Doris 残留行数：${LEFT}"

echo "=== 6. 作业状态 ==="
# jobs/overview 里保留了历史作业（含已取消的旧版），取 start-time 最大的那一个才是当前作业
STATE=$(curl -s "http://127.0.0.1:8081/jobs/overview" | python3 -c '
import json, sys
jobs = [j for j in json.load(sys.stdin)["jobs"] if j["name"] == sys.argv[1]]
cur = max(jobs, key=lambda j: j["start-time"])
print("%s %s" % (cur["state"], cur["jid"]))
' "${JOB_NAME}" 2>/dev/null)
echo "  ${STATE}"
if [ "${STATE%% *}" != "RUNNING" ]; then
  echo "  [FAIL] 当前 J2 作业不在 RUNNING"
  FAIL=1
fi

if [ "$FAIL" -ne 0 ]; then
  echo "冒烟失败，看 scripts/last_job_error.sh 与 docs/08 记录"
  exit 1
fi
echo "J2 冒烟通过"
