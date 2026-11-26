#!/usr/bin/env bash
# 用途：端到端延迟探针（测量窗口模式）——测"订单写入 MySQL → 在 Doris DWD 可见"的延迟
# 对应文档：docs/07-操作步骤.md 阶段 8、docs/03-数据模型.md、docs/08 E34
# 执行：MYSQL_PASSWORD=xxx bash scripts/latency_probe.sh [--duration 600] [--interval 10] [--timeout 120]
#       默认：窗口 600 秒、每 10 秒一条、共 60 个样本，单样本最多等 120 秒
# 依赖：MySQL 3306 可写、Doris 9030 可连、Flink J1（CDC→Kafka）与 J2（Kafka→DWD）在跑
# 说明：
#   1. 每条样本 = 往 trade.order_info 插一条 0.01 元测试单（user_id / shop_id /
#      product_id 全用 999999 哨兵号），再配一条 order_detail。插入后另起后台轮询，
#      等它在 dw.dwd_trade_order_detail 出现：出现即记 latency_ms，超时记 success=0。
#   2. 只写"已下单"（order_status=100），不写支付/退款流水。因此这条单不进支付口径、
#      不进 TOP N（ADS 按 pay_amt>0 过滤）、不进漏斗支付分子。
#   3. 采样间隔默认 10 秒，与 J2 的 10s checkpoint 对齐：Doris sink 开了 2PC，
#      数据在 checkpoint 完成时才提交，所以端到端延迟的理论下限就是 checkpoint 间隔。
#   4. 结果落 dw.latency_probe，不参与任何对账口径，也不参与 ADS 刷新。
#   5. 中断即停：已写入的样本和 MySQL 里的测试单都不会自动清理，重跑换一个 window_id。
set -uo pipefail

cd /mnt/d/projects/ecommerce-realtime-dw || exit 1

DURATION=600
INTERVAL=10
TIMEOUT=120
while [ $# -gt 0 ]; do
  case "$1" in
    --duration) DURATION=${2:?}; shift 2 ;;
    --interval) INTERVAL=${2:?}; shift 2 ;;
    --timeout)  TIMEOUT=${2:?};  shift 2 ;;
    -h|--help)  sed -n '2,20p' "$0"; exit 0 ;;
    *) echo "未知参数：$1"; exit 1 ;;
  esac
done

[ -n "${MYSQL_PASSWORD:-}" ] || { echo "缺少密码：MYSQL_PASSWORD=xxx bash scripts/latency_probe.sh" >&2; exit 1; }

src() { MYSQL_PWD="$MYSQL_PASSWORD" mysql -h127.0.0.1 -P3306 -uroot trade -N -e "$1"; }
dw()  { mysql -h127.0.0.1 -P9030 -uroot dw -N -e "$1"; }

src "SELECT 1" >/dev/null 2>&1 || { echo "MySQL 源库连不上" >&2; exit 1; }
dw  "SELECT 1" >/dev/null 2>&1 || { echo "Doris 9030 连不上" >&2; exit 1; }

WIN=$(date +%Y%m%dT%H%M%S)
START_S=$(date +%s)
SLOTS=$(( DURATION / INTERVAL ))

echo "窗口 $WIN：${SLOTS} 个样本，每 ${INTERVAL}s 一条，单样本超时 ${TIMEOUT}s"

# 后台轮询：等到 DWD 出现就写结果行，超时写 success=0
poll_one() {
  local pid=$1 slot=$2 wts=$3 deadline=$(( $4 / 1000 + TIMEOUT ))
  local seen lat
  while [ "$(date +%s)" -lt "$deadline" ]; do
    if [ "$(dw "SELECT COUNT(*) FROM dw.dwd_trade_order_detail WHERE order_id=$pid" 2>/dev/null)" = "1" ]; then
      seen=$(date '+%Y-%m-%d %H:%M:%S.%3N')
      lat=$(( $(date +%s%3N) - $4 ))
      dw "INSERT INTO dw.latency_probe
            (dt, window_id, probe_id, window_slot, write_ts, seen_ts, latency_ms, success, order_amount)
          VALUES ('${wts:0:10}', '$WIN', $pid, $slot, '$wts', '$seen', $lat, 1, 0.01)" >/dev/null
      return 0
    fi
    sleep 3
  done
  dw "INSERT INTO dw.latency_probe
        (dt, window_id, probe_id, window_slot, write_ts, seen_ts, latency_ms, success, order_amount)
      VALUES ('${wts:0:10}', '$WIN', $pid, $slot, '$wts', NULL, NULL, 0, 0.01)" >/dev/null
}

i=0
while [ "$i" -lt "$SLOTS" ]; do
  i=$(( i + 1 ))
  # 订单号 = 999999 前缀 + 毫秒时间戳后 10 位：始终 16 位、以 999999 开头，
  # 与真实订单号不重叠，按前缀就能精确筛出/清理
  pid=$(( 9999990000000000 + $(date +%s%3N) % 10000000000 ))
  wts=$(date '+%Y-%m-%d %H:%M:%S.%3N')

  src "INSERT INTO order_info
         (order_id, user_id, shop_id, order_amount, item_cnt, order_status, is_deleted, create_time, update_time)
       VALUES ($pid, 999999, 999999, 0.01, 1, 100, 0, '$wts', '$wts')" >/dev/null || {
         echo "第 $i 条写入失败，跳过"; continue; }
  src "INSERT INTO order_detail (order_id, product_id, num, price, amount, create_time)
       VALUES ($pid, 999999, 1, 0.01, 0.01, '$wts')" >/dev/null

  wms=$(date +%s%3N)
  poll_one "$pid" "$i" "$wts" "$wms" &
  printf '\r已投递 %d/%d' "$i" "$SLOTS"

  next=$(( START_S + i * INTERVAL ))
  now=$(date +%s)
  [ "$next" -gt "$now" ] && sleep $(( next - now ))
done
echo
echo "投递完成，等待最后的轮询结束..."
wait

echo "=== 窗口 $WIN 结果 ==="
dw "SELECT COUNT(*) AS 样本数,
           SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) AS 成功数,
           ROUND(AVG(CASE WHEN success = 1 THEN latency_ms END)) AS 平均ms,
           PERCENTILE(latency_ms, 0.5)  AS P50,
           PERCENTILE(latency_ms, 0.99) AS P99
    FROM dw.latency_probe
    WHERE window_id = '$WIN'" | awk -F'\t' '{ for (i = 1; i <= NF; i++) if ($i == "NULL" || $i == "") $i = "-"; printf "样本=%s 成功=%s 平均=%sms P50=%sms P99=%sms\n", $1, $2, $3, $4, $5 }'
