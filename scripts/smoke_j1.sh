#!/usr/bin/env bash
# =============================================================================
# 用途：J1 端到端冒烟测试。在 MySQL 插一条测试单，确认它经 CDC 进入 ods_order，再删除并确认删除事件。
#   1) 记录插入前 offset
#   2) 插入测试单（order_id 固定 900000001，超出历史造数范围）
#   3) 等 8 秒后看 offset 是否增长，并从头消费出这条消息（op=c）
#   4) 删除测试单，再消费出 op=d 的删除事件
# 对应文档：docs/04-架构设计.md 4.2、docs/08-问题索引.md 第七节
# 执行：bash scripts/smoke_j1.sh <mysql_root_password>
# 输出：只打印 offset 与消息体；测试单跑完即删，不在库里留下脏数据
# 依赖：J1 四个作业处于 RUNNING、Kafka 与 MySQL 可用
# =============================================================================
set -u
PW="${1:?用法: $0 <mysql_root_password>}"
export PATH="/home/hzh/apps/kafka/bin:$PATH"
MYSQL="mysql -h127.0.0.1 -P3306 -uroot -p${PW}"
BS="127.0.0.1:9092"
ORDER_ID=900000001

echo "=== 1. 插入前 ods_order 末端位点 ==="
kafka-get-offsets.sh --bootstrap-server "$BS" --topic ods_order --time -1 2>/dev/null

echo
echo "=== 2. 插入测试单 ==="
$MYSQL 2>/dev/null <<SQL
INSERT INTO trade.order_info
  (order_id, user_id, shop_id, order_amount, item_cnt, order_status, is_deleted, create_time)
VALUES
  (${ORDER_ID}, 1, 1, 99.90, 1, 100, 0, NOW(3))
ON DUPLICATE KEY UPDATE update_time = NOW(3);
SQL

sleep 8
echo "=== 3. 插入后位点（应 +1）==="
kafka-get-offsets.sh --bootstrap-server "$BS" --topic ods_order --time -1 2>/dev/null

echo
echo "=== 4. 从头消费到第一条 op=c 的消息 ==="
# 从头消费并只保留与本测试单相关的行，避免历史消息干扰判断
timeout 30 kafka-console-consumer.sh --bootstrap-server "$BS" --topic ods_order \
  --from-beginning --property print.key=true --timeout-ms 15000 2>/dev/null \
  | grep -F "\"order_id\":${ORDER_ID}" | grep -F '"op":"c"' | head -1

echo
echo "=== 5. 删除测试单并消费 op=d 事件 ==="
$MYSQL -e "DELETE FROM trade.order_info WHERE order_id = ${ORDER_ID};" 2>/dev/null
sleep 6
timeout 30 kafka-console-consumer.sh --bootstrap-server "$BS" --topic ods_order \
  --from-beginning --property print.key=true --timeout-ms 15000 2>/dev/null \
  | grep -F "\"order_id\":${ORDER_ID}" | grep -F '"op":"d"' | head -1
