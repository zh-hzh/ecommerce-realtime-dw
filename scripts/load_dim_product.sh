#!/usr/bin/env bash
# =============================================================================
# 用途：把 MySQL trade.product_info 全量导入 Doris dw.dim_product（商品维表）
#       流程：mysql 导出 TSV → curl Stream Load 打到 FE 8030
# 对应文档：docs/03-数据模型.md 4.2 / 6.2、docs/04-架构设计.md 4.2 J3
# 执行：bash scripts/load_dim_product.sh <mysql_root_password>
# 依赖：Doris 已建 dim_product（scripts/init_doris.sh）；FE 8030 可连、root 免密
# 说明：
#   1. dim_product 是"最新快照"表（UNIQUE KEY product_id），可重复执行：
#      同一 product_id 后写入的覆盖先写入的。
#   2. 5 万行一次装载，耗时约几秒；实时性要求高的订单流才走 CDC。
#   3. update_time 用 DATE_FORMAT 截到秒，避免毫秒精度不符被拒（同 dim_shop）。
#   4. Stream Load 必须 -u 'root:' 且 -L --location-trusted（FE 307 到 BE 时保留认证）。
# =============================================================================
set -euo pipefail

PW="${1:?用法: $0 <mysql_root_password>}"
DORIS_DB="dw"
DORIS_TABLE="dim_product"
FE="127.0.0.1:8030"
TMP=$(mktemp /tmp/dim_product.XXXXXX.tsv)
trap 'rm -f "$TMP"' EXIT

echo "=== 1. 从 MySQL 导出 product_info ==="
mysql -h127.0.0.1 -P3306 -uroot -p"${PW}" -N -B \
  -e "SELECT product_id, product_name, category_id, category_name, brand, price, status,
             DATE_FORMAT(update_time, '%Y-%m-%d %H:%i:%s'),
             CURDATE()
      FROM trade.product_info ORDER BY product_id;" 2>/dev/null > "$TMP"
ROWS=$(wc -l < "$TMP")
echo "  导出行数：${ROWS}"

echo "=== 2. Stream Load 到 ${DORIS_DB}.${DORIS_TABLE} ==="
LABEL="dim_product_$(date +%Y%m%d%H%M%S)"
RESP=$(curl -sS -L --location-trusted -X PUT "http://${FE}/api/${DORIS_DB}/${DORIS_TABLE}/_stream_load" \
  -u 'root:' \
  -H "Expect:100-continue" \
  -H "label:${LABEL}" \
  -H "format:csv" \
  -H "column_separator:\t" \
  -H "columns:product_id,product_name,category_id,category_name,brand,price,status,update_time,snapshot_date" \
  -T "$TMP")
echo "  ${RESP}"

if ! echo "$RESP" | grep -qE '"Status"[[:space:]]*:[[:space:]]*"Success"'; then
  echo "Stream Load 失败，按上面 JSON 的 Message / ErrorURL 排查"
  exit 1
fi

echo "=== 3. 校验行数与抽样 ==="
mysql -h127.0.0.1 -P9030 -uroot -N -e \
  "SELECT COUNT(*) AS cnt, MIN(snapshot_date), MAX(snapshot_date) FROM ${DORIS_DB}.${DORIS_TABLE};"
mysql -h127.0.0.1 -P9030 -uroot -e \
  "SELECT * FROM ${DORIS_DB}.${DORIS_TABLE} ORDER BY product_id LIMIT 3;"
