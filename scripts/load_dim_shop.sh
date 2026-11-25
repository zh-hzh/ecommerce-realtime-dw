#!/usr/bin/env bash
# =============================================================================
# 用途：把 MySQL trade.shop_info 全量导入 Doris dw.dim_shop（店铺维表）
#       流程：mysql 导出 TSV → curl Stream Load 打到 FE 8030
# 对应文档：docs/03-数据模型.md 6.2、docs/04-架构设计.md 4.2
# 执行：bash scripts/load_dim_shop.sh <mysql_root_password>
# 依赖：Doris 已建 dim_shop（scripts/init_doris.sh）；FE 8030 可连、root 免密
# 说明：
#   1. dim_shop 是"最新快照"表（UNIQUE KEY shop_id），重复执行安全：
#      同一 shop_id 后写入的覆盖先写入的。
#   2. 用 Stream Load 而不是 Flink/Spark：2000 行维度数据一次性装载，最省事；
#      实时性要求高的订单流才走 CDC。
#   3. create_time 用 DATE_FORMAT 截到秒：Doris 列是 DATETIME（不带毫秒），
#      直接给 '2025-01-02 21:51:10.006' 会因精度不符被拒。
#   4. Stream Load 必须带 -u（FE 的 /_stream_load 未认证会直接 401），且要
#      -L --location-trusted：FE 收到请求后 307 重定向到 BE 8040，curl 默认
#      跨端口重定向会丢掉 Authorization，BE 侧报 "no valid Basic authorization"。
#   5. Stream Load 失败时先看返回 JSON 里的 Status/Message/ErrorURL。
# =============================================================================
set -euo pipefail

PW="${1:?用法: $0 <mysql_root_password>}"
DORIS_DB="dw"
DORIS_TABLE="dim_shop"
FE="127.0.0.1:8030"
TMP=$(mktemp /tmp/dim_shop.XXXXXX.tsv)
trap 'rm -f "$TMP"' EXIT

echo "=== 1. 从 MySQL 导出 shop_info ==="
mysql -h127.0.0.1 -P3306 -uroot -p"${PW}" -N -B \
  -e "SELECT shop_id, shop_name, shop_type, province, city,
             DATE_FORMAT(create_time, '%Y-%m-%d %H:%i:%s'),
             CURDATE()
      FROM trade.shop_info ORDER BY shop_id;" 2>/dev/null > "$TMP"
ROWS=$(wc -l < "$TMP")
echo "  导出行数：${ROWS}"

echo "=== 2. Stream Load 到 ${DORIS_DB}.${DORIS_TABLE} ==="
LABEL="dim_shop_$(date +%Y%m%d%H%M%S)"
RESP=$(curl -sS -L --location-trusted -X PUT "http://${FE}/api/${DORIS_DB}/${DORIS_TABLE}/_stream_load" \
  -u 'root:' \
  -H "Expect:100-continue" \
  -H "label:${LABEL}" \
  -H "format:csv" \
  -H "column_separator:\t" \
  -H "columns:shop_id,shop_name,shop_type,province,city,create_time,snapshot_date" \
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
  "SELECT * FROM ${DORIS_DB}.${DORIS_TABLE} ORDER BY shop_id LIMIT 3;"
