#!/usr/bin/env bash
# 用途：在 Doris 建 dw 库与 J2 目标表 dwd_trade_order_detail
# 对应文档：docs/03-数据模型.md 5.1、docs/04-架构设计.md 4.2
# 执行：bash scripts/init_doris.sh
# 依赖：Doris FE 9030 可连；本机 root 免密（见 docs/01 环境约定）
set -euo pipefail

cd /mnt/d/projects/ecommerce-realtime-dw

mysql -h127.0.0.1 -P9030 -uroot < sql/02_init_doris.sql

echo "--- 表结构 ---"
mysql -h127.0.0.1 -P9030 -uroot -e "SHOW CREATE TABLE dw.dwd_trade_order_detail\G" | head -30
echo "--- 分区数 ---"
mysql -h127.0.0.1 -P9030 -uroot -e "SELECT COUNT(*) AS partitions FROM information_schema.partitions WHERE table_schema='dw' AND table_name='dwd_trade_order_detail';"
