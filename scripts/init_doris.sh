#!/usr/bin/env bash
# 用途：在 Doris 建 dw 库、J2 目标表 dwd_trade_order_detail、店铺/商品维表、
#       J3 的三张 DWS 聚合表
# 对应文档：docs/03-数据模型.md 5.1 / 6.2 / 七节、docs/04-架构设计.md 4.2
# 执行：bash scripts/init_doris.sh
# 依赖：Doris FE 9030 可连；本机 root 免密（见 docs/01 环境约定）
# 说明：只建表不灌数；维表数据由 scripts/load_dim_shop.sh、load_dim_product.sh 装载
set -euo pipefail

cd /mnt/d/projects/ecommerce-realtime-dw

mysql -h127.0.0.1 -P9030 -uroot < sql/02_init_doris.sql
mysql -h127.0.0.1 -P9030 -uroot < sql/03_init_doris_dim.sql
mysql -h127.0.0.1 -P9030 -uroot < sql/04_init_doris_dws.sql
mysql -h127.0.0.1 -P9030 -uroot < sql/05_init_doris_dim_product.sql

echo "--- 表结构 ---"
mysql -h127.0.0.1 -P9030 -uroot -e "SHOW CREATE TABLE dw.dwd_trade_order_detail\G" | head -30
echo "--- 分区数 ---"
mysql -h127.0.0.1 -P9030 -uroot -e "SELECT COUNT(*) AS partitions FROM information_schema.partitions WHERE table_schema='dw' AND table_name='dwd_trade_order_detail';"
echo "--- 维表 ---"
mysql -h127.0.0.1 -P9030 -uroot -e "SHOW CREATE TABLE dw.dim_shop\G" | head -20
echo "--- 商品维表 ---"
mysql -h127.0.0.1 -P9030 -uroot -e "SHOW CREATE TABLE dw.dim_product\G" | head -20
echo "--- DWS 表与分区 ---"
mysql -h127.0.0.1 -P9030 -uroot -e "SELECT table_name, COUNT(*) AS partitions FROM information_schema.partitions WHERE table_schema='dw' AND table_name LIKE 'dws_trade%' GROUP BY table_name;"
