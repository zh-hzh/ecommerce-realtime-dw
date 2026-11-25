#!/usr/bin/env bash
# 用途：在 Doris 建 dw 库、J2 目标表 dwd_trade_order_detail、店铺/商品维表、
#       J3 的三张 DWS 聚合表、J4 的埋点明细表与脏数据表、J5 的流量 DWS 表、
#       J6 的三张 ADS 结果表、离线对账差异表 ads_reconcile_diff
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
mysql -h127.0.0.1 -P9030 -uroot < sql/06_init_doris_traffic.sql
mysql -h127.0.0.1 -P9030 -uroot < sql/07_init_doris_traffic_dws.sql
mysql -h127.0.0.1 -P9030 -uroot < sql/08_init_doris_ads.sql
mysql -h127.0.0.1 -P9030 -uroot < sql/10_init_doris_reconcile.sql

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
echo "--- 埋点表与脏数据表 ---"
mysql -h127.0.0.1 -P9030 -uroot -e "SELECT table_name, COUNT(*) AS partitions FROM information_schema.partitions WHERE table_schema='dw' AND table_name IN ('dwd_traffic_detail','dwd_dirty_record') GROUP BY table_name;"
echo "--- 流量 DWS 表与分区 ---"
mysql -h127.0.0.1 -P9030 -uroot -e "SELECT table_name, COUNT(*) AS partitions FROM information_schema.partitions WHERE table_schema='dw' AND table_name IN ('dws_traffic_1min','dws_user_1d') GROUP BY table_name;"
echo "--- ADS 表与分区 ---"
mysql -h127.0.0.1 -P9030 -uroot -e "SELECT table_name, COUNT(*) AS partitions FROM information_schema.partitions WHERE table_schema='dw' AND table_name LIKE 'ads_%' GROUP BY table_name;"
