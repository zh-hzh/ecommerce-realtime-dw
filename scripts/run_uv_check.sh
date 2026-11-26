#!/usr/bin/env bash
# 是什么：UV 对账入口（实时 bitmap vs 明细去重）
# 干什么：比对 dws_user_1d 与 dwd_traffic_detail 的当日 page_view UV，写 ads_reconcile_diff
# 对应文档：docs/06-数据质量与对账.md 6 节、docs/08-问题索引.md D30/D38
# 执行：bash scripts/run_uv_check.sh [--dt 2026-11-25]
# 退出码：0 PASS；1 FAIL（差异超 0.1%）；2 没有可比数据
set -euo pipefail
cd /mnt/d/projects/ecommerce-realtime-dw
exec python3 -u scripts/uv_check.py "$@"
