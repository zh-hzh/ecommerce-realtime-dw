#!/usr/bin/env bash
# =============================================================================
# 用途：J6 —— 把 Doris 的 DWS 结果刷到三张 ADS 表
#         ads_gmv_realtime / ads_conversion_funnel / ads_topn_product
# 对应文档：docs/04-架构设计.md 4.2 J6、docs/03-数据模型.md 5.5、docs/08 D26
# 执行：
#   bash scripts/run_j6.sh                  # 刷新今天一次
#   bash scripts/run_j6.sh --dt 2026-11-25  # 刷新指定日期一次（历史重跑）
#   bash scripts/run_j6.sh --loop 60        # 每 60 秒刷新今天，Ctrl-C 结束
# 依赖：Doris FE 9030 可连；ADS 三张表已建（bash scripts/init_doris.sh）；
#       DWS 表里有当天数据（J3 / J5 在跑）
# 说明：
#   1. J6 是 Doris 侧 SQL 批刷新，不是 Flink 作业，也不占 Flink 槽位（08 D26）。
#      每天的 ADS 由 sql/j6_ads_refresh.sql 全量重算并覆盖写，重复执行幂等。
#   2. 只刷一天：默认今天；对账/补数用 --dt 指定历史日期。
#   3. 不连 MySQL、不需要密码。
# =============================================================================
set -euo pipefail

cd /mnt/d/projects/ecommerce-realtime-dw

DORIS="mysql -h127.0.0.1 -P9030 -uroot"
DT=$(date +%F)
LOOP=0

while [ $# -gt 0 ]; do
  case "$1" in
    --dt)   DT="${2:?--dt 需要 YYYY-MM-DD}"; shift 2 ;;
    --loop) LOOP="${2:?--loop 需要秒数}"; shift 2 ;;
    *)      echo "未知参数：$1（支持 --dt / --loop）"; exit 2 ;;
  esac
done

refresh_once() {
  local start end
  start=$(date +%s)
  sed "s/__DT__/${DT}/g" sql/j6_ads_refresh.sql | $DORIS
  end=$(date +%s)
  $DORIS -N -e "
    SELECT 'ads_gmv_realtime', COUNT(*) FROM dw.ads_gmv_realtime WHERE dt = '${DT}'
    UNION ALL SELECT 'ads_conversion_funnel', COUNT(*) FROM dw.ads_conversion_funnel WHERE dt = '${DT}'
    UNION ALL SELECT 'ads_topn_product', COUNT(*) FROM dw.ads_topn_product WHERE dt = '${DT}';" \
    | sed 's/^/  /'
  echo "[$(date '+%F %T')] ADS 刷新完成（dt=${DT}，耗时 $((end - start))s）"
}

if [ "${LOOP}" -eq 0 ]; then
  refresh_once
else
  echo "每 ${LOOP} 秒刷新一次（dt=${DT}），Ctrl-C 结束"
  while true; do
    refresh_once
    sleep "${LOOP}"
  done
fi
