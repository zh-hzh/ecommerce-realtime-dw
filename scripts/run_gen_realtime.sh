#!/usr/bin/env bash
# 用途：在 WSL 内运行实时交易生成器，按状态机持续写 MySQL，Ctrl+C 停止
# 对应文档：docs/05-数据源与造数.md 4.2 / 五 / 六
# 执行：MYSQL_PASSWORD=xxx bash scripts/run_gen_realtime.sh [gen_realtime.py 参数]
# 示例：MYSQL_PASSWORD=xxx bash scripts/run_gen_realtime.sh
#       MYSQL_PASSWORD=xxx bash scripts/run_gen_realtime.sh --speed 10 --orders-per-min 20
#       MYSQL_PASSWORD=xxx bash scripts/run_gen_realtime.sh --duration 60 --speed 300 --late-rate 0.2
# 依赖：MySQL 127.0.0.1:3306 可写；J1 在跑（CDC → Kafka）；gen_master.py 已产出 id_pool.json
# 说明：密码只走环境变量 MYSQL_PASSWORD 或 --password 参数，不写进脚本
set -euo pipefail

cd /mnt/d/projects/ecommerce-realtime-dw

if [ -z "${MYSQL_PASSWORD:-}" ] && [[ " $* " != *" --password "* ]]; then
  echo "缺少密码：用 MYSQL_PASSWORD=xxx bash scripts/run_gen_realtime.sh 或传 --password" >&2
  exit 1
fi

python3 generator/gen_realtime.py "$@"
