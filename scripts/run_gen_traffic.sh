#!/usr/bin/env bash
# 用途：在 WSL 内运行埋点生成器，持续往 Kafka ods_traffic 写数据，Ctrl+C 停止
# 对应文档：docs/05-数据源与造数.md 4.3 / 五 / 六
# 执行：bash scripts/run_gen_traffic.sh [gen_traffic.py 参数]
# 示例：bash scripts/run_gen_traffic.sh --qps 200
#       bash scripts/run_gen_traffic.sh --qps 2000 --duration 1800
# 依赖：Kafka 127.0.0.1:9092 可写；gen_master.py 已产出 id_pool.json
set -euo pipefail

cd /mnt/d/projects/ecommerce-realtime-dw

python3 generator/gen_traffic.py "$@"
