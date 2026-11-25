#!/usr/bin/env bash
# 用途：在 WSL 内运行历史数据生成器，密码通过参数传入，不落盘
# 对应文档：docs/05-数据源与造数.md 4.1
# 执行：bash scripts/run_gen_history.sh <MySQL密码> [gen_history.py 的其他参数]
# 示例：bash scripts/run_gen_history.sh 123456 --truncate
set -euo pipefail

cd /mnt/d/projects/ecommerce-realtime-dw

if [ $# -lt 1 ]; then
  echo "用法: $0 <MySQL密码> [额外参数...]" >&2
  exit 1
fi

export MYSQL_PASSWORD="$1"
shift

python3 generator/gen_history.py "$@"
