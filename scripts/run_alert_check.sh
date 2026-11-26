#!/usr/bin/env bash
# 是什么：巡检告警检查的入口
# 干什么：Flink 作业状态、checkpoint 失败、Kafka lag、当日脏数据率、最近对账结果
# 对应文档：docs/06-数据质量与对账.md 8 节、docs/07-操作步骤.md 阶段 10
# 执行：bash scripts/run_alert_check.sh
# 退出码：0 无 FAIL；1 至少一项 FAIL（可直接挂在 DS 上做告警节点）
set -euo pipefail

cd /mnt/d/projects/ecommerce-realtime-dw
exec python3 -u scripts/alert_check.py "$@"
