#!/usr/bin/env bash
# 用途：调整 Flink 单机配置（槽位与内存），适配 J1~J6 的并行度
# 对应文档：docs/04-架构设计.md 十（单机启动顺序与资源预算）
# 执行：bash scripts/tune_flink.sh
# 说明：
#   - Flink 1.20 用 conf/config.yaml（不是旧版 flink-conf.yaml），这里按行精确替换
#   - 幂等，可重复执行；改完需重启集群（scripts/start_flink.sh）
set -euo pipefail

CONF=/home/hzh/apps/flink/conf/config.yaml
cp "$CONF" "$CONF.bak.$(date +%Y%m%d%H%M%S)"

# 槽位：6 个作业最大并行度 6，留 2 个余量
sed -i 's/^  numberOfTaskSlots: [0-9]*$/  numberOfTaskSlots: 8/' "$CONF"
# JM / TM 进程内存
sed -i 's/^      size: 1600m$/      size: 2048m/' "$CONF"
sed -i 's/^      size: 1728m$/      size: 4096m/' "$CONF"

echo "--- 生效值 ---"
grep -n -E '^  numberOfTaskSlots:|^      size:' "$CONF"
