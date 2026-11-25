#!/usr/bin/env bash
# 建 5 个 ODS topic（对应 docs/04-架构设计.md 4.1 / docs/05-数据源与造数.md 7.3）
# 分区数一次定死：改分区会打乱 key -> partition 映射
set -euo pipefail

export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-11-openjdk-amd64}
KAFKA_HOME=${KAFKA_HOME:-/home/hzh/apps/kafka}
BOOTSTRAP=${BOOTSTRAP:-127.0.0.1:9092}

while read -r topic partitions; do
  "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists --topic "$topic" \
    --partitions "$partitions" --replication-factor 1
done <<'EOF'
ods_order 3
ods_order_detail 3
ods_payment 3
ods_refund 3
ods_traffic 6
EOF

"$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --list
