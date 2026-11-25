#!/usr/bin/env bash
# 用途：把 Flink CDC MySQL 连接器升级到 3.7.0-1.20（支持 MySQL 8.4+）
# 背景：3.2.1 读 binlog 位点时执行 SHOW MASTER STATUS，MySQL 8.4 已删除该命令，
#       报 "You have an error in your SQL syntax ... near 'MASTER STATUS'"
#       3.6 起 Maven 版本号带 Flink 小版本后缀（3.6.0-1.20 / 3.7.0-1.20）
# 执行：bash scripts/install_cdc.sh
set -euo pipefail

LIB=/home/hzh/apps/flink/lib
# 旧 jar 必须挪到 lib 之外：Flink 启动时会把 lib 下的 jar **递归**加入 classpath，
# 放在 lib/backup/ 里仍会被加载，且按路径排序在 3.7 之前，导致新版本被旧类覆盖。
OLD_DIR=/home/hzh/apps/flink/old-jars
VER=3.7.0-1.20
JAR=flink-sql-connector-mysql-cdc-${VER}.jar
URL=https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-mysql-cdc/${VER}/${JAR}

mkdir -p "$OLD_DIR"
# 旧版本挪走，避免同一 class 出现两份
for old in "$LIB"/flink-sql-connector-mysql-cdc-*.jar; do
  [ -e "$old" ] || continue
  mv "$old" "$OLD_DIR/"
  echo "已备份 $(basename "$old") 到 $OLD_DIR/"
done

curl -fL --max-time 180 -o "$LIB/$JAR" "$URL"
ls -lh "$LIB/$JAR"
