#!/usr/bin/env bash
# =============================================================================
# 用途：WSL 重启后的环境自检（对应 08-问题索引 E13）。
#   依次检查：systemd 服务 → 监听端口 → 网络地址 → Doris FE/BE → Doris 读写 → HDFS → Hive。
#   其中 Hive 放在最后并对 safemode 做重试，因为 NameNode 刚起来时会在安全模式停几秒，
#   这时建 /tmp/hive 目录会直接抛 SafeModeException，属正常现象而非故障。
# 执行：wsl -d Ubuntu -- bash /mnt/d/projects/ecommerce-realtime-dw/scripts/check_env_after_reboot.sh
# 输出：全部为只读检查，不修改任何服务状态（Doris 会临时建库 doris_health_check 再删掉）。
# 依赖：systemd、ss、mysql 客户端、hdfs / hive 命令
# =============================================================================
set -u

# 与 /etc/profile 里的路径保持一致；非登录 shell 不会自动加载，这里显式声明
export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk-amd64}"
export HADOOP_HOME="${HADOOP_HOME:-/home/hzh/apps/hadoop}"
export HIVE_HOME="${HIVE_HOME:-/home/hzh/apps/hive}"
export PATH="$JAVA_HOME/bin:$HADOOP_HOME/bin:$HIVE_HOME/bin:$PATH"

# 服务名 -> 端口；端口能否监听是“服务真的可用”的最直接判据
declare -A SERVICES=(
  [mysql]=3306
  [kafka]=9092
  [hadoop-namenode]=9000
  [doris-fe]=8030
  [doris-be]=8040
  [hive-metastore]=9083
)

echo "=== 启动时间 ==="
uptime -s
uptime -p

echo
echo "=== 等待端口（最长 180 秒）==="
deadline=$(( $(date +%s) + 180 ))
while :; do
  missing=""
  for name in "${!SERVICES[@]}"; do
    port="${SERVICES[$name]}"
    ss -lnt 2>/dev/null | grep -q ":$port " || missing="$missing $port($name)"
  done
  [ -z "$missing" ] && { echo "端口已全部就绪"; break; }
  if [ "$(date +%s)" -ge "$deadline" ]; then
    echo "等待超时，仍缺端口:$missing"
    break
  fi
  sleep 5
done

echo
echo "=== systemd 服务状态 ==="
for name in "${!SERVICES[@]}"; do
  printf '%-18s %-10s start=%-32s restarts=%s\n' \
    "$name" "$(systemctl is-active "$name" 2>/dev/null)" \
    "$(systemctl show "$name" -p ActiveEnterTimestamp --value)" \
    "$(systemctl show "$name" -p NRestarts --value)"
done

echo
echo "=== 网络地址（mirrored 模式下应与宿主网卡一致）==="
ip -4 addr show | grep 'inet ' | grep -v '127.0.0.1'

echo
echo "=== Doris FE / BE ==="
# 注意：SHOW FRONTENDS 不能用 SELECT ... FROM 包起来，这里用 \G 逐行取关键字段
timeout 25 mysql -h127.0.0.1 -P9030 -uroot -e "SHOW FRONTENDS\G" 2>/dev/null \
  | grep -E 'Host:|Role:|IsMaster:|Alive:'
timeout 25 mysql -h127.0.0.1 -P9030 -uroot -e "SHOW BACKENDS\G" 2>/dev/null \
  | grep -E 'Host:|Alive:|TabletNum:|NodeRole:'

echo
echo "=== Doris 建库→建表→写入→查询→删库 ==="
timeout 60 mysql -h127.0.0.1 -P9030 -uroot -e "
CREATE DATABASE IF NOT EXISTS doris_health_check;
DROP TABLE IF EXISTS doris_health_check.t1;
CREATE TABLE doris_health_check.t1 (id INT, v VARCHAR(10))
DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ('replication_num' = '1');
INSERT INTO doris_health_check.t1 VALUES (1, 'ok');
SELECT COUNT(*) AS cnt FROM doris_health_check.t1;
DROP DATABASE doris_health_check;" 2>&1 | tail -4

echo
echo "=== HDFS ==="
timeout 40 hdfs dfsadmin -report 2>/dev/null \
  | grep -E 'DFS Remaining|Missing blocks:|Live datanodes'

echo
echo "=== Hive（safemode 重试，最多 3 次）==="
for i in 1 2 3; do
  out=$(timeout 90 hive --silent -e "show databases;" 2>&1)
  if echo "$out" | grep -q 'SafeModeException'; then
    echo "第 $i 次：NameNode 仍在安全模式，等 10 秒重试"
    sleep 10
    continue
  fi
  echo "$out" | grep -vE 'SLF4J|WARN|Hive Session|^$' | head -10
  break
done
