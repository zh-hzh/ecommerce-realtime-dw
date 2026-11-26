-- =============================================================================
-- 用途：延迟探针结果表 —— 记录每条测试单从 MySQL 写入到 DWD 可见的端到端延迟
-- 对应文档：docs/07-操作步骤.md 阶段 8、docs/03-数据模型.md
-- 执行：bash scripts/init_doris.sh
-- 说明：
--   1. 探针是"测量窗口"模式：平时不跑；需要时开 10 分钟、每 10 秒写一条测试单，
--      由 scripts/latency_probe.sh 写入结果，共 60 个样本。
--   2. 探针单特征：MySQL 侧 user_id=999999、订单号在哨兵段、金额 0.01 元、停在
--      "已下单"不支付。因此不进支付口径、不进 TOP N、不进漏斗支付分子，
--      副作用边界见 docs/08 E34。
--   3. 本表只是测量结果，不参与任何对账口径，也不参与 ADS 刷新。
--   4. latency_ms 为端到端观测值：写入 MySQL 的系统时间 → 该订单在 DWD 可见的时间。
--      含 CDC + Kafka + Flink 处理，不含 MySQL 事务提交前的排队。
-- =============================================================================

CREATE DATABASE IF NOT EXISTS dw;

CREATE TABLE IF NOT EXISTS dw.latency_probe (
  dt          DATE         NOT NULL COMMENT '写入日期',
  window_id   VARCHAR(32)  NOT NULL COMMENT '测量窗口标识，如 20261126T1500',
  probe_id    BIGINT       NOT NULL COMMENT '探针订单ID（哨兵段，999999 开头）',
  window_slot INT          NOT NULL COMMENT '窗口内第几个样本，1~60',
  write_ts    DATETIME(3)  NOT NULL COMMENT '写入 MySQL 的时间',
  seen_ts     DATETIME(3)           COMMENT '在 DWD 首次出现的时间；超时为 NULL',
  latency_ms  BIGINT                COMMENT '端到端延迟毫秒；超时为 NULL',
  success     TINYINT      NOT NULL COMMENT '1=窗口内观测到；0=超时未观测到',
  order_amount DECIMAL(10, 2) NOT NULL COMMENT '探针订单金额，固定 0.01'
) UNIQUE KEY(dt, window_id, probe_id)
PARTITION BY RANGE(dt) ()
DISTRIBUTED BY HASH(probe_id) BUCKETS 4
PROPERTIES (
  'replication_num' = '1',
  'enable_unique_key_merge_on_write' = 'true',
  'dynamic_partition.enable' = 'true',
  'dynamic_partition.time_unit' = 'DAY',
  'dynamic_partition.start' = '-90',
  'dynamic_partition.end' = '3',
  'dynamic_partition.prefix' = 'p',
  'dynamic_partition.buckets' = '4'
);
