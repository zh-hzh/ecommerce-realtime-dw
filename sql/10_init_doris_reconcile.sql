-- 电商交易实时数仓：对账差异表
-- 对应 docs/06-数据质量与对账.md 7.1；写入方是 spark/reconcile.py
-- 是什么：每个对账日、每个指标一行，存实时值、离线值、差异与判定
-- 干什么：spark/reconcile.py 每次跑批覆盖写（主键 stat_date + metric），看板只读当前结论
-- 执行：bash scripts/init_doris.sh（已包含本文件）

CREATE TABLE IF NOT EXISTS dw.ads_reconcile_diff (
  stat_date      DATE          NOT NULL COMMENT '对账日期，通常 T-1',
  metric         VARCHAR(32)   NOT NULL COMMENT '指标英文名：order_cnt/pay_cnt/pay_user_cnt/pay_amt/refund_amt',
  realtime_value DECIMAL(18, 2) NOT NULL COMMENT '实时侧取值',
  offline_value  DECIMAL(18, 2) NOT NULL COMMENT '离线侧取值',
  diff_value     DECIMAL(18, 2) NOT NULL COMMENT '实时 - 离线',
  diff_rate      DOUBLE        NULL     COMMENT '|差异| / 离线，离线为 0 时置 NULL',
  threshold      VARCHAR(16)   NOT NULL COMMENT '0 或 0.1%',
  status         VARCHAR(8)    NOT NULL COMMENT 'PASS / FAIL',
  check_time     DATETIME(3)   NOT NULL COMMENT '跑批时间'
) UNIQUE KEY(stat_date, metric)
DISTRIBUTED BY HASH(stat_date) BUCKETS 4
PROPERTIES (
  'replication_num' = '1',
  -- MoW 保证同一 (stat_date, metric) 覆盖写后立刻可读，不依赖后台 compaction
  'enable_unique_key_merge_on_write' = 'true'
);
