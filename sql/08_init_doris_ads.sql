-- =============================================================================
-- 用途：Doris 侧建 J6 的三张 ADS 结果表
--         ads_gmv_realtime      分钟级交易看板（下单/支付/退款 + 派生率）
--         ads_conversion_funnel 日级转化漏斗（浏览 → 加购 → 提交 → 支付）
--         ads_topn_product      日级 TOP N 商品（按下单金额，rank 落库）
-- 对应文档：docs/03-数据模型.md 5.5、docs/04-架构设计.md 4.2 J6、
--           docs/02-指标体系.md 六节
-- 执行：bash scripts/init_doris.sh（建表）
-- 说明：
--   1. 三张表都是 Unique Key（merge-on-write）：ADS 是"覆盖写"的结果表，
--      刷新时同一主键后写覆盖先写，重复执行幂等（docs/03 七节）。
--   2. 分区列必须是 key 列（docs/08 E15），所以 dt 放在 key 第一位。
--      分钟表实际主键 (dt, stat_minute)，漏斗 (dt, stat_date)，
--      TOP N (dt, stat_date, rank_no)。
--   3. 动态分区保留 -90 天、预建 +3 天：ADS 可以从 DWS 重算，不需要永久保留，
--      保留 90 天与 DWS 一致（docs/03 9.3 已同步改口径）。
--   4. 单 BE 环境 replication_num 显式 1；分桶 4（docs/03 9.2）。
--   5. 刷新由 scripts/run_j6.sh 执行 Doris SQL（不是 Flink 作业），
--      原因见 docs/08 D26。
--   6. 分桶列也必须是 key 列（与分区列同一约束）。TOP N 的 product_id
--      不在 key 里，所以按 rank_no 分桶（每天最多 10 行，桶数不影响查询）。
-- =============================================================================

CREATE DATABASE IF NOT EXISTS dw;

-- --------------------------------------------------- 分钟级交易看板

CREATE TABLE IF NOT EXISTS dw.ads_gmv_realtime (
  dt           DATE          NOT NULL COMMENT '统计日期，按事件时间',
  stat_minute  DATETIME(3)   NOT NULL COMMENT '分钟起点',
  order_cnt    BIGINT        NOT NULL COMMENT '下单笔数（去重后）',
  order_amt    DECIMAL(18, 2) NOT NULL COMMENT '下单金额',
  pay_cnt      BIGINT        NOT NULL COMMENT '支付笔数',
  pay_amt      DECIMAL(18, 2) NOT NULL COMMENT '支付金额（本项目 GMV 默认口径）',
  pay_user_cnt BIGINT        NOT NULL COMMENT '该分钟支付用户数，DWD 按 user_id 去重',
  refund_cnt   BIGINT        NOT NULL COMMENT '退款笔数',
  refund_amt   DECIMAL(18, 2) NOT NULL COMMENT '退款金额，按退款发生时间归因',
  net_amt      DECIMAL(18, 2) NOT NULL COMMENT '净金额 = pay_amt - refund_amt',
  pay_rate     DECIMAL(10, 4) NOT NULL COMMENT '支付转化率 = pay_cnt / order_cnt（分钟口径，仅供参考）',
  avg_user_amt DECIMAL(18, 2) NOT NULL COMMENT '客单价 = pay_amt / pay_user_cnt（分钟口径）'
) UNIQUE KEY(dt, stat_minute)
PARTITION BY RANGE(dt) ()
DISTRIBUTED BY HASH(stat_minute) BUCKETS 4
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

-- --------------------------------------------------- 日级转化漏斗

CREATE TABLE IF NOT EXISTS dw.ads_conversion_funnel (
  dt          DATE          NOT NULL COMMENT '统计日期',
  stat_date   DATE          NOT NULL COMMENT '业务日期（与 dt 同值）',
  pv          BIGINT        NOT NULL COMMENT '当日浏览量（来自分钟 DWS 求和，受水印限制，见 08 E27）',
  cart_cnt    BIGINT        NOT NULL COMMENT '当日加购次数',
  submit_cnt  BIGINT        NOT NULL COMMENT '当日提交下单次数',
  uv          BIGINT        NOT NULL COMMENT '当日访客数，bitmap 去重',
  cart_uv     BIGINT        NOT NULL COMMENT '当日加购用户数，bitmap 去重',
  submit_uv   BIGINT        NOT NULL COMMENT '当日提交下单用户数，bitmap 去重',
  pay_user_cnt BIGINT       NOT NULL COMMENT '当日支付用户数，DWD 按 user_id 去重',
  cart_rate   DECIMAL(10, 4) NOT NULL COMMENT '浏览→加购转化率 = cart_uv / uv',
  submit_rate DECIMAL(10, 4) NOT NULL COMMENT '加购→提交转化率 = submit_uv / cart_uv',
  pay_rate    DECIMAL(10, 4) NOT NULL COMMENT '提交→支付转化率 = pay_user_cnt / submit_uv'
) UNIQUE KEY(dt, stat_date)
PARTITION BY RANGE(dt) ()
DISTRIBUTED BY HASH(stat_date) BUCKETS 4
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

-- --------------------------------------------------- 日级 TOP N 商品

CREATE TABLE IF NOT EXISTS dw.ads_topn_product (
  dt            DATE          NOT NULL COMMENT '统计日期',
  stat_date     DATE          NOT NULL COMMENT '业务日期（与 dt 同值）',
  rank_no       INT           NOT NULL COMMENT '排名，按支付金额倒序，1 为第一',
  product_id    BIGINT        NOT NULL COMMENT '商品ID',
  product_name  VARCHAR(128)           COMMENT '商品名，自 dim_product 快照退化',
  category_id   BIGINT                 COMMENT '一级类目ID',
  category_name VARCHAR(32)            COMMENT '一级类目名',
  brand         VARCHAR(32)            COMMENT '品牌',
  order_cnt     BIGINT        NOT NULL COMMENT '下单明细行数',
  item_cnt      BIGINT        NOT NULL COMMENT '下单件数',
  order_amt     DECIMAL(18, 2) NOT NULL COMMENT '下单金额',
  pay_cnt       BIGINT        NOT NULL COMMENT '支付明细行数',
  pay_amt       DECIMAL(18, 2) NOT NULL COMMENT '支付金额，排序依据'
) UNIQUE KEY(dt, stat_date, rank_no)
PARTITION BY RANGE(dt) ()
DISTRIBUTED BY HASH(rank_no) BUCKETS 4
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
