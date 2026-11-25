-- =============================================================================
-- 用途：Doris 侧建 J3 的三张 DWS 聚合表（分钟级 / 店铺小时级 / 商品小时级）
-- 对应文档：docs/03-数据模型.md 二节 / 七节 / 九节、docs/04-架构设计.md 4.2 J3
-- 执行：bash scripts/init_doris.sh（建表）
-- 说明：
--   1. Aggregate 模型：写入时就按 key 聚合，看板查询不用 GROUP BY（docs/03 七节）。
--      代价是不能查明细，DWS 本来也不需要。
--   2. 分区列必须是 key 列（docs/08 E15），所以 dt 放在 key 的第一位，
--      实际主键是 (dt, stat_minute) / (dt, stat_hour, shop_id) /
--      (dt, stat_hour, product_id)。文档里的"主键 stat_minute"指的是业务粒度。
--   3. 三条流（订单/支付/退款）各写各的度量列，JSON 里没带的列由 Doris 置 NULL，
--      而 SUM 聚合会跳过 NULL（实测：先写 a=5 再只写 b=7，结果是 a=5 b=7）。
--      所以互不重叠的度量列可以分多次写入同一行。
--   4. 维度列（province/city/category/brand）用 REPLACE_IF_NOT_NULL 而不是 REPLACE：
--      REPLACE 被 NULL 覆盖会把已经写好的维度冲掉（实测），REPLACE_IF_NOT_NULL 不会。
--   5. 单 BE 环境，replication_num 必须显式 1；分桶 4（docs/03 9.2）。
--   6. 动态分区：保留 -90 天、预建 +3 天，与 docs/03 9.1 一致。
-- =============================================================================

CREATE DATABASE IF NOT EXISTS dw;

-- ------------------------------------------------------------- 分钟级（全局）

CREATE TABLE IF NOT EXISTS dw.dws_trade_1min (
  dt          DATE          NOT NULL COMMENT '统计日期，按事件时间',
  stat_minute DATETIME(3)   NOT NULL COMMENT '分钟起点（事件时间截断到分钟）',
  order_cnt   BIGINT        SUM      COMMENT '下单笔数，来源 ods_order 的 c 事件',
  order_amt   DECIMAL(18, 2) SUM     COMMENT '下单金额',
  pay_cnt     BIGINT        SUM      COMMENT '支付笔数，来源 ods_payment 成功支付',
  pay_amt     DECIMAL(18, 2) SUM     COMMENT '支付金额（本项目 GMV 默认口径）',
  refund_cnt  BIGINT        SUM      COMMENT '退款笔数，来源 ods_refund 成功退款',
  refund_amt  DECIMAL(18, 2) SUM     COMMENT '退款金额，按退款发生时间归因'
) AGGREGATE KEY(dt, stat_minute)
PARTITION BY RANGE(dt) ()
DISTRIBUTED BY HASH(stat_minute) BUCKETS 4
PROPERTIES (
  'replication_num' = '1',
  'dynamic_partition.enable' = 'true',
  'dynamic_partition.time_unit' = 'DAY',
  'dynamic_partition.start' = '-90',
  'dynamic_partition.end' = '3',
  'dynamic_partition.prefix' = 'p',
  'dynamic_partition.buckets' = '4'
);

-- --------------------------------------------------------- 店铺小时级

CREATE TABLE IF NOT EXISTS dw.dws_trade_shop_1h (
  dt         DATE          NOT NULL COMMENT '统计日期，按事件时间',
  stat_hour  DATETIME(3)   NOT NULL COMMENT '小时起点（事件时间截断到小时）',
  shop_id    BIGINT        NOT NULL COMMENT '店铺ID；订单流直取，支付/退款流经 DWD Lookup 补齐，查不到兜底 -1',
  province   VARCHAR(32)   REPLACE_IF_NOT_NULL COMMENT '店铺省份，自 dim_shop 退化',
  city       VARCHAR(32)   REPLACE_IF_NOT_NULL COMMENT '店铺城市，自 dim_shop 退化',
  order_cnt  BIGINT        SUM      COMMENT '下单笔数',
  order_amt  DECIMAL(18, 2) SUM     COMMENT '下单金额',
  pay_cnt    BIGINT        SUM      COMMENT '支付笔数',
  pay_amt    DECIMAL(18, 2) SUM     COMMENT '支付金额',
  refund_cnt BIGINT        SUM      COMMENT '退款笔数',
  refund_amt DECIMAL(18, 2) SUM     COMMENT '退款金额'
) AGGREGATE KEY(dt, stat_hour, shop_id)
PARTITION BY RANGE(dt) ()
DISTRIBUTED BY HASH(shop_id) BUCKETS 4
PROPERTIES (
  'replication_num' = '1',
  'dynamic_partition.enable' = 'true',
  'dynamic_partition.time_unit' = 'DAY',
  'dynamic_partition.start' = '-90',
  'dynamic_partition.end' = '3',
  'dynamic_partition.prefix' = 'p',
  'dynamic_partition.buckets' = '4'
);

-- --------------------------------------------------------- 商品小时级

CREATE TABLE IF NOT EXISTS dw.dws_trade_product_1h (
  dt            DATE          NOT NULL COMMENT '统计日期，按下单时间',
  stat_hour     DATETIME(3)   NOT NULL COMMENT '小时起点；下单指标按下单时间，支付指标按支付时间',
  product_id    BIGINT        NOT NULL COMMENT '商品ID，来源 ods_order_detail',
  product_name  VARCHAR(128)  REPLACE_IF_NOT_NULL COMMENT '商品名，自 dim_product 最新快照退化',
  category_id   BIGINT        REPLACE_IF_NOT_NULL COMMENT '一级类目ID，自 dim_product',
  category_name VARCHAR(32)   REPLACE_IF_NOT_NULL COMMENT '一级类目名，自 dim_product',
  brand         VARCHAR(32)   REPLACE_IF_NOT_NULL COMMENT '品牌，自 dim_product',
  order_cnt     BIGINT        SUM      COMMENT '下单明细行数（一笔订单含多件商品时为多行）',
  item_cnt      BIGINT        SUM      COMMENT '下单件数 SUM(num)',
  order_amt     DECIMAL(18, 2) SUM     COMMENT '下单金额 SUM(amount)',
  pay_cnt       BIGINT        SUM      COMMENT '支付明细行数',
  pay_amt       DECIMAL(18, 2) SUM     COMMENT '支付金额：已支付订单的明细金额合计'
) AGGREGATE KEY(dt, stat_hour, product_id)
PARTITION BY RANGE(dt) ()
DISTRIBUTED BY HASH(product_id) BUCKETS 4
PROPERTIES (
  'replication_num' = '1',
  'dynamic_partition.enable' = 'true',
  'dynamic_partition.time_unit' = 'DAY',
  'dynamic_partition.start' = '-90',
  'dynamic_partition.end' = '3',
  'dynamic_partition.prefix' = 'p',
  'dynamic_partition.buckets' = '4'
);
