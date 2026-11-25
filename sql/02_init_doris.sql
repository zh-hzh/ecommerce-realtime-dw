-- =============================================================================
-- 用途：Doris 侧建库建表 —— J2 的目标表 dwd_trade_order_detail（DWD 累积快照事实表）
-- 对应文档：docs/03-数据模型.md 5.1 / docs/04-架构设计.md 4.2
-- 执行：bash scripts/init_doris.sh
-- 说明：
--   1. 单 BE 环境，replication_num 必须显式写 1；不写默认 3，建表直接报
--      "replication num should be less than the number of available backends"
--   2. key 只有 order_id，表不做分区。J2 第三版由订单/支付/退款三条流各写各的列，
--      靠 Doris 部分列更新（partial_columns，见 flink/sql/j2_dwd_trade.sql）合并到一行；
--      部分列更新要求 key 列齐全，而支付/退款事件里没有 order 的 create_time，
--      拿不到 dt。所以 dt 只能是普通列，不能进 key，也就不能做 RANGE 分区。
--      （历史：E15 记录过"分区列必须是 key 列"，这是同一约束的另一面。）
--   3. function_column.sequence_col = update_time：同一组列的多次写入按 update_time
--      取大者生效；三条流写的列互不重叠，跨流不需要比较先后。
--   4. MoW（merge-on-write）是部分列更新的前提。
--   5. 已有旧结构（key 为 order_id+dt、按 dt 分区）时需要 DROP 重建：
--      DROP TABLE IF EXISTS dw.dwd_trade_order_detail;
-- =============================================================================

CREATE DATABASE IF NOT EXISTS dw;

CREATE TABLE IF NOT EXISTS dw.dwd_trade_order_detail (
  order_id      BIGINT        NOT NULL COMMENT '订单ID',
  dt            DATE                 COMMENT '下单日期，普通列（不分区，见文件头说明）',
  user_id       BIGINT               COMMENT '用户ID',
  shop_id       BIGINT               COMMENT '店铺ID',
  province      VARCHAR(32)          COMMENT '店铺所在省，自 dim_shop 退化',
  city          VARCHAR(32)          COMMENT '店铺所在市，自 dim_shop 退化',
  order_amount  DECIMAL(12, 2)       COMMENT '下单金额',
  item_cnt      INT                  COMMENT '商品件数',
  pay_id        BIGINT               COMMENT '最近一次支付流水号',
  pay_amount    DECIMAL(12, 2)       COMMENT '支付金额',
  refund_amount DECIMAL(12, 2)       COMMENT '退款金额',
  create_time   DATETIME(3)          COMMENT '下单时间',
  pay_time      DATETIME(3)          COMMENT '支付时间，未发生为 NULL',
  ship_time     DATETIME(3)          COMMENT '发货时间，未发生为 NULL',
  receive_time  DATETIME(3)          COMMENT '签收时间，未发生为 NULL',
  finish_time   DATETIME(3)          COMMENT '完成时间，未发生为 NULL',
  refund_time   DATETIME(3)          COMMENT '退款时间，未发生为 NULL',
  order_status  SMALLINT             COMMENT '订单状态 100待支付 200已支付 300已发货 400已签收 500已完成 600已取消',
  pay_status    TINYINT              COMMENT '支付状态',
  refund_status TINYINT              COMMENT '退款状态',
  is_deleted    TINYINT              COMMENT '逻辑删除 0否 1是',
  update_time   DATETIME(3)          COMMENT '源端更新时间，sequence 列'
) UNIQUE KEY(order_id)
DISTRIBUTED BY HASH(order_id) BUCKETS 8
PROPERTIES (
  'replication_num' = '1',
  'enable_unique_key_merge_on_write' = 'true',
  'function_column.sequence_col' = 'update_time'
);
