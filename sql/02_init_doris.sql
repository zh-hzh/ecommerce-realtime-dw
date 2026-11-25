-- =============================================================================
-- 用途：Doris 侧建库建表 —— J2 的目标表 dwd_trade_order_detail（DWD 累积快照事实表）
-- 对应文档：docs/03-数据模型.md 5.1 / docs/04-架构设计.md 4.2
-- 执行：bash scripts/init_doris.sh
-- 说明：
--   1. 单 BE 环境，replication_num 必须显式写 1；不写默认 3，建表直接报
--      "replication num should be less than the number of available backends"
--   2. Doris 限制分区列必须是 key 列，所以 UNIQUE KEY 是 (order_id, dt)。
--      dt 取 create_time 的日期，一个订单永远落同一个分区，不影响订单级唯一。
--   3. function_column.sequence_col = update_time：同一 order_id 的乱序/重放写入
--      按 update_time 取大者生效，对应 docs/03 5.1 的"按 update_time 幂等覆盖"。
--   4. MoW（merge-on-write）让 Unique Key 具备点查能力，DWS/ADS 查询才不用回表合并。
--   5. 动态分区 start=-90 / create_history_partition=true：首建时补齐 90 天历史分区，
--      避免"昨天下的单今天才支付"这类迟到事件写不进去。
-- =============================================================================

CREATE DATABASE IF NOT EXISTS dw;

CREATE TABLE IF NOT EXISTS dw.dwd_trade_order_detail (
  order_id      BIGINT        NOT NULL COMMENT '订单ID',
  dt            DATE          NOT NULL COMMENT '下单日期，分区键',
  user_id       BIGINT                 COMMENT '用户ID',
  shop_id       BIGINT                 COMMENT '店铺ID',
  province      VARCHAR(32)            COMMENT '店铺所在省，自 dim_shop 退化',
  city          VARCHAR(32)            COMMENT '店铺所在市，自 dim_shop 退化',
  order_amount  DECIMAL(12, 2)         COMMENT '下单金额',
  item_cnt      INT                    COMMENT '商品件数',
  pay_id        BIGINT                 COMMENT '最近一次支付流水号',
  pay_amount    DECIMAL(12, 2)         COMMENT '支付金额',
  refund_amount DECIMAL(12, 2)         COMMENT '退款金额',
  create_time   DATETIME(3)            COMMENT '下单时间',
  pay_time      DATETIME(3)            COMMENT '支付时间，未发生为 NULL',
  ship_time     DATETIME(3)            COMMENT '发货时间，未发生为 NULL',
  receive_time  DATETIME(3)            COMMENT '签收时间，未发生为 NULL',
  finish_time   DATETIME(3)            COMMENT '完成时间，未发生为 NULL',
  refund_time   DATETIME(3)            COMMENT '退款时间，未发生为 NULL',
  order_status  SMALLINT               COMMENT '订单状态 100待支付 200已支付 300已发货 400已签收 500已完成 600已取消',
  pay_status    TINYINT                COMMENT '支付状态',
  refund_status TINYINT                COMMENT '退款状态',
  is_deleted    TINYINT                COMMENT '逻辑删除 0否 1是',
  update_time   DATETIME(3)            COMMENT '源端更新时间，sequence 列'
) UNIQUE KEY(order_id, dt)
PARTITION BY RANGE(dt) ()
DISTRIBUTED BY HASH(order_id) BUCKETS 8
PROPERTIES (
  'replication_num' = '1',
  'enable_unique_key_merge_on_write' = 'true',
  'function_column.sequence_col' = 'update_time',
  'dynamic_partition.enable' = 'true',
  'dynamic_partition.time_unit' = 'DAY',
  'dynamic_partition.start' = '-90',
  'dynamic_partition.end' = '3',
  'dynamic_partition.prefix' = 'p',
  'dynamic_partition.buckets' = '8',
  'dynamic_partition.create_history_partition' = 'true',
  'dynamic_partition.replication_allocation' = 'tag.location.default: 1'
);
