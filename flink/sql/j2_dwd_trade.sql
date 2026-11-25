-- =============================================================================
-- 用途：J2 第一版 —— ods_order 单表流清洗后写入 Doris 的 dwd_trade_order_detail
-- 对应文档：docs/04-架构设计.md 4.2（J2 dwd_trade）、docs/03-数据模型.md 5.1
-- 执行：bash scripts/run_j2.sh
-- 说明：
--   1. 本版只接 ods_order 一张流，先把"Kafka → Flink → Doris"打通并验证实时可见；
--      payment/refund 的 interval join、dim_shop 的 lookup join 是 J2 后续增量，
--      对应的省份/城市/支付/退款字段本版先写 NULL。
--   2. 幂等靠 Doris 侧：Unique Key(order_id, dt) + sequence_col=update_time，
--      Flink 里不做状态去重，避免状态随订单数膨胀（docs/03 5.1 约束 1）。
--   3. scan.startup.mode=latest-offset：只处理作业启动后的变更，与 J1 同理，
--      库里 90 万历史单不走实时链路；重启时由 group offset 续接。
--   4. dt 取 create_time 的日期（下单口径），保证同一订单固定落一个分区。
-- =============================================================================

SET 'pipeline.name' = 'J2-dwd-trade';
-- 10s checkpoint：Doris sink 开了 2PC，数据在 checkpoint 完成时才提交，
-- 因此 checkpoint 间隔直接决定"插入后多久能在 Doris 查到"
SET 'execution.checkpointing.interval' = '10s';
SET 'parallelism.default' = '3';
-- 流式作业异步提交，同步等待会一直卡住 sql-client（同 J1）
SET 'table.dml-sync' = 'false';

-- ---------------------------------------------------------------- 源：ods_order

CREATE TABLE ods_order_kafka (
  order_id     BIGINT,
  user_id      BIGINT,
  shop_id      BIGINT,
  order_amount DECIMAL(12, 2),
  item_cnt     INT,
  order_status SMALLINT,
  is_deleted   TINYINT,
  create_time  TIMESTAMP(3),
  update_time  TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'ods_order',
  'properties.bootstrap.servers' = '127.0.0.1:9092',
  'properties.group.id' = 'j2_dwd_trade',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'debezium-json',
  'debezium-json.ignore-parse-errors' = 'true'
);

-- ---------------------------------------------------------------- 目标：Doris DWD

CREATE TABLE doris_dwd_order (
  order_id      BIGINT,
  dt            DATE,
  user_id       BIGINT,
  shop_id       BIGINT,
  province      STRING,
  city          STRING,
  order_amount  DECIMAL(12, 2),
  item_cnt      INT,
  pay_id        BIGINT,
  pay_amount    DECIMAL(12, 2),
  refund_amount DECIMAL(12, 2),
  create_time   TIMESTAMP(3),
  pay_time      TIMESTAMP(3),
  ship_time     TIMESTAMP(3),
  receive_time  TIMESTAMP(3),
  finish_time   TIMESTAMP(3),
  refund_time   TIMESTAMP(3),
  order_status  SMALLINT,
  pay_status    TINYINT,
  refund_status TINYINT,
  is_deleted    TINYINT,
  update_time   TIMESTAMP(3)
) WITH (
  'connector' = 'doris',
  'fenodes' = '127.0.0.1:8030',
  'table.identifier' = 'dw.dwd_trade_order_detail',
  'username' = 'root',
  'password' = '',
  'sink.label-prefix' = 'j2_dwd_trade',
  'sink.enable-2pc' = 'true',
  'sink.properties.format' = 'json',
  'sink.properties.read_json_by_line' = 'true'
);

-- ---------------------------------------------------------------- 提交

INSERT INTO doris_dwd_order
SELECT
  order_id,
  CAST(create_time AS DATE)              AS dt,
  user_id,
  shop_id,
  CAST(NULL AS STRING)                   AS province,      -- 待 dim_shop lookup join 补齐
  CAST(NULL AS STRING)                   AS city,          -- 待 dim_shop lookup join 补齐
  order_amount,
  item_cnt,
  CAST(NULL AS BIGINT)                   AS pay_id,        -- 待 ods_payment 补齐
  CAST(NULL AS DECIMAL(12, 2))           AS pay_amount,    -- 待 ods_payment 补齐
  CAST(NULL AS DECIMAL(12, 2))           AS refund_amount, -- 待 ods_refund 补齐
  create_time,
  CAST(NULL AS TIMESTAMP(3))             AS pay_time,      -- 待 ods_payment 补齐
  CAST(NULL AS TIMESTAMP(3))             AS ship_time,     -- 源端不产生，留 NULL
  CAST(NULL AS TIMESTAMP(3))             AS receive_time,  -- 源端不产生，留 NULL
  CAST(NULL AS TIMESTAMP(3))             AS finish_time,   -- 源端不产生，留 NULL
  CAST(NULL AS TIMESTAMP(3))             AS refund_time,   -- 待 ods_refund 补齐
  order_status,
  CAST(NULL AS TINYINT)                  AS pay_status,
  CAST(NULL AS TINYINT)                  AS refund_status,
  is_deleted,
  update_time
FROM ods_order_kafka;
