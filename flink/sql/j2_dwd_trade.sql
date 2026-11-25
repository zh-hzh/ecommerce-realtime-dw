-- =============================================================================
-- 用途：J2 第二版 —— ods_order 清洗后，Lookup Join 店铺维表补齐 province/city，
--       写入 Doris 的 dwd_trade_order_detail
-- 对应文档：docs/04-架构设计.md 4.2（J2 dwd_trade）、docs/03-数据模型.md 5.1 / 6.2
-- 执行：bash scripts/run_j2.sh（提交前先停掉旧版 J2 作业，避免双写）
-- 说明：
--   1. 维表 dw.dim_shop 只保留最新一份快照（UNIQUE KEY shop_id），装载见
--      scripts/load_dim_shop.sh；2026-11 已灌入 2000 个店铺。
--   2. Lookup Join 按 shop_id 点查维表，单行命中才不会膨胀；因此维表不能按 dt 分区。
--      lookup 缓存 5000 行 / 10 分钟，缓存过期后重新查 Doris。
--   3. payment/refund 的 interval join 仍是后续增量，本版支付/退款字段写 NULL。
--   4. 幂等靠 Doris 侧：Unique Key(order_id, dt) + sequence_col=update_time，
--      Flink 里不做状态去重（docs/03 5.1 约束 1）。
--   5. scan.startup.mode=latest-offset：库里历史单不走实时链路；
--      group offset 续接，重启不丢增量。
--   6. sink.label-prefix 每次重新提交作业都要换（这里是 _v2）：Doris 的 Stream Load
--      label 全局唯一，前缀重复且 checkpoint 编号又从头开始时会报
--      "Label Already Exists and load job finished"，作业直接重启失败。
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
  update_time  TIMESTAMP(3),
  -- 处理时间属性：Lookup Join 必须有一个时间属性列作为 FOR SYSTEM_TIME AS OF 的参数
  proc_time    AS PROCTIME()
) WITH (
  'connector' = 'kafka',
  'topic' = 'ods_order',
  'properties.bootstrap.servers' = '127.0.0.1:9092',
  'properties.group.id' = 'j2_dwd_trade',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'debezium-json',
  'debezium-json.ignore-parse-errors' = 'true'
);

-- ------------------------------------------------------- 维表：dw.dim_shop（Doris）

CREATE TABLE dim_shop_doris (
  shop_id  BIGINT,
  province STRING,
  city     STRING
) WITH (
  'connector' = 'doris',
  'fenodes' = '127.0.0.1:8030',
  'table.identifier' = 'dw.dim_shop',
  'username' = 'root',
  'password' = '',
  -- lookup join 通过 JDBC 访问 Doris FE 的 MySQL 协议端口
  'jdbc-url' = 'jdbc:mysql://127.0.0.1:9030/dw',
  'lookup.cache.max-rows' = '5000',
  'lookup.cache.ttl' = '10min'
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
  'sink.label-prefix' = 'j2_dwd_trade_v2',
  'sink.enable-2pc' = 'true',
  'sink.properties.format' = 'json',
  'sink.properties.read_json_by_line' = 'true'
);

-- ---------------------------------------------------------------- 提交

INSERT INTO doris_dwd_order
SELECT
  o.order_id,
  CAST(o.create_time AS DATE)            AS dt,
  o.user_id,
  o.shop_id,
  s.province,                            -- Lookup Join 补齐；店铺不存在时为 NULL
  s.city,
  o.order_amount,
  o.item_cnt,
  CAST(NULL AS BIGINT)                   AS pay_id,        -- 待 ods_payment 补齐
  CAST(NULL AS DECIMAL(12, 2))           AS pay_amount,    -- 待 ods_payment 补齐
  CAST(NULL AS DECIMAL(12, 2))           AS refund_amount, -- 待 ods_refund 补齐
  o.create_time,
  CAST(NULL AS TIMESTAMP(3))             AS pay_time,      -- 待 ods_payment 补齐
  CAST(NULL AS TIMESTAMP(3))             AS ship_time,     -- 源端不产生，留 NULL
  CAST(NULL AS TIMESTAMP(3))             AS receive_time,  -- 源端不产生，留 NULL
  CAST(NULL AS TIMESTAMP(3))             AS finish_time,   -- 源端不产生，留 NULL
  CAST(NULL AS TIMESTAMP(3))             AS refund_time,   -- 待 ods_refund 补齐
  o.order_status,
  CAST(NULL AS TINYINT)                  AS pay_status,
  CAST(NULL AS TINYINT)                  AS refund_status,
  o.is_deleted,
  o.update_time
FROM ods_order_kafka AS o
LEFT JOIN dim_shop_doris FOR SYSTEM_TIME AS OF o.proc_time AS s
  ON o.shop_id = s.shop_id;
