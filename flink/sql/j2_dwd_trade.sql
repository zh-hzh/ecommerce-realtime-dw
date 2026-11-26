-- =============================================================================
-- 用途：J2 第三版 —— 订单 / 支付 / 退款三条流各写 dwd_trade_order_detail 的一部分列，
--       由 Doris 部分列更新（partial_columns）合并成"一行一单"的累积快照；
--       订单分支再 Lookup Join dw.dim_shop 补齐 province/city
-- 对应文档：docs/04-架构设计.md 4.2（J2 dwd_trade）、docs/03-数据模型.md 5.1 / 6.2
-- 执行：bash scripts/run_j2.sh（提交前先停掉旧版 J2 作业，避免双写）
-- 说明：
--   1. 为什么不是 interval join：订单的里程碑事件（下单/支付/发货/完成/退款）
--      分散在三条流、时间跨度可到 20 天，interval join 的窗口留不住这么长的状态，
--      且订单流单独触发时拿不到支付/退款字段。改成"三条流各写各的列"：
--        订单流 → 订单字段 + 省市（Lookup dim_shop）
--        支付流 → pay_id / pay_amount / pay_status / pay_time
--        退款流 → refund_amount / refund_time / refund_status
--      三组列互不重叠，Doris 侧在同一行上合并，谁先谁后都不影响结果。
--   2. 部分列更新靠三个条件同时成立（已实测，见 docs/08 E20）：
--      Doris 表是 Unique Key + MoW；表选项 partial_columns=true；
--      Stream Load 的 columns 头**只列本次要写的列**。列没列进去就不动，
--      列进去了但 JSON 缺这个 key 会被置 NULL——所以每个 sink 的 Flink DDL
--      只声明自己那几列，并在 sink.properties.columns 里保持一致。
--   3. 退款只写"成功"的流水（refund_status=1）。0申请 / 2处理中 / 3拒绝 不改
--      refund_amount，否则会把没退成的钱算进退款额。源端一单一退款（refund_id=order_id），
--      所以后写的退款覆盖先写的即可，不需要 SUM。
--   4. 幂等靠 Doris 侧：Unique Key(order_id) + MoW 后写覆盖。
--      表上**不能**加 function_column.sequence_col：sequence 是表级属性，
--      三个 sink 在同一 checkpoint 内的提交顺序不保证，支付/退款的部分列更新
--      晚于订单状态更新提交时会因 sequence 更旧被整行丢弃（docs/08 E31）。
--      三组列互不重叠，同一条流的多次写入由同一个 sink 按 checkpoint 顺序提交，
--      后写覆盖即正确结果。
--   5. scan.startup.mode=latest-offset：库里历史单不走实时链路；group offset 续接。
--   6. sink.label-prefix 三个 sink 必须各不相同，且每次重新提交都要换：
--      Doris 的 Stream Load label 全局唯一，前缀+checkpoint 编号重复会报
--      "Label Already Exists and load job finished"（docs/08 E19）。
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

-- ---------------------------------------------------------------- 源：ods_payment

CREATE TABLE ods_payment_kafka (
  pay_id      BIGINT,
  order_id    BIGINT,
  pay_amount  DECIMAL(12, 2),
  pay_status  TINYINT,
  pay_time    TIMESTAMP(3),
  update_time TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'ods_payment',
  'properties.bootstrap.servers' = '127.0.0.1:9092',
  'properties.group.id' = 'j2_dwd_pay',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'debezium-json',
  'debezium-json.ignore-parse-errors' = 'true'
);

-- ---------------------------------------------------------------- 源：ods_refund

CREATE TABLE ods_refund_kafka (
  refund_id     BIGINT,
  order_id      BIGINT,
  refund_amount DECIMAL(12, 2),
  refund_status TINYINT,
  refund_time   TIMESTAMP(3),
  update_time   TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'ods_refund',
  'properties.bootstrap.servers' = '127.0.0.1:9092',
  'properties.group.id' = 'j2_dwd_refund',
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

-- ------------------------------------------------- 目标：Doris DWD（订单分支列）

CREATE TABLE dwd_order_sink (
  order_id     BIGINT,
  dt           DATE,
  user_id      BIGINT,
  shop_id      BIGINT,
  province     STRING,
  city         STRING,
  order_amount DECIMAL(12, 2),
  item_cnt     INT,
  create_time  TIMESTAMP(3),
  order_status SMALLINT,
  is_deleted   TINYINT,
  update_time  TIMESTAMP(3)
) WITH (
  'connector' = 'doris',
  'fenodes' = '127.0.0.1:8030',
  'table.identifier' = 'dw.dwd_trade_order_detail',
  'username' = 'root',
  'password' = '',
  'sink.label-prefix' = 'j2_order_v4',
  'sink.enable-2pc' = 'true',
  'sink.properties.format' = 'json',
  'sink.properties.read_json_by_line' = 'true',
  'sink.properties.partial_columns' = 'true',
  'sink.properties.columns' = 'order_id,dt,user_id,shop_id,province,city,order_amount,item_cnt,create_time,order_status,is_deleted,update_time'
);

-- ------------------------------------------------- 目标：Doris DWD（支付分支列）

CREATE TABLE dwd_payment_sink (
  order_id    BIGINT,
  pay_id      BIGINT,
  pay_amount  DECIMAL(12, 2),
  pay_status  TINYINT,
  pay_time    TIMESTAMP(3),
  update_time TIMESTAMP(3)
) WITH (
  'connector' = 'doris',
  'fenodes' = '127.0.0.1:8030',
  'table.identifier' = 'dw.dwd_trade_order_detail',
  'username' = 'root',
  'password' = '',
  'sink.label-prefix' = 'j2_pay_v4',
  'sink.enable-2pc' = 'true',
  'sink.properties.format' = 'json',
  'sink.properties.read_json_by_line' = 'true',
  'sink.properties.partial_columns' = 'true',
  'sink.properties.columns' = 'order_id,pay_id,pay_amount,pay_status,pay_time,update_time'
);

-- ------------------------------------------------- 目标：Doris DWD（退款分支列）

CREATE TABLE dwd_refund_sink (
  order_id      BIGINT,
  refund_amount DECIMAL(12, 2),
  refund_status TINYINT,
  refund_time   TIMESTAMP(3),
  update_time   TIMESTAMP(3)
) WITH (
  'connector' = 'doris',
  'fenodes' = '127.0.0.1:8030',
  'table.identifier' = 'dw.dwd_trade_order_detail',
  'username' = 'root',
  'password' = '',
  'sink.label-prefix' = 'j2_refund_v4',
  'sink.enable-2pc' = 'true',
  'sink.properties.format' = 'json',
  'sink.properties.read_json_by_line' = 'true',
  'sink.properties.partial_columns' = 'true',
  'sink.properties.columns' = 'order_id,refund_amount,refund_status,refund_time,update_time'
);

-- ---------------------------------------------------------------- 提交（三条流各写各的列）

EXECUTE STATEMENT SET
BEGIN

INSERT INTO dwd_order_sink
SELECT
  o.order_id,
  CAST(o.create_time AS DATE)            AS dt,
  o.user_id,
  o.shop_id,
  s.province,                            -- Lookup Join 补齐；店铺不存在时为 NULL
  s.city,
  o.order_amount,
  o.item_cnt,
  o.create_time,
  o.order_status,
  o.is_deleted,
  o.update_time
FROM ods_order_kafka AS o
LEFT JOIN dim_shop_doris FOR SYSTEM_TIME AS OF o.proc_time AS s
  ON o.shop_id = s.shop_id;

-- 只写成功的支付流水；pay_status=2 失败 / 3 退款中不进 DWD
INSERT INTO dwd_payment_sink
SELECT
  order_id,
  pay_id,
  pay_amount,
  pay_status,
  pay_time,
  update_time
FROM ods_payment_kafka
WHERE order_id IS NOT NULL AND pay_status = 1;

-- 只写成功的退款（0申请 / 2处理中 / 3拒绝 不改金额）
INSERT INTO dwd_refund_sink
SELECT
  order_id,
  refund_amount,
  refund_status,
  refund_time,
  update_time
FROM ods_refund_kafka
WHERE order_id IS NOT NULL AND refund_status = 1;

END;
