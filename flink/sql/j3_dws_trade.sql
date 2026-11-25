-- =============================================================================
-- 用途：J3（dws_trade）—— 把交易事件聚合成三张 DWS 表：
--         dws_trade_1min        全局分钟级：下单/支付/退款笔数与金额
--         dws_trade_shop_1h     店铺小时级：同上，按 shop_id 切分
--         dws_trade_product_1h  商品小时级：明细流 + 支付流，算商品粒度下单/支付金额
-- 对应文档：docs/04-架构设计.md 4.2 J3、docs/03-数据模型.md 二节/七节/九节、
--           docs/02-指标体系.md 四/五节
-- 执行：bash scripts/run_j3.sh（提交前先停掉旧版 J3 作业，避免双写）
-- 说明：
--   1. 为什么读 Kafka 的 ODS，而不是"流式消费 DWD"：Doris 不是消息队列。
--      Flink 的 Doris connector 只提供 Lookup（点查）和批量 Source，没有 changelog
--      流；DWD 的更新不会主动推给 Flink。所以 J3 的输入是"ODS 事件流 + DWD/VIM
--      Lookup"：分钟/店铺粒度的度量直接来自 ods_order / ods_payment / ods_refund，
--      订单→店铺的归属在支付/退款流里没有，用 Lookup dwd_trade_order_detail 补。
--      理由与偏差记在 docs/08（D21）。
--   2. 为什么用 format='json' 读信封而不是 format='debezium-json'：需要显式过滤
--      op。debezium-json 把消息解析成 changelog 流，UPDATE 会变成 -U/+U，
--      聚合模型（Aggregate）无法回退；改成按 JSON 信封读 after 字段，只保留
--      op='c'（新增）或指定状态的事件，输出是纯 append，Doris 侧只做累加。
--   3. 每个 topic 出现几次就建几张源表、给不同的 group.id：同一个 group.id 的
--      两个 KafkaSource 会组成同一个消费组、把分区"分掉"，每条流只能看到一部分
--      数据（不是重复消费，是漏数据）。
--   4. 商品粒度的支付金额：明细流只有下单事件，拿不到"是否支付"，所以
--      ods_order_detail 与 ods_payment 做 interval join（下单后 1 小时内支付），
--      再把明细金额记到商品头上。订单金额 = 明细金额合计（docs/05 造数口径），
--      所以直接用明细 amount 当支付金额；退款不摊到商品（退款是订单级）。
--   5. 维度列（province/city/category/brand）在 Doris 侧是 REPLACE_IF_NOT_NULL，
--      查不到维表时写 NULL 不会冲掉已写好的值（实测见 docs/08 E21）。
--   6. sink.label-prefix 8 个各不相同，且每次重新提交都要换版本号：
--      Doris 的 Stream Load label 全局唯一（docs/08 E19）。
--   7. 10s checkpoint：Doris sink 开 2PC，数据在 checkpoint 完成时才提交，
--      插入后约 10~12 秒能在 Doris 查到。
--   8. 并行度 3：订单类 topic 只有 3 个分区（docs/04 4.1）。
-- =============================================================================

SET 'pipeline.name' = 'J3-dws-trade';
SET 'execution.checkpointing.interval' = '10s';
SET 'parallelism.default' = '3';
SET 'table.dml-sync' = 'false';
-- 3 个分区里可能有空分区，不设空闲超时水位线会一直卡在最小值，interval join 不出结果
SET 'table.exec.source.idle-timeout' = '30s';

-- ======================================================= 源：ods_order（订单）
-- 用两次（分钟级、店铺级），各建一张表、各用独立 group.id（见文件头第 3 条）

CREATE TABLE ods_order_1min (
  `after` ROW<
    order_id     BIGINT,
    user_id      BIGINT,
    shop_id      BIGINT,
    order_amount DECIMAL(12, 2),
    item_cnt     INT,
    order_status SMALLINT,
    is_deleted   TINYINT,
    create_time  TIMESTAMP(3),
    update_time  TIMESTAMP(3)
  >,
  `op` STRING,
  order_id     AS `after`.order_id,
  shop_id      AS `after`.shop_id,
  order_amount AS `after`.order_amount,
  create_time  AS `after`.create_time,
  proc_time    AS PROCTIME()
) WITH (
  'connector' = 'kafka',
  'topic' = 'ods_order',
  'properties.bootstrap.servers' = '127.0.0.1:9092',
  'properties.group.id' = 'j3_order_1min',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

CREATE TABLE ods_order_shop (
  `after` ROW<
    order_id     BIGINT,
    user_id      BIGINT,
    shop_id      BIGINT,
    order_amount DECIMAL(12, 2),
    item_cnt     INT,
    order_status SMALLINT,
    is_deleted   TINYINT,
    create_time  TIMESTAMP(3),
    update_time  TIMESTAMP(3)
  >,
  `op` STRING,
  order_id     AS `after`.order_id,
  shop_id      AS `after`.shop_id,
  order_amount AS `after`.order_amount,
  create_time  AS `after`.create_time,
  proc_time    AS PROCTIME()
) WITH (
  'connector' = 'kafka',
  'topic' = 'ods_order',
  'properties.bootstrap.servers' = '127.0.0.1:9092',
  'properties.group.id' = 'j3_order_shop',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

-- ===================================================== 源：ods_payment（支付）
-- 用三次（分钟级、店铺级、商品级），各用独立 group.id

CREATE TABLE ods_payment_1min (
  `after` ROW<
    pay_id      BIGINT,
    order_id    BIGINT,
    pay_amount  DECIMAL(12, 2),
    pay_channel STRING,
    pay_status  TINYINT,
    pay_time    TIMESTAMP(3),
    update_time TIMESTAMP(3)
  >,
  `op` STRING,
  order_id   AS `after`.order_id,
  pay_amount AS `after`.pay_amount,
  pay_status AS `after`.pay_status,
  pay_time   AS `after`.pay_time,
  proc_time  AS PROCTIME()
) WITH (
  'connector' = 'kafka',
  'topic' = 'ods_payment',
  'properties.bootstrap.servers' = '127.0.0.1:9092',
  'properties.group.id' = 'j3_pay_1min',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

CREATE TABLE ods_payment_shop (
  `after` ROW<
    pay_id      BIGINT,
    order_id    BIGINT,
    pay_amount  DECIMAL(12, 2),
    pay_channel STRING,
    pay_status  TINYINT,
    pay_time    TIMESTAMP(3),
    update_time TIMESTAMP(3)
  >,
  `op` STRING,
  order_id   AS `after`.order_id,
  pay_amount AS `after`.pay_amount,
  pay_status AS `after`.pay_status,
  pay_time   AS `after`.pay_time,
  proc_time  AS PROCTIME()
) WITH (
  'connector' = 'kafka',
  'topic' = 'ods_payment',
  'properties.bootstrap.servers' = '127.0.0.1:9092',
  'properties.group.id' = 'j3_pay_shop',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

CREATE TABLE ods_payment_product (
  `after` ROW<
    pay_id      BIGINT,
    order_id    BIGINT,
    pay_amount  DECIMAL(12, 2),
    pay_channel STRING,
    pay_status  TINYINT,
    pay_time    TIMESTAMP(3),
    update_time TIMESTAMP(3)
  >,
  `op` STRING,
  order_id   AS `after`.order_id,
  pay_amount AS `after`.pay_amount,
  pay_status AS `after`.pay_status,
  pay_time   AS `after`.pay_time,
  -- interval join 要求两侧都有事件时间属性
  WATERMARK FOR pay_time AS pay_time - INTERVAL '10' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'ods_payment',
  'properties.bootstrap.servers' = '127.0.0.1:9092',
  'properties.group.id' = 'j3_pay_product',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

-- ====================================================== 源：ods_refund（退款）
-- 用两次（分钟级、店铺级）

CREATE TABLE ods_refund_1min (
  `after` ROW<
    refund_id     BIGINT,
    order_id      BIGINT,
    refund_amount DECIMAL(12, 2),
    refund_status TINYINT,
    refund_time   TIMESTAMP(3),
    update_time   TIMESTAMP(3)
  >,
  `op` STRING,
  order_id      AS `after`.order_id,
  refund_amount AS `after`.refund_amount,
  refund_status AS `after`.refund_status,
  refund_time   AS `after`.refund_time,
  proc_time     AS PROCTIME()
) WITH (
  'connector' = 'kafka',
  'topic' = 'ods_refund',
  'properties.bootstrap.servers' = '127.0.0.1:9092',
  'properties.group.id' = 'j3_refund_1min',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

CREATE TABLE ods_refund_shop (
  `after` ROW<
    refund_id     BIGINT,
    order_id      BIGINT,
    refund_amount DECIMAL(12, 2),
    refund_status TINYINT,
    refund_time   TIMESTAMP(3),
    update_time   TIMESTAMP(3)
  >,
  `op` STRING,
  order_id      AS `after`.order_id,
  refund_amount AS `after`.refund_amount,
  refund_status AS `after`.refund_status,
  refund_time   AS `after`.refund_time,
  proc_time     AS PROCTIME()
) WITH (
  'connector' = 'kafka',
  'topic' = 'ods_refund',
  'properties.bootstrap.servers' = '127.0.0.1:9092',
  'properties.group.id' = 'j3_refund_shop',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

-- ============================================ 源：ods_order_detail（订单明细）
-- 用两次：商品级下单度量、商品级支付度量（与支付流 interval join）

CREATE TABLE ods_detail_order (
  `after` ROW<
    detail_id   BIGINT,
    order_id    BIGINT,
    product_id  BIGINT,
    num         INT,
    price       DECIMAL(10, 2),
    amount      DECIMAL(12, 2),
    create_time TIMESTAMP(3)
  >,
  `op` STRING,
  order_id    AS `after`.order_id,
  product_id  AS `after`.product_id,
  num         AS `after`.num,
  amount      AS `after`.amount,
  create_time AS `after`.create_time,
  proc_time   AS PROCTIME()
) WITH (
  'connector' = 'kafka',
  'topic' = 'ods_order_detail',
  'properties.bootstrap.servers' = '127.0.0.1:9092',
  'properties.group.id' = 'j3_detail_order',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

CREATE TABLE ods_detail_pay (
  `after` ROW<
    detail_id   BIGINT,
    order_id    BIGINT,
    product_id  BIGINT,
    num         INT,
    price       DECIMAL(10, 2),
    amount      DECIMAL(12, 2),
    create_time TIMESTAMP(3)
  >,
  `op` STRING,
  order_id    AS `after`.order_id,
  product_id  AS `after`.product_id,
  num         AS `after`.num,
  amount      AS `after`.amount,
  create_time AS `after`.create_time,
  proc_time   AS PROCTIME(),
  WATERMARK FOR create_time AS create_time - INTERVAL '10' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'ods_order_detail',
  'properties.bootstrap.servers' = '127.0.0.1:9092',
  'properties.group.id' = 'j3_detail_pay',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

-- ======================================================== 维表（Doris Lookup）

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
  'jdbc-url' = 'jdbc:mysql://127.0.0.1:9030/dw',
  'lookup.cache.max-rows' = '5000',
  'lookup.cache.ttl' = '10min'
);

CREATE TABLE dim_product_doris (
  product_id    BIGINT,
  product_name  STRING,
  category_id   BIGINT,
  category_name STRING,
  brand         STRING
) WITH (
  'connector' = 'doris',
  'fenodes' = '127.0.0.1:8030',
  'table.identifier' = 'dw.dim_product',
  'username' = 'root',
  'password' = '',
  'jdbc-url' = 'jdbc:mysql://127.0.0.1:9030/dw',
  'lookup.cache.max-rows' = '60000',
  'lookup.cache.ttl' = '30min'
);

-- 支付/退款事件里没有 shop_id，用 DWD 的订单行补
CREATE TABLE dwd_order_doris (
  order_id BIGINT,
  shop_id  BIGINT,
  province STRING,
  city     STRING
) WITH (
  'connector' = 'doris',
  'fenodes' = '127.0.0.1:8030',
  'table.identifier' = 'dw.dwd_trade_order_detail',
  'username' = 'root',
  'password' = '',
  'jdbc-url' = 'jdbc:mysql://127.0.0.1:9030/dw',
  'lookup.cache.max-rows' = '20000',
  'lookup.cache.ttl' = '10min'
);

-- ================================================== 目标：dws_trade_1min（3 个）

CREATE TABLE dws_1min_order_sink (
  dt          DATE,
  stat_minute TIMESTAMP(3),
  order_cnt   BIGINT,
  order_amt   DECIMAL(18, 2)
) WITH (
  'connector' = 'doris',
  'fenodes' = '127.0.0.1:8030',
  'table.identifier' = 'dw.dws_trade_1min',
  'username' = 'root',
  'password' = '',
  'sink.label-prefix' = 'j3_1min_order_v1',
  'sink.enable-2pc' = 'true',
  'sink.properties.format' = 'json',
  'sink.properties.read_json_by_line' = 'true',
  'sink.properties.columns' = 'dt,stat_minute,order_cnt,order_amt'
);

CREATE TABLE dws_1min_pay_sink (
  dt          DATE,
  stat_minute TIMESTAMP(3),
  pay_cnt     BIGINT,
  pay_amt     DECIMAL(18, 2)
) WITH (
  'connector' = 'doris',
  'fenodes' = '127.0.0.1:8030',
  'table.identifier' = 'dw.dws_trade_1min',
  'username' = 'root',
  'password' = '',
  'sink.label-prefix' = 'j3_1min_pay_v1',
  'sink.enable-2pc' = 'true',
  'sink.properties.format' = 'json',
  'sink.properties.read_json_by_line' = 'true',
  'sink.properties.columns' = 'dt,stat_minute,pay_cnt,pay_amt'
);

CREATE TABLE dws_1min_refund_sink (
  dt          DATE,
  stat_minute TIMESTAMP(3),
  refund_cnt  BIGINT,
  refund_amt  DECIMAL(18, 2)
) WITH (
  'connector' = 'doris',
  'fenodes' = '127.0.0.1:8030',
  'table.identifier' = 'dw.dws_trade_1min',
  'username' = 'root',
  'password' = '',
  'sink.label-prefix' = 'j3_1min_refund_v1',
  'sink.enable-2pc' = 'true',
  'sink.properties.format' = 'json',
  'sink.properties.read_json_by_line' = 'true',
  'sink.properties.columns' = 'dt,stat_minute,refund_cnt,refund_amt'
);

-- =============================================== 目标：dws_trade_shop_1h（3 个）

CREATE TABLE dws_shop_order_sink (
  dt        DATE,
  stat_hour TIMESTAMP(3),
  shop_id   BIGINT,
  province  STRING,
  city      STRING,
  order_cnt BIGINT,
  order_amt DECIMAL(18, 2)
) WITH (
  'connector' = 'doris',
  'fenodes' = '127.0.0.1:8030',
  'table.identifier' = 'dw.dws_trade_shop_1h',
  'username' = 'root',
  'password' = '',
  'sink.label-prefix' = 'j3_shop_order_v1',
  'sink.enable-2pc' = 'true',
  'sink.properties.format' = 'json',
  'sink.properties.read_json_by_line' = 'true',
  'sink.properties.columns' = 'dt,stat_hour,shop_id,province,city,order_cnt,order_amt'
);

CREATE TABLE dws_shop_pay_sink (
  dt        DATE,
  stat_hour TIMESTAMP(3),
  shop_id   BIGINT,
  province  STRING,
  city      STRING,
  pay_cnt   BIGINT,
  pay_amt   DECIMAL(18, 2)
) WITH (
  'connector' = 'doris',
  'fenodes' = '127.0.0.1:8030',
  'table.identifier' = 'dw.dws_trade_shop_1h',
  'username' = 'root',
  'password' = '',
  'sink.label-prefix' = 'j3_shop_pay_v1',
  'sink.enable-2pc' = 'true',
  'sink.properties.format' = 'json',
  'sink.properties.read_json_by_line' = 'true',
  'sink.properties.columns' = 'dt,stat_hour,shop_id,province,city,pay_cnt,pay_amt'
);

CREATE TABLE dws_shop_refund_sink (
  dt         DATE,
  stat_hour  TIMESTAMP(3),
  shop_id    BIGINT,
  province   STRING,
  city       STRING,
  refund_cnt BIGINT,
  refund_amt DECIMAL(18, 2)
) WITH (
  'connector' = 'doris',
  'fenodes' = '127.0.0.1:8030',
  'table.identifier' = 'dw.dws_trade_shop_1h',
  'username' = 'root',
  'password' = '',
  'sink.label-prefix' = 'j3_shop_refund_v1',
  'sink.enable-2pc' = 'true',
  'sink.properties.format' = 'json',
  'sink.properties.read_json_by_line' = 'true',
  'sink.properties.columns' = 'dt,stat_hour,shop_id,province,city,refund_cnt,refund_amt'
);

-- ============================================ 目标：dws_trade_product_1h（2 个）

CREATE TABLE dws_product_order_sink (
  dt            DATE,
  stat_hour     TIMESTAMP(3),
  product_id    BIGINT,
  product_name  STRING,
  category_id   BIGINT,
  category_name STRING,
  brand         STRING,
  order_cnt     BIGINT,
  item_cnt      BIGINT,
  order_amt     DECIMAL(18, 2)
) WITH (
  'connector' = 'doris',
  'fenodes' = '127.0.0.1:8030',
  'table.identifier' = 'dw.dws_trade_product_1h',
  'username' = 'root',
  'password' = '',
  'sink.label-prefix' = 'j3_product_order_v1',
  'sink.enable-2pc' = 'true',
  'sink.properties.format' = 'json',
  'sink.properties.read_json_by_line' = 'true',
  'sink.properties.columns' = 'dt,stat_hour,product_id,product_name,category_id,category_name,brand,order_cnt,item_cnt,order_amt'
);

CREATE TABLE dws_product_pay_sink (
  dt            DATE,
  stat_hour     TIMESTAMP(3),
  product_id    BIGINT,
  product_name  STRING,
  category_id   BIGINT,
  category_name STRING,
  brand         STRING,
  pay_cnt       BIGINT,
  pay_amt       DECIMAL(18, 2)
) WITH (
  'connector' = 'doris',
  'fenodes' = '127.0.0.1:8030',
  'table.identifier' = 'dw.dws_trade_product_1h',
  'username' = 'root',
  'password' = '',
  'sink.label-prefix' = 'j3_product_pay_v1',
  'sink.enable-2pc' = 'true',
  'sink.properties.format' = 'json',
  'sink.properties.read_json_by_line' = 'true',
  'sink.properties.columns' = 'dt,stat_hour,product_id,product_name,category_id,category_name,brand,pay_cnt,pay_amt'
);

-- ==================================================================== 提交

EXECUTE STATEMENT SET
BEGIN

-- 1) 分钟级：下单（只取 op='c'，一笔订单只计一次；状态更新事件被过滤）
INSERT INTO dws_1min_order_sink
SELECT
  CAST(create_time AS DATE)                                        AS dt,
  CAST(DATE_FORMAT(create_time, 'yyyy-MM-dd HH:mm:00') AS TIMESTAMP(3)) AS stat_minute,
  1                                                                AS order_cnt,
  order_amount                                                     AS order_amt
FROM ods_order_1min AS o
WHERE o.`op` = 'c' AND o.order_id IS NOT NULL;

-- 2) 分钟级：支付（成功支付，按 pay_time 归分钟）
INSERT INTO dws_1min_pay_sink
SELECT
  CAST(pay_time AS DATE)                                           AS dt,
  CAST(DATE_FORMAT(pay_time, 'yyyy-MM-dd HH:mm:00') AS TIMESTAMP(3)) AS stat_minute,
  1                                                                AS pay_cnt,
  pay_amount                                                       AS pay_amt
FROM ods_payment_1min AS p
WHERE p.`op` = 'c' AND p.order_id IS NOT NULL AND p.pay_status = 1;

-- 3) 分钟级：退款（成功退款，按 refund_time 归分钟）
--    退款状态机是 0申请 → 2处理中 → 1成功/3拒绝，所以成功事件可能是 c 也可能是 u；
--    生成器只造 2→1 与 2→3 两条路径，状态到 1 只发生一次，不会重复计数。
INSERT INTO dws_1min_refund_sink
SELECT
  CAST(refund_time AS DATE)                                        AS dt,
  CAST(DATE_FORMAT(refund_time, 'yyyy-MM-dd HH:mm:00') AS TIMESTAMP(3)) AS stat_minute,
  1                                                                AS refund_cnt,
  refund_amount                                                    AS refund_amt
FROM ods_refund_1min AS r
WHERE r.`op` IN ('c', 'u') AND r.order_id IS NOT NULL AND r.refund_status = 1;

-- 4) 店铺小时级：下单（省市 Lookup dim_shop）
INSERT INTO dws_shop_order_sink
SELECT
  CAST(o.create_time AS DATE)                                      AS dt,
  CAST(DATE_FORMAT(o.create_time, 'yyyy-MM-dd HH:00:00') AS TIMESTAMP(3)) AS stat_hour,
  o.shop_id,
  s.province,
  s.city,
  1                                                                AS order_cnt,
  o.order_amount                                                   AS order_amt
FROM ods_order_shop AS o
LEFT JOIN dim_shop_doris FOR SYSTEM_TIME AS OF o.proc_time AS s
  ON o.shop_id = s.shop_id
WHERE o.`op` = 'c' AND o.order_id IS NOT NULL;

-- 5) 店铺小时级：支付（店归属走 DWD Lookup；查不到兜底 shop_id=-1，不丢数）
INSERT INTO dws_shop_pay_sink
SELECT
  CAST(p.pay_time AS DATE)                                         AS dt,
  CAST(DATE_FORMAT(p.pay_time, 'yyyy-MM-dd HH:00:00') AS TIMESTAMP(3)) AS stat_hour,
  COALESCE(d.shop_id, -1)                                          AS shop_id,
  d.province,
  d.city,
  1                                                                AS pay_cnt,
  p.pay_amount                                                     AS pay_amt
FROM ods_payment_shop AS p
LEFT JOIN dwd_order_doris FOR SYSTEM_TIME AS OF p.proc_time AS d
  ON p.order_id = d.order_id
WHERE p.`op` = 'c' AND p.order_id IS NOT NULL AND p.pay_status = 1;

-- 6) 店铺小时级：退款
INSERT INTO dws_shop_refund_sink
SELECT
  CAST(r.refund_time AS DATE)                                      AS dt,
  CAST(DATE_FORMAT(r.refund_time, 'yyyy-MM-dd HH:00:00') AS TIMESTAMP(3)) AS stat_hour,
  COALESCE(d.shop_id, -1)                                          AS shop_id,
  d.province,
  d.city,
  1                                                                AS refund_cnt,
  r.refund_amount                                                  AS refund_amt
FROM ods_refund_shop AS r
LEFT JOIN dwd_order_doris FOR SYSTEM_TIME AS OF r.proc_time AS d
  ON r.order_id = d.order_id
WHERE r.`op` IN ('c', 'u') AND r.order_id IS NOT NULL AND r.refund_status = 1;

-- 7) 商品小时级：下单（明细流直接累加，维度 Lookup dim_product）
INSERT INTO dws_product_order_sink
SELECT
  CAST(d.create_time AS DATE)                                      AS dt,
  CAST(DATE_FORMAT(d.create_time, 'yyyy-MM-dd HH:00:00') AS TIMESTAMP(3)) AS stat_hour,
  d.product_id,
  dp.product_name,
  dp.category_id,
  dp.category_name,
  dp.brand,
  1                                                                AS order_cnt,
  CAST(d.num AS BIGINT)                                            AS item_cnt,
  d.amount                                                         AS order_amt
FROM ods_detail_order AS d
LEFT JOIN dim_product_doris FOR SYSTEM_TIME AS OF d.proc_time AS dp
  ON d.product_id = dp.product_id
WHERE d.`op` = 'c' AND d.product_id IS NOT NULL;

-- 8) 商品小时级：支付（明细 ⋈ 支付，下单后 1 小时内支付才匹配；金额按明细记账）
INSERT INTO dws_product_pay_sink
SELECT
  CAST(p.pay_time AS DATE)                                         AS dt,
  CAST(DATE_FORMAT(p.pay_time, 'yyyy-MM-dd HH:00:00') AS TIMESTAMP(3)) AS stat_hour,
  d.product_id,
  dp.product_name,
  dp.category_id,
  dp.category_name,
  dp.brand,
  1                                                                AS pay_cnt,
  d.amount                                                         AS pay_amt
FROM ods_detail_pay AS d
JOIN ods_payment_product AS p
  ON d.order_id = p.order_id
 AND p.pay_time BETWEEN d.create_time AND d.create_time + INTERVAL '1' HOUR
LEFT JOIN dim_product_doris FOR SYSTEM_TIME AS OF d.proc_time AS dp
  ON d.product_id = dp.product_id
WHERE d.`op` = 'c' AND p.`op` = 'c' AND p.pay_status = 1 AND d.product_id IS NOT NULL;

END;
