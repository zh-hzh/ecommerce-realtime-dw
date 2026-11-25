-- 用途：J1 —— 把 MySQL 4 张交易表的 binlog 实时写入 Kafka，作为 ODS 层
-- 对应文档：docs/04-架构设计.md 4.1 / 4.2
-- 执行：bash scripts/run_j1.sh <CDC密码>
-- 说明：
--   - scan.startup.mode=latest-offset：跳过库里已有的 90 万历史单，只抓启动后的变更
--   - key 用 order_id：同一订单的所有变更落同一分区，保序
--   - 4 个 sink 对应 ods_order / ods_order_detail / ods_payment / ods_refund
--   - ods_traffic 不在这里，由 gen_traffic.py 直接写 Kafka

SET 'pipeline.name' = 'J1-cdc2kafka';
SET 'execution.checkpointing.interval' = '30s';
-- dml-sync=false：4 条 INSERT 都是流式作业，同步等待会永远卡住客户端；改为异步提交后由 REST 查状态
SET 'table.dml-sync' = 'false';

-- ---------------------------------------------------------------- 源表

CREATE TABLE mysql_order (
  order_id     BIGINT,
  user_id      BIGINT,
  shop_id      BIGINT,
  order_amount DECIMAL(12, 2),
  item_cnt     INT,
  order_status SMALLINT,
  is_deleted   TINYINT,
  create_time  TIMESTAMP(3),
  update_time  TIMESTAMP(3),
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
  'connector' = 'mysql-cdc',
  'hostname' = '127.0.0.1',
  'port' = '3306',
  'username' = 'cdc',
  'password' = '__CDC_PASSWORD__',
  'database-name' = 'trade',
  'table-name' = 'order_info',
  'scan.startup.mode' = 'latest-offset',
  'server-id' = '5401'
);

CREATE TABLE mysql_order_detail (
  detail_id   BIGINT,
  order_id    BIGINT,
  product_id  BIGINT,
  num         INT,
  price       DECIMAL(10, 2),
  amount      DECIMAL(12, 2),
  create_time TIMESTAMP(3),
  PRIMARY KEY (detail_id) NOT ENFORCED
) WITH (
  'connector' = 'mysql-cdc',
  'hostname' = '127.0.0.1',
  'port' = '3306',
  'username' = 'cdc',
  'password' = '__CDC_PASSWORD__',
  'database-name' = 'trade',
  'table-name' = 'order_detail',
  'scan.startup.mode' = 'latest-offset',
  'server-id' = '5402'
);

CREATE TABLE mysql_payment (
  pay_id      BIGINT,
  order_id    BIGINT,
  pay_amount  DECIMAL(12, 2),
  pay_channel STRING,
  pay_status  TINYINT,
  pay_time    TIMESTAMP(3),
  update_time TIMESTAMP(3),
  PRIMARY KEY (pay_id) NOT ENFORCED
) WITH (
  'connector' = 'mysql-cdc',
  'hostname' = '127.0.0.1',
  'port' = '3306',
  'username' = 'cdc',
  'password' = '__CDC_PASSWORD__',
  'database-name' = 'trade',
  'table-name' = 'payment_info',
  'scan.startup.mode' = 'latest-offset',
  'server-id' = '5403'
);

CREATE TABLE mysql_refund (
  refund_id     BIGINT,
  order_id      BIGINT,
  refund_amount DECIMAL(12, 2),
  refund_status TINYINT,
  refund_reason STRING,
  refund_time   TIMESTAMP(3),
  update_time   TIMESTAMP(3),
  PRIMARY KEY (refund_id) NOT ENFORCED
) WITH (
  'connector' = 'mysql-cdc',
  'hostname' = '127.0.0.1',
  'port' = '3306',
  'username' = 'cdc',
  'password' = '__CDC_PASSWORD__',
  'database-name' = 'trade',
  'table-name' = 'refund_info',
  'scan.startup.mode' = 'latest-offset',
  'server-id' = '5404'
);

-- ---------------------------------------------------------------- ODS sink

CREATE TABLE kafka_order (
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
  'format' = 'debezium-json',
  'key.format' = 'json',
  'key.fields' = 'order_id'
);

CREATE TABLE kafka_order_detail (
  detail_id   BIGINT,
  order_id    BIGINT,
  product_id  BIGINT,
  num         INT,
  price       DECIMAL(10, 2),
  amount      DECIMAL(12, 2),
  create_time TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'ods_order_detail',
  'properties.bootstrap.servers' = '127.0.0.1:9092',
  'format' = 'debezium-json',
  'key.format' = 'json',
  'key.fields' = 'order_id'
);

CREATE TABLE kafka_payment (
  pay_id      BIGINT,
  order_id    BIGINT,
  pay_amount  DECIMAL(12, 2),
  pay_channel STRING,
  pay_status  TINYINT,
  pay_time    TIMESTAMP(3),
  update_time TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'ods_payment',
  'properties.bootstrap.servers' = '127.0.0.1:9092',
  'format' = 'debezium-json',
  'key.format' = 'json',
  'key.fields' = 'order_id'
);

CREATE TABLE kafka_refund (
  refund_id     BIGINT,
  order_id      BIGINT,
  refund_amount DECIMAL(12, 2),
  refund_status TINYINT,
  refund_reason STRING,
  refund_time   TIMESTAMP(3),
  update_time   TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'ods_refund',
  'properties.bootstrap.servers' = '127.0.0.1:9092',
  'format' = 'debezium-json',
  'key.format' = 'json',
  'key.fields' = 'order_id'
);

-- ---------------------------------------------------------------- 提交

INSERT INTO kafka_order SELECT * FROM mysql_order;
INSERT INTO kafka_order_detail SELECT * FROM mysql_order_detail;
INSERT INTO kafka_payment SELECT * FROM mysql_payment;
INSERT INTO kafka_refund SELECT * FROM mysql_refund;
