-- =============================================================================
-- 用途：J4（dwd_traffic）—— 消费 Kafka ods_traffic 埋点流，清洗后分流：
--         合法事件 → dw.dwd_traffic_detail（按 event_id 去重）
--         非法事件 → dw.dwd_dirty_record（Q04 时间越界 / Q05 枚举与字段非法）
-- 对应文档：docs/04-架构设计.md 4.2 J4、docs/03-数据模型.md 5.2、
--           docs/06-数据质量与对账.md 二 / 三节（Q04 / Q05 / Q08）
-- 执行：bash scripts/run_j4.sh（提交前先停掉旧版 J4，避免双写）
-- 说明：
--   1. 输入是埋点生成器直接写的单行 JSON（不是 CDC 信封），字段见 docs/05 第三节；
--      event_time 是 epoch 毫秒，用 TO_TIMESTAMP_LTZ(event_time, 3) 转成事件时间。
--   2. 去重不在这里做：Doris 侧 dwd_traffic_detail 是 Unique Key(event_id, dt) +
--      merge-on-write，同一 event_id 重复发送只会留一行（Q08 只计数、不落脏表）。
--   3. 清洗只拦截两类值本身坏掉的事件（Q04 / Q05）。迟到事件（event_time 比发送
--      时间早几分钟但在 30 天内）是合法数据，只影响实时性，不拦截、不落脏表。
--   4. 同一条 Kafka 消息要被"合法/非法"两条 DML 各判断一次，所以建两张源表、
--      给不同的 group.id。同一 group.id 的两个 KafkaSource 会组成同一消费组把
--      分区"分掉"，每条流只能看到一部分数据（docs/08 E22 同源问题）。
--   5. 时间边界按处理时间判断：event_time > now+5min 或 < now-30d 拦截。
--      CURRENT_TIMESTAMP 在流模式下逐条求值，不会固定在作业启动时刻。
--      脏数据的 dt 取**拦截当天**而不是事件日期：Q04 的历史越界事件可能早于
--      建表日期，按事件日期写会因分区不存在而 load 失败；按天出脏数据率也应以
--      "哪天发现的"为准（docs/06 4.2）。
--   6. sink.label-prefix 两个 sink 各不相同，且每次重新提交都要换版本号：
--      Doris Stream Load label 全局唯一（docs/08 E19）。
--   7. 并行度 6：ods_traffic 是 6 分区（docs/04 4.1），与槽位数刚好匹配。
-- =============================================================================

SET 'pipeline.name' = 'J4-dwd-traffic';
SET 'execution.checkpointing.interval' = '10s';
SET 'parallelism.default' = '6';
SET 'table.dml-sync' = 'false';

-- ============================================== 源：ods_traffic（合法分支）
-- 6 个分区，group.id 与脏数据分支必须不同（见文件头第 4 条）

CREATE TABLE ods_traffic_valid (
  event_id    STRING,
  user_id     BIGINT,
  product_id  BIGINT,
  event_type  STRING,
  event_time  BIGINT,
  session_id  STRING,
  -- epoch 毫秒 → 事件时间；会话时区是 Asia/Shanghai（与 WSL 一致）
  event_time_ts AS TO_TIMESTAMP_LTZ(event_time, 3),
  WATERMARK FOR event_time_ts AS event_time_ts - INTERVAL '30' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'ods_traffic',
  'properties.bootstrap.servers' = '127.0.0.1:9092',
  'properties.group.id' = 'j4_traffic_valid',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

-- ============================================== 源：ods_traffic（脏数据分支）

CREATE TABLE ods_traffic_dirty (
  event_id    STRING,
  user_id     BIGINT,
  product_id  BIGINT,
  event_type  STRING,
  event_time  BIGINT,
  session_id  STRING,
  event_time_ts AS TO_TIMESTAMP_LTZ(event_time, 3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'ods_traffic',
  'properties.bootstrap.servers' = '127.0.0.1:9092',
  'properties.group.id' = 'j4_traffic_dirty',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

-- ================================================== 目标：dwd_traffic_detail

CREATE TABLE dwd_traffic_sink (
  event_id   STRING,
  dt         DATE,
  user_id    BIGINT,
  product_id BIGINT,
  event_type STRING,
  event_time TIMESTAMP(3),
  session_id STRING
) WITH (
  'connector' = 'doris',
  'fenodes' = '127.0.0.1:8030',
  'table.identifier' = 'dw.dwd_traffic_detail',
  'username' = 'root',
  'password' = '',
  'sink.label-prefix' = 'j4_traffic_v1',
  'sink.enable-2pc' = 'true',
  'sink.properties.format' = 'json',
  'sink.properties.read_json_by_line' = 'true',
  'sink.properties.columns' = 'event_id,dt,user_id,product_id,event_type,event_time,session_id'
);

-- ================================================== 目标：dwd_dirty_record

CREATE TABLE dwd_dirty_sink (
  dt           DATE,
  rule_id      STRING,
  source_table STRING,
  biz_key      STRING,
  reason       STRING,
  raw_data     STRING,
  dirty_time   TIMESTAMP(3)
) WITH (
  'connector' = 'doris',
  'fenodes' = '127.0.0.1:8030',
  'table.identifier' = 'dw.dwd_dirty_record',
  'username' = 'root',
  'password' = '',
  'sink.label-prefix' = 'j4_dirty_v1',
  'sink.enable-2pc' = 'true',
  'sink.properties.format' = 'json',
  'sink.properties.read_json_by_line' = 'true',
  'sink.properties.columns' = 'dt,rule_id,source_table,biz_key,reason,raw_data,dirty_time'
);

-- ==================================================================== 提交

EXECUTE STATEMENT SET
BEGIN

-- 1) 合法事件：字段齐全、枚举合法、事件时间在 [now-30d, now+5min] 内
INSERT INTO dwd_traffic_sink
SELECT
  event_id,
  CAST(event_time_ts AS DATE)              AS dt,
  user_id,
  product_id,
  event_type,
  CAST(event_time_ts AS TIMESTAMP(3))      AS event_time,
  session_id
FROM ods_traffic_valid
WHERE event_id IS NOT NULL
  AND user_id IS NOT NULL
  AND event_time_ts IS NOT NULL
  AND event_type IN ('page_view', 'add_cart', 'order_submit')
  AND event_time_ts <= CURRENT_TIMESTAMP + INTERVAL '5' MINUTE
  AND event_time_ts >= CURRENT_TIMESTAMP - INTERVAL '30' DAY;

-- 2) 非法事件：Q04 时间越界优先归类，其余归 Q05（枚举非法 / 关键字段缺失）
INSERT INTO dwd_dirty_sink
SELECT
  CAST(CURRENT_TIMESTAMP AS DATE)                                        AS dt,
  CASE
    WHEN event_time_ts IS NOT NULL
     AND (event_time_ts > CURRENT_TIMESTAMP + INTERVAL '5' MINUTE
       OR event_time_ts < CURRENT_TIMESTAMP - INTERVAL '30' DAY)
    THEN 'Q04'
    ELSE 'Q05'
  END                                                                    AS rule_id,
  'ods_traffic'                                                          AS source_table,
  COALESCE(
    event_id,
    CONCAT('no-id-', COALESCE(CAST(event_time AS STRING), 'null'), '-',
           COALESCE(CAST(user_id AS STRING), 'null'))
  )                                                                      AS biz_key,
  CASE
    WHEN event_time_ts IS NULL
    THEN 'Q05 event_time 缺失或无法解析'
    WHEN event_time_ts > CURRENT_TIMESTAMP + INTERVAL '5' MINUTE
    THEN 'Q04 event_time 超出未来界 now+5min'
    WHEN event_time_ts < CURRENT_TIMESTAMP - INTERVAL '30' DAY
    THEN 'Q04 event_time 超出历史界 now-30d'
    WHEN event_id IS NULL
    THEN 'Q05 event_id 为空'
    WHEN user_id IS NULL
    THEN 'Q05 user_id 为空'
    ELSE CONCAT('Q05 event_type 非法: ', COALESCE(event_type, 'NULL'))
  END                                                                    AS reason,
  CONCAT(
    '{"event_id":"', COALESCE(event_id, ''),
    '","user_id":', COALESCE(CAST(user_id AS STRING), 'null'),
    ',"product_id":', COALESCE(CAST(product_id AS STRING), 'null'),
    ',"event_type":"', COALESCE(event_type, ''),
    '","event_time":', COALESCE(CAST(event_time AS STRING), 'null'),
    ',"session_id":"', COALESCE(session_id, ''), '"}'
  )                                                                      AS raw_data,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3))                                AS dirty_time
FROM ods_traffic_dirty
WHERE event_id IS NULL
   OR user_id IS NULL
   OR event_time_ts IS NULL
   OR event_type IS NULL
   OR event_type NOT IN ('page_view', 'add_cart', 'order_submit')
   OR event_time_ts > CURRENT_TIMESTAMP + INTERVAL '5' MINUTE
   OR event_time_ts < CURRENT_TIMESTAMP - INTERVAL '30' DAY;

END;
