-- =============================================================================
-- 用途：J5（dws_traffic）—— 消费 Kafka ods_traffic 埋点流，产出
--         dw.dws_traffic_1min：分钟级 pv / 加购 / 下单次数 + 分钟 UV（bitmap）
--         dw.dws_user_1d：日级 page_view / 加购 / 下单用户 bitmap
-- 对应文档：docs/04-架构设计.md 4.2 J5、docs/03-数据模型.md 5.3 / 八节、
--           docs/02-指标体系.md 四 / 五节、docs/08-问题索引.md D21
-- 执行：bash scripts/run_j5.sh（提交前确认槽位够；J5 占 3 个，J4 已降到 3）
-- 说明：
--   1. 输入不是 dwd_traffic_detail：Doris 表没有 changelog 流，Flink 读不到 DWD 的
--      变更（docs/08 D21）。所以 J5 直接消费 ods_traffic，并复用 J4 的清洗条件
--      （Q04 时间越界 / Q05 枚举与字段非法）。脏数据由 J4 负责落 dwd_dirty_record，
--      J5 只做过滤、不重复写脏表，保证同一批事件只被拦截一次。
--   2. 去重口径与 DWD 对齐（Q08）：
--      - pv / 加购 / 下单次数用 COUNT(DISTINCT event_id)，在 1 分钟窗口内去重；
--        埋点重发在秒级内到达，分钟窗口足以覆盖，且状态只留一个窗口。
--      - UV 用 user_id 的 bitmap，重复 user_id 由 Doris BITMAP_UNION 天然去重，
--        不需要 Flink 状态（docs/03 八节）。
--      - 分钟表的 UV 也放进同一个 TUMBLE 窗口（GROUP BY window_start, user_id）：
--        这样 pv 和 uv 用的是同一批“水印内”事件。若 UV 按事件逐条写，超过水印的
--        迟到事件会只进 UV 不进 pv，出现 uv > pv 的自相矛盾（实测踩到过）。
--   3. 迟到口径：生成器默认注入 1% 迟到事件（5~30 分钟），它们是合法数据（docs/05
--      第五节），不进脏表。分钟表只统计水印内事件，超过水印的迟到不进分钟表；
--      日表 dws_user_1d 不设窗口，收全量（与 DWD 的全量口径一致）。
--      另一个水印坑：未来时间的事件被 WHERE 过滤掉，但水印在源表上、先于过滤生效，
--      仍会把水位线往前推（docs/08 E27）。
--   4. bitmap 不能由 Flink 构造，靠 Doris Stream Load 的 columns 表达式
--      `uv=to_bitmap(uv_id)` 写入（官方 Flink Connector 文档 Importing Bitmap Data
--      的写法，本机已实测：NULL 得到空 bitmap、重复 user_id 自动去重）。
--   5. 同一 topic 出现两次（分钟表、日表），各建一张源表、各用独立 group.id；
--      同 group.id 会把 6 个分区“分掉”导致每条流只看到一部分数据（docs/08 E22）。
--   6. 时间窗口按事件时间；watermark 30s、source idle 30s，稀疏时段窗口也能按时
--      触发（否则空闲分区卡住水位线，分钟结果永远不出）。
--   7. 并行度 3：槽位预算 J1 4 + J2 3 + J3 3 + J4 3 + J5 3 = 16（docs/08 D25）。
-- =============================================================================

SET 'pipeline.name' = 'J5-dws-traffic';
SET 'execution.checkpointing.interval' = '10s';
SET 'parallelism.default' = '3';
SET 'table.exec.source.idle-timeout' = '30s';
SET 'table.dml-sync' = 'false';

-- ====================================== 源：ods_traffic（分钟级分支）

CREATE TABLE ods_traffic_min (
  event_id    STRING,
  user_id     BIGINT,
  product_id  BIGINT,
  event_type  STRING,
  event_time  BIGINT,
  session_id  STRING,
  event_time_ts AS TO_TIMESTAMP_LTZ(event_time, 3),
  WATERMARK FOR event_time_ts AS event_time_ts - INTERVAL '30' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'ods_traffic',
  'properties.bootstrap.servers' = '127.0.0.1:9092',
  'properties.group.id' = 'j5_traffic_min',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

-- 清洗条件与 J4 合法分支完全一致（见文件头第 1 条）
CREATE TEMPORARY VIEW traffic_valid_min AS
SELECT event_id, user_id, event_type, event_time_ts
FROM ods_traffic_min
WHERE event_id IS NOT NULL
  AND user_id IS NOT NULL
  AND event_time_ts IS NOT NULL
  AND event_type IN ('page_view', 'add_cart', 'order_submit')
  AND event_time_ts <= CURRENT_TIMESTAMP + INTERVAL '5' MINUTE
  AND event_time_ts >= CURRENT_TIMESTAMP - INTERVAL '30' DAY;

-- ====================================== 目标：dws_traffic_1min
-- 分钟指标行（pv/cart/submit）与分钟 UV 行（每用户一行，uv_id 非空）写同一张表：
-- SUM 聚合把 UV 行的 0 加上去不影响结果；UV 行 pv 写 0，不虚增 pv。

CREATE TABLE dws_traffic_1min_sink (
  dt          DATE,
  stat_minute TIMESTAMP(3),
  pv          BIGINT,
  cart_cnt    BIGINT,
  submit_cnt  BIGINT,
  uv_id       BIGINT
) WITH (
  'connector' = 'doris',
  'fenodes' = '127.0.0.1:8030',
  'table.identifier' = 'dw.dws_traffic_1min',
  'username' = 'root',
  'password' = '',
  'sink.label-prefix' = 'j5_traffic_min_v3',
  'sink.enable-2pc' = 'true',
  'sink.properties.format' = 'json',
  'sink.properties.read_json_by_line' = 'true',
  'sink.properties.columns' = 'dt,stat_minute,pv,cart_cnt,submit_cnt,uv_id,uv=to_bitmap(uv_id)'
);

-- ====================================== 源：ods_traffic（日级用户集分支）

CREATE TABLE ods_traffic_uv (
  event_id    STRING,
  user_id     BIGINT,
  product_id  BIGINT,
  event_type  STRING,
  event_time  BIGINT,
  session_id  STRING,
  event_time_ts AS TO_TIMESTAMP_LTZ(event_time, 3),
  WATERMARK FOR event_time_ts AS event_time_ts - INTERVAL '30' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'ods_traffic',
  'properties.bootstrap.servers' = '127.0.0.1:9092',
  'properties.group.id' = 'j5_traffic_uv',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

CREATE TEMPORARY VIEW traffic_valid_uv AS
SELECT event_id, user_id, event_type, event_time_ts
FROM ods_traffic_uv
WHERE event_id IS NOT NULL
  AND user_id IS NOT NULL
  AND event_time_ts IS NOT NULL
  AND event_type IN ('page_view', 'add_cart', 'order_submit')
  AND event_time_ts <= CURRENT_TIMESTAMP + INTERVAL '5' MINUTE
  AND event_time_ts >= CURRENT_TIMESTAMP - INTERVAL '30' DAY;

-- ====================================== 目标：dws_user_1d
-- 一条事件只填一个 bitmap 列，其余 uv_id 为 NULL → to_bitmap(NULL) 是空 bitmap，
-- 不会把用户写进别的集合（实测见 sql/07 文件头第 3 条）。

CREATE TABLE dws_user_1d_sink (
  dt           DATE,
  stat_date    DATE,
  uv_id        BIGINT,
  cart_uv_id   BIGINT,
  submit_uv_id BIGINT
) WITH (
  'connector' = 'doris',
  'fenodes' = '127.0.0.1:8030',
  'table.identifier' = 'dw.dws_user_1d',
  'username' = 'root',
  'password' = '',
  'sink.label-prefix' = 'j5_user_uv_v3',
  'sink.enable-2pc' = 'true',
  'sink.properties.format' = 'json',
  'sink.properties.read_json_by_line' = 'true',
  'sink.properties.columns' = 'dt,stat_date,uv_id,cart_uv_id,submit_uv_id,uv=to_bitmap(uv_id),cart_uv=to_bitmap(cart_uv_id),submit_uv=to_bitmap(submit_uv_id)'
);

-- ==================================================================== 提交

EXECUTE STATEMENT SET
BEGIN

-- 1) 分钟级指标 + 分钟 UV（两个分支都走同一个 1 分钟窗口，口径一致）
INSERT INTO dws_traffic_1min_sink
SELECT
  CAST(window_start AS DATE)                                                AS dt,
  CAST(DATE_FORMAT(window_start, 'yyyy-MM-dd HH:mm:00') AS TIMESTAMP(3))    AS stat_minute,
  COUNT(DISTINCT CASE WHEN event_type = 'page_view'    THEN event_id END)   AS pv,
  COUNT(DISTINCT CASE WHEN event_type = 'add_cart'     THEN event_id END)   AS cart_cnt,
  COUNT(DISTINCT CASE WHEN event_type = 'order_submit' THEN event_id END)   AS submit_cnt,
  CAST(NULL AS BIGINT)                                                      AS uv_id
FROM TABLE(
  TUMBLE(TABLE traffic_valid_min, DESCRIPTOR(event_time_ts), INTERVAL '1' MINUTE)
)
GROUP BY window_start, window_end
UNION ALL
SELECT
  CAST(window_start AS DATE)                                                AS dt,
  CAST(DATE_FORMAT(window_start, 'yyyy-MM-dd HH:mm:00') AS TIMESTAMP(3))    AS stat_minute,
  0                                                                         AS pv,
  0                                                                         AS cart_cnt,
  0                                                                         AS submit_cnt,
  user_id                                                                   AS uv_id
FROM TABLE(
  TUMBLE(TABLE traffic_valid_min, DESCRIPTOR(event_time_ts), INTERVAL '1' MINUTE)
)
WHERE event_type = 'page_view'
GROUP BY window_start, window_end, user_id;

-- 2) 日级用户集合：三个分支各写一个 bitmap，其余置 NULL
INSERT INTO dws_user_1d_sink
SELECT
  CAST(event_time_ts AS DATE) AS dt,
  CAST(event_time_ts AS DATE) AS stat_date,
  user_id                     AS uv_id,
  CAST(NULL AS BIGINT)        AS cart_uv_id,
  CAST(NULL AS BIGINT)        AS submit_uv_id
FROM traffic_valid_uv
WHERE event_type = 'page_view'
UNION ALL
SELECT
  CAST(event_time_ts AS DATE) AS dt,
  CAST(event_time_ts AS DATE) AS stat_date,
  CAST(NULL AS BIGINT)        AS uv_id,
  user_id                     AS cart_uv_id,
  CAST(NULL AS BIGINT)        AS submit_uv_id
FROM traffic_valid_uv
WHERE event_type = 'add_cart'
UNION ALL
SELECT
  CAST(event_time_ts AS DATE) AS dt,
  CAST(event_time_ts AS DATE) AS stat_date,
  CAST(NULL AS BIGINT)        AS uv_id,
  CAST(NULL AS BIGINT)        AS cart_uv_id,
  user_id                     AS submit_uv_id
FROM traffic_valid_uv
WHERE event_type = 'order_submit';

END;
