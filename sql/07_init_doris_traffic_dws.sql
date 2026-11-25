-- =============================================================================
-- 用途：Doris 侧建 J5 的两张流量 DWS 表（分钟级流量 / 日级用户 bitmap）
-- 对应文档：docs/03-数据模型.md 二 / 七 / 八节、docs/04-架构设计.md 4.2 J5
-- 执行：bash scripts/init_doris.sh（建表）
-- 说明：
--   1. 两张表都是 Aggregate 模型，`dt` 是分区列所以必须在 key 里（docs/08 E15）。
--      `dws_traffic_1min` 的 pv / 加购 / 下单是 SUM，`uv` 是 BITMAP_UNION；
--      `dws_user_1d` 三个用户集合都是 BITMAP_UNION。
--   2. UV 用 bitmap 而不是 Flink 状态去重（docs/03 八节）：bitmap 求并集可跨维度
--      精确上卷，状态也不会随用户数无限膨胀。
--   3. Flink 侧不能直接构造 bitmap，写入靠 Doris Stream Load 的 `columns` 表达式
--      `uv=to_bitmap(user_id)`（官方 Flink Connector 文档“Importing Bitmap Data”
--      的写法）。实测 to_bitmap(NULL) 得到空 bitmap，不会把 NULL 当成 0 号用户
--      混进 UV；重复 user_id 由 BITMAP_UNION 去重。
--   4. 单 BE 环境 replication_num 显式 1；分桶 4（docs/03 9.2）。
--   5. 动态分区保留 -90 天、预建 +3 天，与 docs/03 9.1 一致。
-- =============================================================================

CREATE DATABASE IF NOT EXISTS dw;

-- ------------------------------------------------------ 分钟级流量
-- 一分钟一行：pv / 加购 / 下单是去重后的事件数，uv 是当分钟浏览用户 bitmap。
-- pv 用 COUNT(DISTINCT event_id) 而不是 COUNT(1)：埋点重发占 ~5%，
-- 重复事件在 DWD 会被 Unique Key 覆盖掉，DWS 必须同口径（docs/06 Q08）。

CREATE TABLE IF NOT EXISTS dw.dws_traffic_1min (
  dt          DATE        NOT NULL COMMENT '统计日期，按事件时间',
  stat_minute DATETIME(3) NOT NULL COMMENT '分钟起点（事件时间截断到分钟）',
  pv          BIGINT      SUM      COMMENT '浏览量：page_view 事件数，按 event_id 去重',
  cart_cnt    BIGINT      SUM      COMMENT '加购次数：add_cart 事件数，按 event_id 去重',
  submit_cnt  BIGINT      SUM      COMMENT '下单提交次数：order_submit 事件数，按 event_id 去重',
  uv          BITMAP      BITMAP_UNION COMMENT '当分钟访客数：page_view 的 user_id 集合'
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

-- ------------------------------------------------------ 日级用户集合
-- 一天一行，按漏斗步骤各存一个用户 bitmap：
--   uv        —— page_view 用户（对应 02 的 traffic_uv_1d）
--   cart_uv   —— 加购用户
--   submit_uv —— 提交下单用户
-- 三个集合分开存，加购转化率 / 漏斗的“人数”口径可以直接 bitmap_union_count 得出；
-- 查询任意维度组合时直接求并集，不会像“人数相加”那样重复计人。

CREATE TABLE IF NOT EXISTS dw.dws_user_1d (
  dt        DATE   NOT NULL COMMENT '统计日期，按事件时间',
  stat_date DATE   NOT NULL COMMENT '业务日期（与 dt 同值，保留业务粒度命名）',
  uv        BITMAP BITMAP_UNION COMMENT '当日访客数：page_view 的 user_id 集合',
  cart_uv   BITMAP BITMAP_UNION COMMENT '当日加购用户：add_cart 的 user_id 集合',
  submit_uv BITMAP BITMAP_UNION COMMENT '当日提交下单用户：order_submit 的 user_id 集合'
) AGGREGATE KEY(dt, stat_date)
PARTITION BY RANGE(dt) ()
DISTRIBUTED BY HASH(stat_date) BUCKETS 4
PROPERTIES (
  'replication_num' = '1',
  'dynamic_partition.enable' = 'true',
  'dynamic_partition.time_unit' = 'DAY',
  'dynamic_partition.start' = '-90',
  'dynamic_partition.end' = '3',
  'dynamic_partition.prefix' = 'p',
  'dynamic_partition.buckets' = '4'
);
