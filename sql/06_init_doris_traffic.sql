-- =============================================================================
-- 用途：Doris 侧建 J4 的两张表
--         dwd_traffic_detail  埋点事务事实表（Unique Key + MoW，按 event_id 去重）
--         dwd_dirty_record    脏数据旁路表（Duplicate Key，只追加）
-- 对应文档：docs/03-数据模型.md 5.2 / 七节 / 九节、docs/04-架构设计.md 4.2 J4、
--           docs/06-数据质量与对账.md 三节（Q04 / Q05 / Q08）
-- 执行：bash scripts/init_doris.sh（建表）
-- 说明：
--   1. 埋点是"只追加"的事务事实表，但 5% 重复发送要按 event_id 去重，所以用
--      Unique Key + merge-on-write：同 event_id 后写覆盖先写，聚合前就只有一份。
--   2. 分区列必须是 key 列（docs/08 E15），因此 key 是 (event_id, dt)。同一个事件
--      的 event_time 不变，dt 也固定，不会因为分区把去重拆散。
--   3. 单 BE 环境，replication_num 必须显式 1（docs/08 E16）；桶数 8 与 docs/03 9.2 一致。
--   4. 脏数据表用 Duplicate Key：拦截记录要保留全部原始行，不能去重；
--      按 dt + rule_id 就能出日报（docs/06 4.2 不建汇总表）。
--   5. 动态分区保留 -90 天、预建 +3 天：Q04 的"早于 30 天"仍在范围内，
--      未来 +5 分钟的事件也不会因为没分区而写入失败。
-- =============================================================================

CREATE DATABASE IF NOT EXISTS dw;

-- ------------------------------------------------------- 埋点明细（DWD）

CREATE TABLE IF NOT EXISTS dw.dwd_traffic_detail (
  event_id   VARCHAR(64) NOT NULL COMMENT '事件唯一ID，去重依据',
  dt         DATE        NOT NULL COMMENT '事件日期，按 event_time 归日',
  user_id    BIGINT      NOT NULL COMMENT '用户ID，与 MySQL 同一 ID 池',
  product_id BIGINT               COMMENT '商品ID，首页浏览可为空',
  event_type VARCHAR(16) NOT NULL COMMENT 'page_view / add_cart / order_submit',
  event_time DATETIME(3) NOT NULL COMMENT '事件发生时间（epoch 毫秒转换）',
  session_id VARCHAR(64)          COMMENT '会话ID，30 分钟无操作切新会话'
) UNIQUE KEY(event_id, dt)
PARTITION BY RANGE(dt) ()
DISTRIBUTED BY HASH(event_id) BUCKETS 8
PROPERTIES (
  'replication_num' = '1',
  'enable_unique_key_merge_on_write' = 'true',
  'dynamic_partition.enable' = 'true',
  'dynamic_partition.time_unit' = 'DAY',
  'dynamic_partition.start' = '-90',
  'dynamic_partition.end' = '3',
  'dynamic_partition.prefix' = 'p',
  'dynamic_partition.buckets' = '8'
);

-- ----------------------------------------------------- 脏数据旁路（质量）

CREATE TABLE IF NOT EXISTS dw.dwd_dirty_record (
  dt           DATE          NOT NULL COMMENT '拦截日期（处理时间），Q04 的历史事件可能早于建表日期',
  rule_id      VARCHAR(8)    NOT NULL COMMENT '规则编号，当前只有 Q04 / Q05',
  source_table VARCHAR(64)   NOT NULL COMMENT '来源，埋点为 ods_traffic',
  biz_key      VARCHAR(128)  NOT NULL COMMENT '业务主键快照，埋点为 event_id',
  reason       VARCHAR(256)  NOT NULL COMMENT '拦截原因（写清是哪一条判定没过）',
  raw_data     VARCHAR(2048)          COMMENT '违规记录字段快照：JSON 解析后重构，非原始字节',
  dirty_time   DATETIME(3)            COMMENT '写入 Doris 的时间'
) DUPLICATE KEY(dt, rule_id, source_table, biz_key)
PARTITION BY RANGE(dt) ()
DISTRIBUTED BY HASH(biz_key) BUCKETS 4
PROPERTIES (
  'replication_num' = '1',
  'dynamic_partition.enable' = 'true',
  'dynamic_partition.time_unit' = 'DAY',
  'dynamic_partition.start' = '-90',
  'dynamic_partition.end' = '3',
  'dynamic_partition.prefix' = 'p',
  'dynamic_partition.buckets' = '4'
);
