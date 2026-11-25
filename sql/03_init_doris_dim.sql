-- =============================================================================
-- 用途：Doris 侧建店铺维表 dw.dim_shop —— 供 J2 做 Lookup Join 补齐 province/city
-- 对应文档：docs/03-数据模型.md 6.2、docs/04-架构设计.md 4.2
-- 执行：bash scripts/init_doris.sh（建表）+ bash scripts/load_dim_shop.sh（灌数）
-- 说明：
--   1. 与 docs/03 6.2 的"每日全量快照 + dt 分区"不同，这里只保留最新一份快照：
--      Flink Lookup Join 按 key 查维表，要求一个 shop_id 只命中一行；若按 dt 分区，
--      同一 shop_id 会有多天数据，lookup 会返回多行导致关联出错。店铺所在省市
--      基本不变（docs/03 6.2 已评估），只留最新快照不失真。
--   2. 唯一键 shop_id + MoW：重复装载同一天数据或店铺信息变更时直接覆盖。
--   3. 单 BE 环境，replication_num 必须显式 1（同 02 脚本）。
--   4. snapshot_date 记录本行是"哪天的快照"，便于排查维表新旧，不参与分区。
-- =============================================================================

CREATE TABLE IF NOT EXISTS dw.dim_shop (
  shop_id       BIGINT      NOT NULL COMMENT '店铺ID',
  shop_name     VARCHAR(64)          COMMENT '店铺名',
  shop_type     VARCHAR(16)          COMMENT '旗舰店/专卖店/专营店',
  province      VARCHAR(32)          COMMENT '省份',
  city          VARCHAR(32)          COMMENT '城市',
  create_time   DATETIME             COMMENT '源端开店时间',
  snapshot_date DATE                 COMMENT '本行快照日期'
) UNIQUE KEY(shop_id)
DISTRIBUTED BY HASH(shop_id) BUCKETS 4
PROPERTIES (
  'replication_num' = '1',
  'enable_unique_key_merge_on_write' = 'true'
);
