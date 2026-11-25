-- =============================================================================
-- 用途：Doris 侧建商品维表 dw.dim_product —— 供 J3 做 Lookup Join 补齐类目/品牌
-- 对应文档：docs/03-数据模型.md 4.2 / 6.2、docs/04-架构设计.md 4.2 J3
-- 执行：bash scripts/init_doris.sh（建表）+ bash scripts/load_dim_product.sh（灌数）
-- 说明：
--   1. 与 docs/03 6.2 的"每日全量快照 + dt 分区"不同，这里只保留最新一份快照，
--      原因同 dim_shop：Flink Lookup Join 按 key 点查，要求一个 product_id 只命中一行，
--      且 Doris 维表 lookup 不支持"只查某个分区"。偏差已记入 docs/03 6.2 与 docs/08。
--   2. 商品改类目会失真（历史订单用最新类目），但成交价不受影响——
--      价格快照存在 order_detail.price 里（docs/05 一节的设计说明）。
--   3. 唯一键 product_id + MoW：重复装载同一天数据时直接覆盖。
--   4. snapshot_date 记录本行是哪天的快照，便于排查维表新旧。
-- =============================================================================

CREATE TABLE IF NOT EXISTS dw.dim_product (
  product_id    BIGINT       NOT NULL COMMENT '商品ID',
  product_name  VARCHAR(128)          COMMENT '商品名',
  category_id   BIGINT                COMMENT '一级类目ID',
  category_name VARCHAR(32)           COMMENT '一级类目名',
  brand         VARCHAR(32)           COMMENT '品牌',
  price         DECIMAL(10, 2)        COMMENT '当前售价（成交价看 order_detail.price）',
  status        TINYINT               COMMENT '1上架 0下架',
  update_time   DATETIME(3)           COMMENT '源端更新时间',
  snapshot_date DATE                  COMMENT '本行快照日期'
) UNIQUE KEY(product_id)
DISTRIBUTED BY HASH(product_id) BUCKETS 4
PROPERTIES (
  'replication_num' = '1',
  'enable_unique_key_merge_on_write' = 'true'
);
