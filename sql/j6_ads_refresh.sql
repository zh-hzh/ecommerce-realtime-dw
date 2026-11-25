-- =============================================================================
-- 用途：J6 刷新 ADS 三张结果表（DWS → ADS），由 scripts/run_j6.sh 执行
-- 对应文档：docs/03-数据模型.md 5.5、docs/04-架构设计.md 4.2 J6
-- 说明：
--   1. 占位符 __DT__ 由 run_j6.sh 用目标日期替换；一次只重算一天，
--      对当天全量覆盖写（Unique Key 后写覆盖先写），重复执行结果一致。
--   2. 为什么不是 Flink 作业：Doris Connector 只提供有界 Source / Lookup，
--      没有 changelog 流，读不到 DWS 的增量；ADS 又只是覆盖写的小结果表，
--      一条 INSERT 比每分钟提交一个 Flink 批作业更简单，也不占槽位（08 D26）。
--   3. ads_topn_product 先删当天再写：商品掉出 TOP 10 后，旧排名必须清掉，
--      否则 Unique Key 只会覆盖同 rank 的行，残留行会一直留着。
--   4. 漏斗的 pv / cart_cnt / submit_cnt 来自分钟 DWS 求和，受分钟水位线限制
--      （08 E27）；uv / cart_uv / submit_uv 来自日级 bitmap，收全量。
--   5. pay_user_cnt 是 ADS 唯一回读 DWD 的字段：DWS 没有支付用户 bitmap，
--      按 DWD 的 pay_time 去重人数据算（90 万行级以内的全表扫描可接受）。
--      过滤 is_deleted = 0：逻辑删除的订单不再计为支付用户（冒烟清理后
--      DWD 会留下 is_deleted=1 的软删行，不加这个条件会一直算 1 个支付用户）。
-- =============================================================================

-- ------------------------------------------------ 1. 分钟级交易看板

INSERT INTO dw.ads_gmv_realtime
  (dt, stat_minute, order_cnt, order_amt, pay_cnt, pay_amt, pay_user_cnt,
   refund_cnt, refund_amt, net_amt, pay_rate, avg_user_amt)
SELECT dt, stat_minute,
       order_cnt, order_amt, pay_cnt, pay_amt, pay_user_cnt,
       refund_cnt, refund_amt,
       pay_amt - refund_amt AS net_amt,
       COALESCE(ROUND(pay_cnt / NULLIF(order_cnt, 0), 4), 0) AS pay_rate,
       COALESCE(ROUND(pay_amt / NULLIF(pay_user_cnt, 0), 2), 0) AS avg_user_amt
FROM (
  SELECT d.dt, d.stat_minute,
         d.order_cnt, d.order_amt, d.pay_cnt, d.pay_amt,
         COALESCE(u.pay_user_cnt, 0) AS pay_user_cnt,
         d.refund_cnt, d.refund_amt
  FROM (
    SELECT dt, stat_minute,
           SUM(COALESCE(order_cnt, 0))  AS order_cnt,
           SUM(COALESCE(order_amt, 0))  AS order_amt,
           SUM(COALESCE(pay_cnt, 0))    AS pay_cnt,
           SUM(COALESCE(pay_amt, 0))    AS pay_amt,
           SUM(COALESCE(refund_cnt, 0)) AS refund_cnt,
           SUM(COALESCE(refund_amt, 0)) AS refund_amt
    FROM dw.dws_trade_1min
    WHERE dt = '__DT__'
    GROUP BY dt, stat_minute
  ) d
  LEFT JOIN (
    SELECT DATE_TRUNC(pay_time, 'minute') AS stat_minute,
           COUNT(DISTINCT user_id)        AS pay_user_cnt
    FROM dw.dwd_trade_order_detail
    WHERE pay_time >= '__DT__'
      AND pay_time < DATE_ADD('__DT__', INTERVAL 1 DAY)
      AND COALESCE(is_deleted, 0) = 0
    GROUP BY DATE_TRUNC(pay_time, 'minute')
  ) u ON d.stat_minute = u.stat_minute
) x;

-- ------------------------------------------------ 2. 日级转化漏斗

INSERT INTO dw.ads_conversion_funnel
  (dt, stat_date, pv, cart_cnt, submit_cnt, uv, cart_uv, submit_uv, pay_user_cnt,
   cart_rate, submit_rate, pay_rate)
SELECT dt, stat_date, pv, cart_cnt, submit_cnt, uv, cart_uv, submit_uv, pay_user_cnt,
       COALESCE(ROUND(cart_uv / NULLIF(uv, 0), 4), 0) AS cart_rate,
       COALESCE(ROUND(submit_uv / NULLIF(cart_uv, 0), 4), 0) AS submit_rate,
       COALESCE(ROUND(pay_user_cnt / NULLIF(submit_uv, 0), 4), 0) AS pay_rate
FROM (
  SELECT '__DT__' AS dt, '__DT__' AS stat_date,
         COALESCE(t.pv, 0)         AS pv,
         COALESCE(t.cart_cnt, 0)   AS cart_cnt,
         COALESCE(t.submit_cnt, 0) AS submit_cnt,
         COALESCE(b.uv, 0)         AS uv,
         COALESCE(b.cart_uv, 0)    AS cart_uv,
         COALESCE(b.submit_uv, 0)  AS submit_uv,
         COALESCE(p.pay_user_cnt, 0) AS pay_user_cnt
  FROM (
    SELECT SUM(COALESCE(pv, 0))         AS pv,
           SUM(COALESCE(cart_cnt, 0))   AS cart_cnt,
           SUM(COALESCE(submit_cnt, 0)) AS submit_cnt
    FROM dw.dws_traffic_1min
    WHERE dt = '__DT__'
  ) t
  LEFT JOIN (
    SELECT bitmap_union_count(uv)        AS uv,
           bitmap_union_count(cart_uv)   AS cart_uv,
           bitmap_union_count(submit_uv) AS submit_uv
    FROM dw.dws_user_1d
    WHERE dt = '__DT__'
  ) b ON 1 = 1
  LEFT JOIN (
    SELECT COUNT(DISTINCT user_id) AS pay_user_cnt
    FROM dw.dwd_trade_order_detail
    WHERE pay_time >= '__DT__'
      AND pay_time < DATE_ADD('__DT__', INTERVAL 1 DAY)
      AND COALESCE(is_deleted, 0) = 0
  ) p ON 1 = 1
) x;

-- ------------------------------------------------ 3. 日级 TOP N 商品

DELETE FROM dw.ads_topn_product WHERE dt = '__DT__';

INSERT INTO dw.ads_topn_product
  (dt, stat_date, rank_no, product_id, product_name, category_id, category_name,
   brand, order_cnt, item_cnt, order_amt, pay_cnt, pay_amt)
SELECT dt, stat_date, rank_no, product_id, product_name, category_id, category_name,
       brand, order_cnt, item_cnt, order_amt, pay_cnt, pay_amt
FROM (
  SELECT '__DT__' AS dt, '__DT__' AS stat_date,
         ROW_NUMBER() OVER (ORDER BY pay_amt DESC, product_id ASC) AS rank_no,
         product_id, product_name, category_id, category_name, brand,
         order_cnt, item_cnt, order_amt, pay_cnt, pay_amt
  FROM (
    SELECT product_id,
           MAX(product_name)  AS product_name,
           MAX(category_id)   AS category_id,
           MAX(category_name) AS category_name,
           MAX(brand)         AS brand,
           SUM(COALESCE(order_cnt, 0)) AS order_cnt,
           SUM(COALESCE(item_cnt, 0))  AS item_cnt,
           SUM(COALESCE(order_amt, 0)) AS order_amt,
           SUM(COALESCE(pay_cnt, 0))   AS pay_cnt,
           SUM(COALESCE(pay_amt, 0))   AS pay_amt
    FROM dw.dws_trade_product_1h
    WHERE dt = '__DT__'
    GROUP BY product_id
    HAVING SUM(COALESCE(pay_amt, 0)) > 0
  ) a
) b
WHERE rank_no <= 10;
