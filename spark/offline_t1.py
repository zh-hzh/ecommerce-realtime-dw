#!/usr/bin/env python3
"""
离线 T+1 跑批：MySQL 业务库 -> Hive ods / dwd / dws / ads

是什么
  按天把 MySQL 的 5 张表（订单、订单明细、3 张维表）和 2 张流水表（支付、退款）
  抽到 Hive，再在 Hive 内算出天级交易聚合与对账口径结果。

干什么
  1. ods：order_info / order_detail 按 dt 分区（dt = 下单日）；shop / user / product
     三张维表全量覆盖（当前值）；payment / refund 在抽取时 JOIN 订单，落到
     dwd.dwd_payment_detail / dwd.dwd_refund_detail，dt = 支付日 / 退款日。
  2. dwd.dwd_trade_order_detail：一单一行，dt = 下单日，支付 / 退款取截至跑批时刻的
     最新一条成功记录（与 Doris 侧 J2 的合并结果同口径）。
  3. dws.trade_1d：事件时间口径的天级聚合（下单按下单日、支付按支付日、退款按退款日）。
  4. ads.trade_recon_1d：对账口径，退款金额回溯到原订单日（对账用 ADS，不是 DWS）。

执行
  bash scripts/run_offline_t1.sh --dt 2026-11-25      # 缺省 dt = 昨天
  或直接：
  spark-submit --master local[2] --jars <mysql-connector>.jar \
    --conf spark.sql.session.timeZone=Asia/Shanghai \
    spark/offline_t1.py --dt 2026-11-25

依赖
  - Hive metastore 9083 可连、HDFS 可写（库表见 sql/09_init_hive.sql）
  - MySQL JDBC 驱动由 --jars 传入
  - MYSQL_PASSWORD 环境变量或 --password（口令不落盘）

幂等
  每个 dt 分区都是 INSERT OVERWRITE，可重复跑；维表每次全量覆盖。
  跑批只读 MySQL，不写任何源表。

口径注意
  - 时间统一按 Asia/Shanghai 处理，JDBC 连接串带 serverTimezone，避免 8 小时偏移。
  - 退款金额两个口径：dws.trade_1d.refund_amt 按退款发生日；ads.trade_recon_1d.refund_amt
    按原订单日回溯（和实时侧 DWD 的 create_time 口径一致）。
"""

import argparse
import os
import sys
from datetime import date, timedelta

from pyspark.sql import SparkSession


def parse_args():
    p = argparse.ArgumentParser(description="离线 T+1：MySQL -> Hive")
    p.add_argument("--dt", default=(date.today() - timedelta(days=1)).isoformat(),
                   help="跑批日期 yyyy-MM-dd，默认昨天")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", default=3306, type=int)
    p.add_argument("--user", default="root")
    p.add_argument("--password", default=os.environ.get("MYSQL_PASSWORD", ""))
    p.add_argument("--db", default="trade")
    return p.parse_args()


def main():
    args = parse_args()
    try:
        dt = date.fromisoformat(args.dt)
    except ValueError:
        sys.exit(f"--dt 格式错误：{args.dt}，应为 yyyy-MM-dd")
    dt_str = dt.isoformat()
    next_str = (dt + timedelta(days=1)).isoformat()

    if not args.password:
        sys.exit("缺少 MySQL 口令：设置 MYSQL_PASSWORD 或传 --password")

    spark = (
        SparkSession.builder
        .appName(f"offline_t1_{dt_str}")
        .config("spark.sql.session.timeZone", "Asia/Shanghai")
        .config("spark.sql.sources.partitionOverwriteMode", "static")
        .config("spark.sql.shuffle.partitions", "4")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    url = (
        f"jdbc:mysql://{args.host}:{args.port}/{args.db}"
        "?useSSL=false&allowPublicKeyRetrieval=true"
        "&serverTimezone=Asia/Shanghai&characterEncoding=utf8"
        "&tinyInt1isBit=false&rewriteBatchedStatements=true"
    )
    props = {"driver": "com.mysql.cj.jdbc.Driver", "user": args.user, "password": args.password}

    def jdbc(name, sql):
        print(f"[read] {name}")
        return spark.read.jdbc(url=url, table=f"({sql}) t", properties=props)

    def overwrite_partition(df, table):
        view = "tmp_" + table.split(".")[-1]
        df.createOrReplaceTempView(view)
        spark.sql(f"INSERT OVERWRITE TABLE {table} PARTITION (dt='{dt_str}') SELECT * FROM {view}")

    ts_range = f"{{col}} >= '{dt_str} 00:00:00' AND {{col}} < '{next_str} 00:00:00'"

    try:
        # ---------------- ODS：订单 + 明细 ----------------
        order_df = jdbc("order_info", f"""
            SELECT order_id, user_id, shop_id, order_amount, item_cnt, order_status,
                   is_deleted, create_time, update_time
            FROM order_info
            WHERE {ts_range.format(col='create_time')}
        """)
        overwrite_partition(order_df, "ods.order_info")

        detail_df = jdbc("order_detail", f"""
            SELECT detail_id, order_id, product_id, num, price, amount, create_time
            FROM order_detail
            WHERE {ts_range.format(col='create_time')}
        """)
        overwrite_partition(detail_df, "ods.order_detail")

        # ---------------- ODS：维表全量覆盖 ----------------
        shop_df = jdbc("shop_info", "SELECT shop_id, shop_name, shop_type, province, city, create_time, update_time FROM shop_info")
        shop_df.createOrReplaceTempView("tmp_shop")
        spark.sql("INSERT OVERWRITE TABLE ods.shop_info SELECT * FROM tmp_shop")

        user_df = jdbc("user_info", "SELECT user_id, gender, age, age_range, city, user_level, register_time, update_time FROM user_info")
        user_df.createOrReplaceTempView("tmp_user")
        spark.sql("INSERT OVERWRITE TABLE ods.user_info SELECT * FROM tmp_user")

        product_df = jdbc("product_info", "SELECT product_id, product_name, category_id, category_name, brand, price, status, update_time FROM product_info")
        product_df.createOrReplaceTempView("tmp_product")
        spark.sql("INSERT OVERWRITE TABLE ods.product_info SELECT * FROM tmp_product")

        # ---------------- DWD：支付 / 退款明细（抽取时 JOIN 订单） ----------------
        pay_df = jdbc("payment_info", f"""
            SELECT p.pay_id, p.order_id, o.user_id, o.shop_id, p.pay_amount, p.pay_channel,
                   p.pay_status, p.pay_time, o.create_time AS order_create_time, o.is_deleted
            FROM payment_info p JOIN order_info o ON p.order_id = o.order_id
            WHERE {ts_range.format(col='p.pay_time')}
        """)
        overwrite_partition(pay_df, "dwd.dwd_payment_detail")

        refund_df = jdbc("refund_info", f"""
            SELECT r.refund_id, r.order_id, o.user_id, o.shop_id, r.refund_amount, r.refund_status,
                   r.refund_reason, r.refund_time, o.create_time AS order_create_time, o.is_deleted
            FROM refund_info r JOIN order_info o ON r.order_id = o.order_id
            WHERE {ts_range.format(col='r.refund_time')}
        """)
        overwrite_partition(refund_df, "dwd.dwd_refund_detail")

        # ---------------- DWD：交易明细（一单一行） ----------------
        spark.sql("""
            CREATE OR REPLACE TEMPORARY VIEW pay_latest AS
            SELECT order_id, pay_id, pay_amount, pay_time
            FROM (
              SELECT order_id, pay_id, pay_amount, pay_time,
                     ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY pay_time DESC, pay_id DESC) AS rn
              FROM dwd.dwd_payment_detail WHERE pay_status = 1
            ) x WHERE rn = 1
        """)
        spark.sql("""
            CREATE OR REPLACE TEMPORARY VIEW refund_latest AS
            SELECT order_id, refund_amount
            FROM (
              SELECT order_id, refund_amount,
                     ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY refund_time DESC, refund_id DESC) AS rn
              FROM dwd.dwd_refund_detail WHERE refund_status = 1
            ) x WHERE rn = 1
        """)
        spark.sql(f"""
            INSERT OVERWRITE TABLE dwd.dwd_trade_order_detail PARTITION (dt='{dt_str}')
            SELECT o.order_id, o.user_id, o.shop_id, s.province, s.city,
                   o.order_amount, o.item_cnt,
                   p.pay_id, p.pay_amount, r.refund_amount,
                   o.create_time, p.pay_time, o.order_status, o.is_deleted, o.update_time
            FROM ods.order_info o
            LEFT JOIN ods.shop_info s ON o.shop_id = s.shop_id
            LEFT JOIN pay_latest p ON o.order_id = p.order_id
            LEFT JOIN refund_latest r ON o.order_id = r.order_id
            WHERE o.dt = '{dt_str}'
        """)

        # ---------------- DWS：事件时间口径 ----------------
        spark.sql(f"""
            INSERT OVERWRITE TABLE dws.trade_1d PARTITION (dt='{dt_str}')
            SELECT
              (SELECT COUNT(DISTINCT order_id) FROM ods.order_info WHERE dt = '{dt_str}') AS order_cnt,
              (SELECT COALESCE(SUM(order_amount), 0) FROM ods.order_info WHERE dt = '{dt_str}') AS order_amt,
              (SELECT COUNT(DISTINCT pay_id) FROM dwd.dwd_payment_detail WHERE dt = '{dt_str}' AND pay_status = 1) AS pay_cnt,
              (SELECT COALESCE(SUM(pay_amount), 0) FROM dwd.dwd_payment_detail WHERE dt = '{dt_str}' AND pay_status = 1) AS pay_amt,
              (SELECT COUNT(DISTINCT user_id) FROM dwd.dwd_payment_detail
                WHERE dt = '{dt_str}' AND pay_status = 1 AND is_deleted = 0) AS pay_user_cnt,
              (SELECT COUNT(DISTINCT refund_id) FROM dwd.dwd_refund_detail WHERE dt = '{dt_str}' AND refund_status = 1) AS refund_cnt,
              (SELECT COALESCE(SUM(refund_amount), 0) FROM dwd.dwd_refund_detail WHERE dt = '{dt_str}' AND refund_status = 1) AS refund_amt,
              current_timestamp() AS stat_time
        """)

        # ---------------- ADS：对账口径（退款回溯原订单日） ----------------
        spark.sql(f"""
            INSERT OVERWRITE TABLE ads.trade_recon_1d PARTITION (dt='{dt_str}')
            SELECT d.order_cnt, d.pay_cnt, d.pay_user_cnt, d.pay_amt,
                   (SELECT COALESCE(SUM(refund_amount), 0)
                      FROM dwd.dwd_refund_detail
                     WHERE refund_status = 1
                       AND CAST(order_create_time AS DATE) = DATE '{dt_str}') AS refund_amt,
                   current_timestamp() AS stat_time
            FROM dws.trade_1d d WHERE d.dt = '{dt_str}'
        """)

        # ---------------- 汇总 ----------------
        print(f"[done] dt={dt_str}")
        for t in ("ods.order_info", "ods.order_detail", "dwd.dwd_payment_detail",
                  "dwd.dwd_refund_detail", "dwd.dwd_trade_order_detail", "dws.trade_1d",
                  "ads.trade_recon_1d"):
            cnt = spark.sql(f"SELECT COUNT(*) FROM {t} WHERE dt = '{dt_str}'").collect()[0][0]
            print(f"  {t}: {cnt}")
        row = spark.sql(f"SELECT * FROM ads.trade_recon_1d WHERE dt = '{dt_str}'").collect()[0]
        print(f"  ADS: order_cnt={row.order_cnt} pay_cnt={row.pay_cnt} pay_user_cnt={row.pay_user_cnt} "
              f"pay_amt={row.pay_amt} refund_amt={row.refund_amt}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
