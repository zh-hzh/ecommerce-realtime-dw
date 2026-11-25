#!/usr/bin/env python3
"""
离线 T+1 对账：Hive ADS（离线） vs Doris（实时），结果写 Doris ads_reconcile_diff

是什么
  按对账日把 5 个交易指标的两侧取值摆在一起算差异，按 docs/06 6.2 的阈值判 PASS/FAIL。

干什么
  1. 读 Hive ads.trade_recon_1d 的 dt 分区（离线值）；
  2. 用 JDBC 读 Doris 的实时口径值：
       order_cnt    dwd_trade_order_detail  COUNT(DISTINCT order_id)，dt
       pay_cnt      dwd_trade_order_detail  COUNT(DISTINCT pay_id)，pay_time 归日
       pay_user_cnt dwd_trade_order_detail  COUNT(DISTINCT user_id)，pay_time 归日且 is_deleted=0
       pay_amt      dws_trade_1min          SUM(pay_amt)，dt = 支付日
       refund_amt   dwd_trade_order_detail  SUM(refund_amount)，dt = 下单日（退款回溯原订单日）
  3. 计算 diff_value / diff_rate / status，覆盖写 dw.ads_reconcile_diff。

执行
  bash scripts/run_reconcile.sh --dt 2026-11-25

依赖
  - Hive 里的 dt 分区已由 offline_t1 产出
  - Doris FE 9030 可连；MySQL JDBC 驱动由 --jars 传入

口径
  - 金额类阈值 0.1%（DECIMAL 求和精度），笔数/人数类 0 容忍。
  - 离线为 0 且实时为 0 判 PASS；离线为 0 实时不为 0 判 FAIL、diff_rate 置 NULL。
  - 同一 dt 重跑覆盖，只保留最新结论。
"""

import argparse
import os
import sys
from datetime import date, datetime, timedelta
from decimal import Decimal

from pyspark.sql import SparkSession

# 指标 -> (阈值, 是否金额类)
METRICS = {
    "order_cnt": ("0", False),
    "pay_cnt": ("0", False),
    "pay_user_cnt": ("0", False),
    "pay_amt": ("0.1%", True),
    "refund_amt": ("0.1%", True),
}


def parse_args():
    p = argparse.ArgumentParser(description="离线 T+1 对账：Hive vs Doris")
    p.add_argument("--dt", default=(date.today() - timedelta(days=1)).isoformat(),
                   help="对账日期 yyyy-MM-dd，默认昨天")
    p.add_argument("--host", default="127.0.0.1", help="Doris FE 地址")
    p.add_argument("--port", default=9030, type=int)
    p.add_argument("--user", default="root")
    p.add_argument("--password", default=os.environ.get("DORIS_PASSWORD", ""))
    p.add_argument("--database", default="dw")
    return p.parse_args()


def main():
    args = parse_args()
    try:
        dt = date.fromisoformat(args.dt)
    except ValueError:
        sys.exit(f"--dt 格式错误：{args.dt}，应为 yyyy-MM-dd")
    dt_str = dt.isoformat()

    spark = (
        SparkSession.builder
        .appName(f"reconcile_{dt_str}")
        .config("spark.sql.session.timeZone", "Asia/Shanghai")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    rows = spark.sql(f"""
        SELECT order_cnt, pay_cnt, pay_user_cnt, pay_amt, refund_amt
        FROM ads.trade_recon_1d WHERE dt = '{dt_str}'
    """).collect()
    if not rows:
        sys.exit(f"Hive 没有 ads.trade_recon_1d 的 dt={dt_str} 分区，先跑 run_offline_t1.sh")
    offline = rows[0].asDict()

    url = (
        f"jdbc:mysql://{args.host}:{args.port}/{args.database}"
        "?useSSL=false&allowPublicKeyRetrieval=true"
        "&serverTimezone=Asia/Shanghai&characterEncoding=utf8"
    )
    props = {"driver": "com.mysql.cj.jdbc.Driver", "user": args.user, "password": args.password}

    rt_sql = f"""
        SELECT 'order_cnt' AS metric, CAST(COUNT(DISTINCT order_id) AS DECIMAL(18,2)) AS v
          FROM dw.dwd_trade_order_detail WHERE dt = '{dt_str}'
        UNION ALL
        SELECT 'pay_cnt', CAST(COUNT(DISTINCT pay_id) AS DECIMAL(18,2))
          FROM dw.dwd_trade_order_detail WHERE DATE(pay_time) = '{dt_str}'
        UNION ALL
        SELECT 'pay_user_cnt', CAST(COUNT(DISTINCT user_id) AS DECIMAL(18,2))
          FROM dw.dwd_trade_order_detail WHERE DATE(pay_time) = '{dt_str}' AND is_deleted = 0
        UNION ALL
        SELECT 'pay_amt', CAST(COALESCE(SUM(pay_amt), 0) AS DECIMAL(18,2))
          FROM dw.dws_trade_1min WHERE dt = '{dt_str}'
        UNION ALL
        SELECT 'refund_amt', CAST(COALESCE(SUM(refund_amount), 0) AS DECIMAL(18,2))
          FROM dw.dwd_trade_order_detail WHERE dt = '{dt_str}'
    """
    rt_rows = spark.read.jdbc(url=url, table=f"({rt_sql}) t", properties=props).collect()
    # Doris 的 MySQL 协议会把 VARCHAR 右补空格到列宽，这里必须 strip
    realtime = {r["metric"].strip(): r["v"] for r in rt_rows}
    missing = set(METRICS) - set(realtime)
    if missing:
        sys.exit(f"Doris 没有返回指标：{sorted(missing)}")

    check_time = datetime.now().replace(microsecond=0)
    selects = []
    printed = []
    for metric, (threshold, is_amount) in METRICS.items():
        rt_val = Decimal(realtime[metric])
        off_val = Decimal(offline[metric])
        diff = rt_val - off_val
        if off_val == 0:
            rate = None
            status = "PASS" if rt_val == 0 else "FAIL"
        else:
            rate = float(abs(diff) / off_val)
            if is_amount:
                status = "PASS" if rate < 0.001 else "FAIL"
            else:
                status = "PASS" if diff == 0 else "FAIL"
        printed.append((metric, rt_val, off_val, diff, rate, threshold, status))
        rate_sql = "CAST(NULL AS DOUBLE)" if rate is None else f"CAST({rate} AS DOUBLE)"
        selects.append(f"""
            SELECT DATE '{dt_str}' AS stat_date, '{metric}' AS metric,
                   CAST({rt_val} AS DECIMAL(18,2)) AS realtime_value,
                   CAST({off_val} AS DECIMAL(18,2)) AS offline_value,
                   CAST({diff} AS DECIMAL(18,2)) AS diff_value,
                   {rate_sql} AS diff_rate,
                   '{threshold}' AS threshold, '{status}' AS status,
                   TIMESTAMP '{check_time:%Y-%m-%d %H:%M:%S}' AS check_time
        """)
    # 不经过 createDataFrame：PySpark 3.5 在 Python 3.14 下序列化本地数据会 RecursionError
    df = spark.sql(" UNION ALL ".join(selects))
    df.write.mode("append").jdbc(url=url, table="dw.ads_reconcile_diff", properties=props)

    print(f"[done] dt={dt_str}，结果写入 dw.ads_reconcile_diff")
    for metric, rt_val, off_val, diff, rate, threshold, status in printed:
        rate_txt = "-" if rate is None else f"{rate * 100:.4f}%"
        print(f"  {metric:<13} 实时={rt_val:>14} 离线={off_val:>14} 差异={diff:>12} "
              f"差异率={rate_txt:>9} 阈值={threshold:<5} {status}")
    spark.stop()


if __name__ == "__main__":
    main()
