#!/usr/bin/env python3
"""
批巡检 C01~C05（DS 跑批内第 4 步）

是什么
  把 docs/06 四节的 5 条批巡检规则落成可执行脚本：4 条查 Doris，C02 查 Hive。
  结果统一写回 dw.dwd_dirty_record，PASS 只打日志、不落表。

干什么
  1. C04 先跑（分区完整性）：Hive 三张 DWD 分区 + Doris 两张明细表当日行数，
     任一为 0 即 FAIL，写完证据立即退出，阻断下游对账。
  2. C02（Hive）：dwd.dwd_trade_order_detail 的 order_amount 与 ods.order_detail
     明细合计勾稽，差额 > 0.01 落违规行。
  3. C01/C03/C05（Doris）：主键重复、支付金额与订单金额不符、脏数据率 >= 1%。
  4. 任一规则 FAIL → 退出码 1（DS 用它做阻断）。

执行
  bash scripts/run_quality_check.sh --dt 2026-11-25     # 缺省 dt = 昨天
  依赖：Hive metastore 9083、Doris 9030、pymysql（WSL 已装）

幂等
  dt 相同的一轮结果先 DELETE 再 INSERT，重跑不会重复计数。

口径注意
  - C01 是"防重复漏网"的配置校验：两张 DWD 都是 Unique Key + MoW，正常应恒为 0；
    不为 0 说明建表属性被改了，属于严重故障。
  - C03 只适用于全额支付。造数脚本只有全额支付场景；若将来出现部分支付，
    这条会误报，需要改成按支付流水逐笔比对（见 docs/06 4.3）。
  - C05 的分母取当日 dwd_trade_order_detail 与 dwd_traffic_detail 行数之和，
    分子排除 C01~C05 自己的巡检结果行，避免自证。
"""

import argparse
import json
import sys
from datetime import date, timedelta

import pymysql
from pyspark.sql import SparkSession

BATCH_RULES = ("C01", "C02", "C03", "C04", "C05")
MAX_ROWS = 500            # 单规则最多落多少条证据，避免脏表被整表重复灌满
RAW_MAX = 2048            # raw_data 列宽
DIRTY_RATE_THRESHOLD = 0.01

HIVE_DWD_TABLES = [
    "dwd.dwd_trade_order_detail",
    "dwd.dwd_payment_detail",
    "dwd.dwd_refund_detail",
]
DORIS_DWD_TABLES = [
    "dw.dwd_trade_order_detail",
    "dw.dwd_traffic_detail",
]


def parse_args():
    p = argparse.ArgumentParser(description="批巡检 C01~C05")
    p.add_argument("--dt", default=(date.today() - timedelta(days=1)).isoformat(),
                   help="检查日期 yyyy-MM-dd，默认昨天（T-1）")
    p.add_argument("--doris-host", default="127.0.0.1")
    p.add_argument("--doris-port", default=9030, type=int)
    p.add_argument("--doris-user", default="root")
    p.add_argument("--doris-password", default="")
    return p.parse_args()


# ---------------------------------------------------------------- Doris 访问
class Doris:
    """只做两件事：查数、批量写证据。表结构和流内 J4 共用同一张脏数据旁路表。"""

    def __init__(self, host, port, user, password):
        self.conn = pymysql.connect(host=host, port=port, user=user,
                                    password=password, database="dw",
                                    autocommit=True, connect_timeout=10)

    def query(self, sql, args=None):
        with self.conn.cursor() as cur:
            cur.execute(sql, args or ())
            return cur.fetchall()

    def replace_evidence(self, dt, rows):
        """同一 dt 的 C01~C05 结果先清后写，保证重跑幂等。"""
        with self.conn.cursor() as cur:
            cur.execute(
                "DELETE FROM dw.dwd_dirty_record WHERE dt=%s "
                "AND rule_id IN ('C01','C02','C03','C04','C05')", (dt,))
            if rows:
                cur.executemany(
                    "INSERT INTO dw.dwd_dirty_record "
                    "(dt, rule_id, source_table, biz_key, reason, raw_data, dirty_time) "
                    "VALUES (%s, %s, %s, %s, %s, %s, NOW(3))",
                    [(dt, r["rule_id"], r["source_table"], r["biz_key"],
                      r["reason"], r["raw_data"][:RAW_MAX]) for r in rows],
                )


def evidence(rule_id, source_table, biz_key, reason, raw):
    return {
        "rule_id": rule_id,
        "source_table": source_table,
        "biz_key": str(biz_key)[:128],
        "reason": reason[:256],
        "raw_data": json.dumps(raw, ensure_ascii=False, default=str),
    }


# ---------------------------------------------------------------- 各条规则
def check_c04(spark, doris, dt):
    """分区完整性：缺一个即 FAIL。返回违规行列表。"""
    rows = []
    for table in HIVE_DWD_TABLES:
        cnt = spark.sql(
            f"SELECT COUNT(*) AS c FROM {table} WHERE dt='{dt}'").collect()[0]["c"]
        if cnt == 0:
            rows.append(evidence("C04", table, f"{table}@{dt}",
                                 "Hive 分区缺失或为空", {"rows": cnt}))
    for table in DORIS_DWD_TABLES:
        cnt = doris.query(f"SELECT COUNT(*) FROM {table} WHERE dt=%s", (dt,))[0][0]
        if cnt == 0:
            rows.append(evidence("C04", table, f"{table}@{dt}",
                                 "Doris 当日无数据", {"rows": cnt}))
    return rows


def check_c02(spark, dt):
    """订单金额与明细合计勾稽（Hive 侧，两侧同源快照）。"""
    df = spark.sql(f"""
        SELECT o.order_id, o.order_amount, d.amt
        FROM dwd.dwd_trade_order_detail o
        JOIN (
          SELECT order_id, SUM(amount) AS amt
          FROM ods.order_detail WHERE dt='{dt}' GROUP BY order_id
        ) d ON o.order_id = d.order_id
        WHERE o.dt='{dt}' AND ABS(o.order_amount - d.amt) > 0.01
        LIMIT {MAX_ROWS}
    """)
    return [
        evidence("C02", "dwd.dwd_trade_order_detail", r["order_id"],
                 f"订单金额与明细合计不符：order_amount={r['order_amount']}, detail_sum={r['amt']}",
                 {"order_id": r["order_id"], "order_amount": str(r["order_amount"]),
                  "detail_sum": str(r["amt"])})
        for r in df.collect()
    ]


def check_c01(doris, dt):
    """主键重复：Unique Key + MoW 下应恒为 0，不为 0 说明建表属性被改。"""
    rows = []
    for order_id, cnt in doris.query(
            "SELECT order_id, COUNT(*) c FROM dw.dwd_trade_order_detail "
            "GROUP BY order_id HAVING COUNT(*) > 1 LIMIT %s", (MAX_ROWS,)):
        rows.append(evidence("C01", "dw.dwd_trade_order_detail", order_id,
                             f"order_id 重复 {cnt} 次", {"order_id": order_id, "cnt": cnt}))
    for event_id, cnt in doris.query(
            "SELECT event_id, COUNT(*) c FROM dw.dwd_traffic_detail WHERE dt=%s "
            "GROUP BY event_id HAVING COUNT(*) > 1 LIMIT %s", (dt, MAX_ROWS)):
        rows.append(evidence("C01", "dw.dwd_traffic_detail", event_id,
                             f"event_id 重复 {cnt} 次", {"event_id": event_id, "cnt": cnt}))
    return rows


def check_c03(doris, dt):
    """全额支付场景下，支付金额应与订单金额一致。"""
    rows = []
    for order_id, order_amount, pay_amount in doris.query(
            "SELECT order_id, order_amount, pay_amount FROM dw.dwd_trade_order_detail "
            "WHERE dt=%s AND pay_status=1 AND ABS(pay_amount - order_amount) > 0.01 "
            "LIMIT %s", (dt, MAX_ROWS)):
        rows.append(evidence("C03", "dw.dwd_trade_order_detail", order_id,
                             f"支付金额与订单金额不符：order_amount={order_amount}, pay_amount={pay_amount}",
                             {"order_id": order_id, "order_amount": str(order_amount),
                              "pay_amount": str(pay_amount)}))
    return rows


def check_c05(doris, dt):
    """脏数据率 = 当日流内脏行数 / 当日 DWD 行数，>= 1% 记违规。"""
    placeholders = ",".join(["%s"] * len(BATCH_RULES))
    dirty, total = doris.query(
        f"SELECT (SELECT COUNT(*) FROM dw.dwd_dirty_record WHERE dt=%s "
        f"        AND rule_id NOT IN ({placeholders})), "
        f"       (SELECT COUNT(*) FROM dw.dwd_trade_order_detail WHERE dt=%s) + "
        f"       (SELECT COUNT(*) FROM dw.dwd_traffic_detail WHERE dt=%s)",
        (dt,) + BATCH_RULES + (dt, dt))[0]
    if total == 0:
        return [], 0.0
    rate = dirty / total
    if rate < DIRTY_RATE_THRESHOLD:
        return [], rate
    return [evidence("C05", "dwd_dirty_record", f"dirty_rate@{dt}",
                     f"脏数据率 {rate:.4%} 超过阈值 {DIRTY_RATE_THRESHOLD:.0%}（脏 {dirty} 行 / 总 {total} 行）",
                     {"dirty": dirty, "total": total, "rate": round(rate, 6)})], rate


# ---------------------------------------------------------------- 主流程
def main():
    args = parse_args()
    try:
        dt = date.fromisoformat(args.dt).isoformat()
    except ValueError:
        sys.exit(f"--dt 格式错误：{args.dt}，应为 yyyy-MM-dd")

    spark = (
        SparkSession.builder
        .appName(f"quality_check_{dt}")
        .config("spark.sql.session.timeZone", "Asia/Shanghai")
        .config("spark.sql.shuffle.partitions", "4")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    doris = Doris(args.doris_host, args.doris_port, args.doris_user, args.doris_password)

    print(f"=== 批巡检 {dt} ===", flush=True)

    # C04 先行：分区不全时后面的勾稽没有意义，直接落证据并阻断
    rows_c04 = check_c04(spark, doris, dt)
    if rows_c04:
        doris.replace_evidence(dt, rows_c04)
        for r in rows_c04:
            print(f"C04 FAIL {r['source_table']} {r['reason']}", flush=True)
        print("C04 未通过，已阻断下游（对账不执行）", flush=True)
        sys.exit(1)
    print("C04 PASS 分区完整", flush=True)

    rows_c02 = check_c02(spark, dt)
    rows_c01 = check_c01(doris, dt)
    rows_c03 = check_c03(doris, dt)
    rows_c05, rate = check_c05(doris, dt)

    doris.replace_evidence(dt, rows_c02 + rows_c01 + rows_c03 + rows_c05)

    failed = False
    for rule, rows in (("C01", rows_c01), ("C02", rows_c02), ("C03", rows_c03),
                       ("C05", rows_c05)):
        if rows:
            failed = True
            print(f"{rule} FAIL {len(rows)} 条（已落 dwd_dirty_record）", flush=True)
            for r in rows[:5]:
                print(f"  - {r['biz_key']}: {r['reason']}", flush=True)
        else:
            extra = f" 脏数据率 {rate:.4%}" if rule == "C05" else ""
            print(f"{rule} PASS{extra}", flush=True)

    print("=== 结论：" + ("FAIL（退出码 1，阻断下游）" if failed else "全部通过") + " ===",
          flush=True)
    spark.stop()
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
