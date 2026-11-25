# -*- coding: utf-8 -*-
"""
用途：生成 30 天历史交易数据（订单 / 明细 / 支付 / 退款），只写 MySQL，不进 Kafka。
对应文档：docs/05-数据源与造数.md 4.1 / 4.3 / 五 / 7.4
执行：
    MYSQL_PASSWORD=xxx python3 generator/gen_history.py --days 30 --orders-per-day 30000
输出：
    trade.order_info / order_detail / payment_info / refund_info
依赖：
    - 先跑 gen_master.py（要读 id_pool.json 的用户/商品/店铺范围）
    - pymysql
说明：
    - 必须在 J1（Flink CDC）启动之前跑，否则 90 万条 INSERT 会灌进 ods_order
    - order_id 按天连续编号，同参数重跑结果一致；--start-day 可断点续跑
    - update_time 写成"业务上合理的历史时间"，不能是 now()，否则离线增量抽取会全量重抽
    - 金额异常注入：--bad-amount-rate（默认 0.002），order_amount 置 0，支付金额仍为明细合计
"""

import argparse
import json
import os
import random
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import pymysql

# ---------------------------------------------------------------- 静态配置

# 一天内的下单热度：凌晨低、上午和晚间两个峰
HOUR_WEIGHTS = [
    1, 1, 1, 1, 1, 2, 3, 5,       # 0-7
    8, 12, 15, 14,                # 8-11
    12, 12, 11, 10,               # 12-15
    12, 14, 17, 20,               # 16-19
    23, 20, 12, 5,                # 20-23
]

# 支付渠道，占比贴近真实：支付宝 > 微信 >> 银行卡
PAY_CHANNELS = ["alipay", "wechat", "card"]
PAY_CHANNEL_WEIGHTS = [55, 40, 5]

REFUND_REASONS = ["七天无理由", "质量问题", "发错货", "尺寸不符", "不想要了"]

# 订单终态分布（历史数据都已完成生命周期）：已支付 94%，取消 6%
STATUS_CHOICES = [500, 400, 300, 600]
STATUS_WEIGHTS = [84, 8, 2, 6]


# ---------------------------------------------------------------- 基础工具

def connect(args):
    """连接 MySQL，返回不自动提交的连接（按天提交，便于观察进度）。"""
    password = args.password or os.getenv("MYSQL_PASSWORD")
    if not password:
        sys.exit("缺少数据库密码：用 --password 或环境变量 MYSQL_PASSWORD 传入")
    return pymysql.connect(
        host=args.host,
        port=args.port,
        user=args.user,
        password=password,
        database=args.db,
        charset="utf8mb4",
        autocommit=False,
    )


def load_pool(path):
    """读取 gen_master 产出的 ID 池，缺文件直接退出而不是瞎猜范围。"""
    if not path.exists():
        sys.exit(f"找不到 {path}，请先运行 gen_master.py")
    return json.loads(path.read_text(encoding="utf-8"))


def load_prices(conn):
    """把商品当前售价读进内存。

    只取 (product_id, price)：50k 行、约 2 MB，比每单回查数据库快得多。
    下架商品也保留，用来制造"维表缺失"订单。
    """
    with conn.cursor() as cur:
        cur.execute("SELECT product_id, price FROM product_info")
        return {pid: float(price) for pid, price in cur.fetchall()}


def ensure_fact_tables_empty(conn, truncate):
    """事实表非空时默认拒绝执行，避免和历史数据/实时数据打架。"""
    tables = ["order_info", "order_detail", "payment_info", "refund_info"]
    with conn.cursor() as cur:
        counts = {}
        for table in tables:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            counts[table] = cur.fetchone()[0]

        if any(counts.values()):
            if not truncate:
                detail = ", ".join(f"{k}={v}" for k, v in counts.items() if v)
                sys.exit(f"事实表已有数据（{detail}）；重建请加 --truncate")
            for table in tables:
                cur.execute(f"TRUNCATE TABLE {table}")
            print("已清空四张事实表")


def insert_batches(conn, table, columns, rows, chunk_size=2000):
    """分批 executemany，避免单条 SQL 过大；不在这里提交，由调用方按天 commit。"""
    if not rows:
        return
    placeholders = ", ".join(["%s"] * len(columns))
    sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
    with conn.cursor() as cur:
        for start in range(0, len(rows), chunk_size):
            cur.executemany(sql, rows[start:start + chunk_size])


# ---------------------------------------------------------------- 单日造数

def pick_create_time(rng, day_start):
    """在给定当天内按热度分布抽样下单时间，精确到毫秒。"""
    hour = rng.choices(range(24), weights=HOUR_WEIGHTS, k=1)[0]
    return day_start + timedelta(
        hours=hour,
        minutes=rng.randint(0, 59),
        seconds=rng.randint(0, 59),
        milliseconds=rng.randint(0, 999),
    )


def pick_item_count(rng):
    """明细条数：均值约 3.3，与 90 万单 / 300 万明细的口径对齐。"""
    return max(1, min(8, round(rng.gauss(3.3, 1.2))))


def build_day(rng, day_index, day_start, args, pool, prices, now):
    """生成一天的四张表数据。

    Args:
        rng: 随机数发生器。
        day_index: 第几天（1 = 最早，args.days = 昨天）。
        day_start: 当天 00:00。
        args: 命令行参数。
        pool: ID 池。
        prices: product_id -> 售价。
        now: 运行时刻，用于把 update_time / refund_time 截断到过去。

    Returns:
        dict，含 orders / details / payments / refunds 四个行列表。
    """
    orders, details, payments, refunds = [], [], [], []
    user_max = pool["users"]["max"]
    shop_max = pool["shops"]["max"]
    product_ids = list(prices.keys())

    for seq in range(1, args.orders_per_day + 1):
        # order_id 按天连续：resume 时不需要额外状态文件
        order_id = (day_index - 1) * args.orders_per_day + seq
        create_time = pick_create_time(rng, day_start)
        user_id = rng.randint(1, user_max)
        shop_id = rng.randint(1, shop_max)

        item_count = pick_item_count(rng)
        chosen = rng.sample(product_ids, item_count)

        detail_total = 0.0
        item_cnt = 0
        detail_rows = []
        for product_id in chosen:
            num = rng.choices([1, 2, 3], weights=[70, 20, 10], k=1)[0]
            # 成交价围绕当前售价浮动 ±10%，模拟历史调价；明细存快照
            price = round(prices[product_id] * rng.uniform(0.9, 1.1), 2)
            amount = round(price * num, 2)
            detail_total += amount
            item_cnt += num
            detail_rows.append((order_id, product_id, num, price, amount, create_time))

        order_amount = round(detail_total, 2)

        # 金额异常：只改订单金额，支付金额仍按明细合计，制造对账差异
        if rng.random() < args.bad_amount_rate:
            order_amount = 0.0

        order_status = rng.choices(STATUS_CHOICES, weights=STATUS_WEIGHTS, k=1)[0]
        is_deleted = 1 if rng.random() < args.delete_rate else 0

        # 状态推进耗时：支付 1 小时内，履约 1~3 天，逻辑删除更晚
        if order_status == 600:
            update_time = create_time + timedelta(minutes=rng.randint(10, 120))
        else:
            update_time = create_time + timedelta(
                hours=rng.randint(1, 72), minutes=rng.randint(0, 59)
            )
        if is_deleted:
            update_time += timedelta(days=rng.randint(1, 5))
        # 不能出现"未来的更新时间"，否则离线增量抽取会一直捞到脏边界
        update_time = min(update_time, now - timedelta(minutes=rng.randint(1, 30)))

        orders.append((
            order_id, user_id, shop_id, order_amount, item_cnt,
            order_status, is_deleted, create_time, update_time,
        ))
        details.extend(detail_rows)

        # 取消单不支付；其余 94% 生成支付流水
        if order_status == 600:
            continue

        pay_time = create_time + timedelta(
            minutes=rng.randint(1, 60), seconds=rng.randint(0, 59)
        )
        pay_time = min(pay_time, now - timedelta(minutes=rng.randint(1, 30)))
        payments.append((
            order_id,                      # pay_id 与 order_id 一一对应，天然唯一
            order_id,
            round(detail_total, 2),
            rng.choices(PAY_CHANNELS, weights=PAY_CHANNEL_WEIGHTS, k=1)[0],
            1,                             # 1 成功；失败支付不进历史
            pay_time,
            pay_time,
        ))

        # 退款：按订单总量 3%；已删除/未完成/取消的不退
        if order_status != 500 or is_deleted:
            continue
        if rng.random() >= args.refund_rate:
            continue

        refund_time = pay_time + timedelta(
            hours=rng.randint(2, 480), minutes=rng.randint(0, 59)
        )
        refund_time = min(refund_time, now - timedelta(minutes=rng.randint(1, 30)))
        refund_status = 1 if rng.random() < 0.88 else 3
        refunds.append((
            order_id,                      # refund_id 同样复用 order_id
            order_id,
            round(detail_total, 2),
            refund_status,
            rng.choice(REFUND_REASONS),
            refund_time,
            min(refund_time + timedelta(minutes=rng.randint(1, 30)),
                now - timedelta(minutes=1)),
        ))

    return {
        "orders": orders,
        "details": details,
        "payments": payments,
        "refunds": refunds,
    }


# ---------------------------------------------------------------- 主流程

def parse_args():
    """命令行参数。默认值对应 05 第六节的"历史 30 天 × 3 万单"。"""
    parser = argparse.ArgumentParser(description="生成 30 天历史交易数据（只写 MySQL）")
    parser.add_argument("--days", type=int, default=30, help="天数，最后一天为昨天")
    parser.add_argument("--orders-per-day", type=int, default=30000)
    parser.add_argument("--start-day", type=int, default=1, help="断点续跑：从第几天开始")
    parser.add_argument("--refund-rate", type=float, default=0.036,
                        help="退款率（占已完成未删除订单），折算全部订单约 3%%")
    parser.add_argument("--bad-amount-rate", type=float, default=0.002, help="金额异常比例")
    parser.add_argument("--delete-rate", type=float, default=0.005, help="逻辑删除比例")
    parser.add_argument("--seed", type=int, default=20261124)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=3306)
    parser.add_argument("--user", default="root")
    parser.add_argument("--password", default=None, help="建议用环境变量 MYSQL_PASSWORD")
    parser.add_argument("--db", default="trade")
    parser.add_argument("--truncate", action="store_true", help="先清空四张事实表")
    parser.add_argument(
        "--pool",
        default=str(Path(__file__).with_name("id_pool.json")),
        help="ID 池路径",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    rng = random.Random(args.seed)
    pool = load_pool(Path(args.pool))
    conn = connect(args)

    try:
        ensure_fact_tables_empty(conn, args.truncate)
        prices = load_prices(conn)
        print(f"商品价格已载入 {len(prices)} 条，开始生成 {args.days} 天历史")

        # 最后一天固定为"昨天"，保证 T-1 是完整的一天
        now = datetime.now()
        today = now.replace(hour=0, minute=0, second=0, microsecond=0)
        latest = today - timedelta(days=1)

        started = time.time()
        for day_index in range(args.start_day, args.days + 1):
            day_start = latest - timedelta(days=args.days - day_index)
            batch = build_day(rng, day_index, day_start, args, pool, prices, now)

            insert_batches(
                conn, "order_info",
                ["order_id", "user_id", "shop_id", "order_amount", "item_cnt",
                 "order_status", "is_deleted", "create_time", "update_time"],
                batch["orders"],
            )
            insert_batches(
                conn, "order_detail",
                ["order_id", "product_id", "num", "price", "amount", "create_time"],
                batch["details"],
            )
            insert_batches(
                conn, "payment_info",
                ["pay_id", "order_id", "pay_amount", "pay_channel", "pay_status",
                 "pay_time", "update_time"],
                batch["payments"],
            )
            insert_batches(
                conn, "refund_info",
                ["refund_id", "order_id", "refund_amount", "refund_status",
                 "refund_reason", "refund_time", "update_time"],
                batch["refunds"],
            )

            # 按天提交：中途失败时前面的天已落库，--start-day 可以接着跑
            conn.commit()
            elapsed = time.time() - started
            print(
                f"day {day_index:02d}/{args.days} {day_start:%Y-%m-%d} "
                f"订单 {len(batch['orders'])} 明细 {len(batch['details'])} "
                f"支付 {len(batch['payments'])} 退款 {len(batch['refunds'])} "
                f"累计 {elapsed:.1f}s"
            )

        # 回读核对：总行数与关键异常量
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM order_info")
            print(f"校验 order_info: {cur.fetchone()[0]} 行")
            cur.execute("SELECT COUNT(*) FROM order_detail")
            print(f"校验 order_detail: {cur.fetchone()[0]} 行")
            cur.execute("SELECT COUNT(*) FROM payment_info")
            print(f"校验 payment_info: {cur.fetchone()[0]} 行")
            cur.execute("SELECT COUNT(*) FROM refund_info")
            print(f"校验 refund_info: {cur.fetchone()[0]} 行")
            cur.execute("SELECT COUNT(*) FROM order_info WHERE order_amount <= 0")
            print(f"金额异常单: {cur.fetchone()[0]} 行")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
