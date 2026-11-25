# -*- coding: utf-8 -*-
"""
用途：持续生成实时交易数据——按状态机推进订单生命周期（下单 → 支付 → 发货 → 签收 → 完成，
     旁支退款），只写 MySQL；J1 的 MySQL CDC 抓 binlog 后进 Kafka，供 J2/J3 实时消费。
对应文档：docs/05-数据源与造数.md 4.2 / 五 / 六、docs/06 数据质量与对账
执行：
    MYSQL_PASSWORD=xxx python3 generator/gen_realtime.py
    MYSQL_PASSWORD=xxx python3 generator/gen_realtime.py --speed 10 --orders-per-min 20
    MYSQL_PASSWORD=xxx python3 generator/gen_realtime.py --duration 60 --speed 300 --late-rate 0.2
输出：
    MySQL trade.order_info / order_detail / payment_info / refund_info（每步一条真实 SQL）
依赖：
    - 先跑 gen_master.py（读 id_pool.json 与商品价格）
    - J1（CDC → Kafka）在跑，否则 MySQL 的变更不会进流
    - pymysql
说明：
    - 默认参数对应 docs/05 第六节"日常实时 20 单/分钟"；--speed 是业务时间的加速倍率
    - **--speed 只压缩"等待间隔"，写库时间戳仍用真实时间**。如果把 create_time 写成
      业务时间（未来），J3 的分钟窗口会被未来水印提前推高（与 08 E27 同源），所以不这么干
    - 异常注入（均可调，一条订单只命中一种"主"异常，便于对账时归因）：
        支付回调迟到  --late-rate      1%    pay_time 正常，但 payment 行晚 5~15 分钟业务时间才落库
        状态乱序      --disorder-rate  0.5%  先写"已支付"，2 秒后再写一条 update_time 更早的"待支付"
        金额异常      --bad-amount-rate 0.2% order_amount 写 0（明细金额正常，制造对账差异）
        维表缺失      --dim-miss-rate  2%    订单里带一个 status=0 的下架商品
        退款旁支      --refund-rate    3%    refund_info 先 2 处理中，再 1 成功 / 3 拒绝
    - 生命周期（--speed 10 时）：支付 +3~12 秒、发货 +30~90 秒、签收 +3~6 分钟、完成 +6 分钟
    - 常驻进程，Ctrl+C / SIGTERM 优雅退出并打印汇总；--duration 到点自动退出
    - 订单号用"启动毫秒 ×1000 + 序号"，重启不与历史单（1~90 万）和冒烟单（9 亿段）相撞
"""

import argparse
import heapq
import json
import os
import random
import signal
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import pymysql

# ---------------------------------------------------------------- 静态配置

PAY_CHANNELS = ["alipay", "wechat", "card"]
PAY_CHANNEL_WEIGHTS = [55, 40, 5]

REFUND_REASONS = ["七天无理由", "质量问题", "发错货", "尺寸不符", "不想要了"]

# 一次动作对应的状态值，对应 sql/01 注释
STATUS_PENDING = 100
STATUS_PAID = 200
STATUS_SHIPPED = 300
STATUS_RECEIVED = 400
STATUS_DONE = 500

# 业务时间（秒），折算真实等待时除以 --speed
PAY_DELAY = (30, 120)            # 下单 → 支付
LATE_DELAY = (5 * 60, 15 * 60)   # 支付回调迟到
SHIP_DELAY = (5 * 60, 15 * 60)   # 支付 → 发货
RECEIVE_DELAY = (30 * 60, 60 * 60)   # 发货 → 签收
DONE_DELAY = (60 * 60, 60 * 60)      # 签收 → 完成
REFUND_APPLY_DELAY = (60, 10 * 60)   # 支付 → 退款申请
REFUND_RESULT_DELAY = (60, 30 * 60)  # 申请 → 处理结果


# ---------------------------------------------------------------- 基础工具

def parse_args():
    """命令行参数，默认值对应 docs/05 第六节"日常实时"。"""
    parser = argparse.ArgumentParser(description="持续生成实时交易状态机数据（只写 MySQL）")
    parser.add_argument("--speed", type=float, default=10.0,
                        help="业务时间加速倍率，只影响等待间隔，不影响落库时间戳")
    parser.add_argument("--orders-per-min", type=float, default=20.0,
                        help="每个业务分钟产生多少订单")
    parser.add_argument("--duration", type=int, default=0,
                        help="运行真实秒数；0 表示一直跑到 Ctrl+C")
    parser.add_argument("--late-rate", type=float, default=0.01, help="支付回调迟到比例")
    parser.add_argument("--disorder-rate", type=float, default=0.005, help="状态乱序比例")
    parser.add_argument("--bad-amount-rate", type=float, default=0.002, help="金额异常比例")
    parser.add_argument("--dim-miss-rate", type=float, default=0.02,
                        help="订单引用下架商品的比例（维表缺失靶子）")
    parser.add_argument("--refund-rate", type=float, default=0.03, help="支付后退款比例")
    parser.add_argument("--seed", type=int, default=20261125)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=3306)
    parser.add_argument("--user", default="root")
    parser.add_argument("--password", default=None, help="建议用环境变量 MYSQL_PASSWORD")
    parser.add_argument("--db", default="trade")
    parser.add_argument(
        "--pool",
        default=str(Path(__file__).with_name("id_pool.json")),
        help="ID 池路径（gen_master.py 产出）",
    )
    return parser.parse_args()


def load_pool(path):
    """读 gen_master 的 ID 池；缺文件直接退出，不瞎猜 ID 范围。"""
    if not path.exists():
        sys.exit(f"找不到 {path}，请先运行 gen_master.py")
    return json.loads(path.read_text(encoding="utf-8"))


def connect(args):
    """连接 MySQL。自动提交：每一步都要立刻可见，CDC 才能及时抓到。"""
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
        autocommit=True,
    )


def load_products(conn):
    """载入商品价格与上/下架集合。

    50k 行约 2 MB，一次读完；下架商品单独存一份，用来按比例造"维表缺失"订单。
    """
    with conn.cursor() as cur:
        cur.execute("SELECT product_id, price, status FROM product_info")
        rows = cur.fetchall()
    prices = {pid: float(price) for pid, price, _ in rows}
    off_shelf = [pid for pid, _, status in rows if status == 0]
    return prices, off_shelf


def now_dt():
    """当前真实时间，毫秒精度（与 MySQL DATETIME(3) 对齐）。"""
    dt = datetime.now()
    return dt.replace(microsecond=dt.microsecond // 1000 * 1000)


def real_delay(biz_seconds, speed):
    """业务秒 → 真实秒。speed=10 时 10 分钟的业务等待只等 1 分钟。"""
    return max(0.05, biz_seconds / max(speed, 0.1))


# ---------------------------------------------------------------- 状态机动作

class Runner:
    """单线程调度器：时间堆 + 每步一次真实 SQL。

    20 单/分钟的量级下，没必要上线程池；单线程顺序执行反而保证同一订单的状态推进有序。
    """

    def __init__(self, args, rng, pool, prices, off_shelf, conn):
        self.args = args
        self.rng = rng
        self.pool = pool
        self.prices = prices
        self.off_shelf = off_shelf
        self.conn = conn
        self.heap = []          # (due_monotonic, seq, kind, order_id, payload)
        self.seq = 0
        self.order_seq = 0
        self.start_ms = int(time.time() * 1000)
        self.counters = {
            "orders": 0, "details": 0, "payments": 0, "refunds": 0,
            "late": 0, "disorder": 0, "bad_amount": 0, "dim_miss": 0,
            "error": 0,
        }
        self.first_error = None

    # ------------------------------------------------------------ SQL 执行

    def execute(self, sql, params=()):
        """执行一条 SQL；失败时重连一次，仍失败只计数不退出（生成器不该被单条 SQL 打死）。"""
        try:
            with self.conn.cursor() as cur:
                cur.execute(sql, params)
            return True
        except pymysql.Error as exc:
            self.counters["error"] += 1
            if self.first_error is None:
                self.first_error = repr(exc)
            try:
                self.conn.ping(reconnect=True)
            except pymysql.Error:
                pass
            return False

    # ------------------------------------------------------------ 调度

    def at(self, biz_seconds, kind, order_id, payload=None):
        """把一个动作排进堆；biz_seconds 是业务等待，按 speed 折算成真实秒。"""
        due = time.monotonic() + real_delay(biz_seconds, self.args.speed)
        self.seq += 1
        heapq.heappush(self.heap, (due, self.seq, kind, order_id, payload))

    # ------------------------------------------------------------ 造单

    def build_order(self):
        """生成一笔订单（明细 + 金额），按比例注入金额异常 / 维表缺失。"""
        args = self.args
        rng = self.rng
        self.order_seq += 1
        order_id = self.start_ms * 1000 + self.order_seq
        create_time = now_dt()

        user_id = rng.randint(1, self.pool["users"]["max"])
        shop_id = rng.randint(1, self.pool["shops"]["max"])

        item_count = max(1, min(5, round(rng.gauss(2.0, 0.8))))
        product_ids = list(self.prices.keys())
        chosen = rng.sample(product_ids, min(item_count, len(product_ids)))

        # 维表缺失：把一个明细商品换成下架商品（有下架商品时才注入）
        dim_miss = False
        if self.off_shelf and rng.random() < args.dim_miss_rate:
            chosen[0] = rng.choice(self.off_shelf)
            dim_miss = True

        detail_rows = []
        total = 0.0
        item_cnt = 0
        for product_id in chosen:
            num = rng.choices([1, 2, 3], weights=[75, 18, 7], k=1)[0]
            price = round(self.prices[product_id] * rng.uniform(0.95, 1.05), 2)
            amount = round(price * num, 2)
            total += amount
            item_cnt += num
            detail_rows.append((order_id, product_id, num, price, amount, create_time))

        order_amount = round(total, 2)
        bad_amount = False
        if rng.random() < args.bad_amount_rate:
            order_amount = 0.0
            bad_amount = True

        self.execute(
            "INSERT INTO order_info "
            "(order_id, user_id, shop_id, order_amount, item_cnt, order_status, "
            " is_deleted, create_time, update_time) "
            "VALUES (%s, %s, %s, %s, %s, %s, 0, %s, %s)",
            (order_id, user_id, shop_id, order_amount, item_cnt,
             STATUS_PENDING, create_time, create_time),
        )
        for row in detail_rows:
            self.execute(
                "INSERT INTO order_detail (order_id, product_id, num, price, amount, create_time) "
                "VALUES (%s, %s, %s, %s, %s, %s)", row,
            )

        self.counters["orders"] += 1
        self.counters["details"] += len(detail_rows)
        if bad_amount:
            self.counters["bad_amount"] += 1
        if dim_miss:
            self.counters["dim_miss"] += 1

        return order_id, create_time, round(total, 2), detail_rows

    # ------------------------------------------------------------ 生命周期

    def create_order(self):
        """造单并按策略决定后续：正常支付 / 迟到支付 / 状态乱序 / 直接已支付。"""
        args = self.args
        rng = self.rng
        order_id, create_time, pay_amount, _ = self.build_order()

        # 状态乱序：直接写"已支付"（含支付流水），2 秒后再补一条 update_time 更早的"待支付"
        if rng.random() < args.disorder_rate:
            self.execute(
                "UPDATE order_info SET order_status = %s WHERE order_id = %s",
                (STATUS_PAID, order_id),
            )
            self.insert_payment(order_id, pay_amount, create_time)
            self.at(2, "disorder_stale", order_id, create_time)
            self.at(4, "disorder_restore", order_id)
            self.counters["disorder"] += 1
            self.maybe_schedule_refund(order_id, pay_amount)
            return

        # 支付回调迟到：按正常时间点应支付，但 payment 行晚 5~15 分钟业务时间才落库；
        # pay_time 写成"应支付时刻"（真实时间回退），制造消息晚于事件时间的靶子
        if rng.random() < args.late_rate:
            delay = rng.randint(*LATE_DELAY)
            self.at(delay, "pay_late", order_id, (pay_amount, create_time, delay))
            self.counters["late"] += 1
            return

        self.at(rng.randint(*PAY_DELAY), "pay", order_id, (pay_amount, create_time))

    def insert_payment(self, order_id, pay_amount, pay_time):
        """写支付流水。pay_id 复用 order_id，天然唯一。"""
        self.execute(
            "INSERT INTO payment_info "
            "(pay_id, order_id, pay_amount, pay_channel, pay_status, pay_time, update_time) "
            "VALUES (%s, %s, %s, %s, 1, %s, %s)",
            (order_id, order_id, pay_amount,
             self.rng.choices(PAY_CHANNELS, weights=PAY_CHANNEL_WEIGHTS, k=1)[0],
             pay_time, pay_time),
        )
        self.counters["payments"] += 1

    def mark_paid(self, order_id):
        self.execute(
            "UPDATE order_info SET order_status = %s WHERE order_id = %s",
            (STATUS_PAID, order_id),
        )

    def maybe_schedule_refund(self, order_id, pay_amount):
        """支付后按比例排退款：先 2 处理中，再随机 1 成功 / 3 拒绝。"""
        if self.rng.random() >= self.args.refund_rate:
            return
        self.at(self.rng.randint(*REFUND_APPLY_DELAY), "refund_apply", order_id,
                (pay_amount,))

    # ------------------------------------------------------------ 动作执行

    def run_action(self, kind, order_id, payload):
        rng = self.rng
        if kind == "pay":
            pay_amount, create_time = payload
            pay_time = now_dt()
            # 防御：pay_time 不能早于 create_time（迟到单也不会出现时间倒挂）
            if pay_time < create_time:
                pay_time = create_time
            self.insert_payment(order_id, pay_amount, pay_time)
            self.mark_paid(order_id)
            self.maybe_schedule_refund(order_id, pay_amount)
            self.at(rng.randint(*SHIP_DELAY), "ship", order_id)

        elif kind == "pay_late":
            pay_amount, create_time, delay = payload
            now = now_dt()
            # pay_time 回退到"本来应该支付"的时刻；晚到的是消息，不是事件时间
            pay_time = now - timedelta(seconds=delay / max(self.args.speed, 0.1))
            if pay_time < create_time:
                pay_time = create_time
            self.insert_payment(order_id, pay_amount, pay_time)
            self.mark_paid(order_id)
            self.maybe_schedule_refund(order_id, pay_amount)
            self.at(rng.randint(*SHIP_DELAY), "ship", order_id)

        elif kind == "disorder_stale":
            # update_time 早 60 秒的"待支付"迟到消息。DWD 是 Unique Key + MoW、
            # 不设 sequence，所以这条会先覆盖成 100，2 秒后 restore 再覆盖回 200；
            # 下游 J3 会看到一次 新(200) → 旧(100) → 新(200)，正是对账要抓的靶子。
            create_time = payload
            stale_time = create_time - timedelta(seconds=60)
            self.execute(
                "UPDATE order_info SET order_status = %s, update_time = %s WHERE order_id = %s",
                (STATUS_PENDING, stale_time, order_id),
            )

        elif kind == "disorder_restore":
            # 补回正确状态，保证 MySQL 源表本身是对的；下游看到的是 新(200) → 旧(100) → 新(200)
            self.execute(
                "UPDATE order_info SET order_status = %s, update_time = %s WHERE order_id = %s",
                (STATUS_PAID, now_dt(), order_id),
            )

        elif kind == "ship":
            self.execute(
                "UPDATE order_info SET order_status = %s WHERE order_id = %s",
                (STATUS_SHIPPED, order_id),
            )
            self.at(rng.randint(*RECEIVE_DELAY), "receive", order_id)

        elif kind == "receive":
            self.execute(
                "UPDATE order_info SET order_status = %s WHERE order_id = %s",
                (STATUS_RECEIVED, order_id),
            )
            self.at(rng.randint(*DONE_DELAY), "done", order_id)

        elif kind == "done":
            self.execute(
                "UPDATE order_info SET order_status = %s WHERE order_id = %s",
                (STATUS_DONE, order_id),
            )

        elif kind == "refund_apply":
            pay_amount = payload[0]
            refund_time = now_dt()
            self.execute(
                "INSERT INTO refund_info "
                "(refund_id, order_id, refund_amount, refund_status, refund_reason, "
                " refund_time, update_time) "
                "VALUES (%s, %s, %s, 2, %s, %s, %s)",
                (order_id, order_id, pay_amount, rng.choice(REFUND_REASONS),
                 refund_time, refund_time),
            )
            self.counters["refunds"] += 1
            self.at(rng.randint(*REFUND_RESULT_DELAY), "refund_result", order_id,
                    (pay_amount,))

        elif kind == "refund_result":
            status = 1 if rng.random() < 0.88 else 3
            self.execute(
                "UPDATE refund_info SET refund_status = %s, update_time = %s "
                "WHERE refund_id = %s",
                (status, now_dt(), order_id),
            )

    # ------------------------------------------------------------ 主循环

    def run(self):
        args = self.args
        started = time.monotonic()
        next_create = started
        next_report = started + 10
        interval = 60.0 / max(args.speed * args.orders_per_min, 0.01)

        running = {"value": True}

        def stop(_signum, _frame):
            running["value"] = False

        signal.signal(signal.SIGTERM, stop)
        signal.signal(signal.SIGINT, stop)

        print(
            f"实时交易生成器启动：speed={args.speed} orders_per_min={args.orders_per_min} "
            f"late={args.late_rate} disorder={args.disorder_rate} "
            f"bad_amount={args.bad_amount_rate} dim_miss={args.dim_miss_rate} "
            f"refund={args.refund_rate} 上架商品 {len(self.prices)} 下架 {len(self.off_shelf)}"
        )

        try:
            while running["value"]:
                now = time.monotonic()
                elapsed = now - started
                if args.duration and elapsed >= args.duration:
                    break

                # 1) 到期动作
                while self.heap and self.heap[0][0] <= now:
                    _, _, kind, order_id, payload = heapq.heappop(self.heap)
                    self.run_action(kind, order_id, payload)

                # 2) 造单（按业务分钟摊平；卡顿后不补发，避免瞬时洪峰）
                if now >= next_create:
                    self.create_order()
                    next_create += interval
                    if next_create < now - 1:
                        next_create = now + interval

                # 3) 汇报
                if now >= next_report:
                    c = self.counters
                    print(
                        f"[{elapsed:6.1f}s] 订单 {c['orders']} 明细 {c['details']} "
                        f"支付 {c['payments']} 退款 {c['refunds']} "
                        f"迟到 {c['late']} 乱序 {c['disorder']} "
                        f"金额异常 {c['bad_amount']} 维表缺失 {c['dim_miss']} "
                        f"错误 {c['error']}"
                    )
                    next_report += 10

                # 4) 睡到下一个动作或下一次造单，最长 0.2 秒（保证 Ctrl+C 及时）
                wake = next_create
                if self.heap:
                    wake = min(wake, self.heap[0][0])
                time.sleep(max(0.0, min(wake - time.monotonic(), 0.2)))
        except KeyboardInterrupt:
            print("\n收到 Ctrl+C，退出中…")
        except pymysql.Error as exc:
            print(f"\n数据库连接异常：{exc!r}")
            self.counters["error"] += 1

        c = self.counters
        print(
            f"汇总：订单 {c['orders']} 明细 {c['details']} 支付 {c['payments']} "
            f"退款 {c['refunds']} 迟到 {c['late']} 乱序 {c['disorder']} "
            f"金额异常 {c['bad_amount']} 维表缺失 {c['dim_miss']} 错误 {c['error']}"
        )
        if self.first_error:
            print(f"第一条错误：{self.first_error}")


# ---------------------------------------------------------------- 入口

def main():
    args = parse_args()
    rng = random.Random(args.seed)
    pool = load_pool(Path(args.pool))
    conn = connect(args)
    try:
        prices, off_shelf = load_products(conn)
        if not prices:
            sys.exit("product_info 为空，请先运行 gen_master.py")
        Runner(args, rng, pool, prices, off_shelf, conn).run()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
