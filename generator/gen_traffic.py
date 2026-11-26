# -*- coding: utf-8 -*-
"""
用途：持续向 Kafka `ods_traffic` 写用户行为埋点（浏览 / 加购 / 下单点击），
     并按 docs/05 第五节注入异常，给 J4 的清洗与去重规则提供真实靶子。
对应文档：docs/05-数据源与造数.md 4.3 / 五 / 六、docs/03-数据模型.md 5.2、docs/06 三节
执行：
    python3 generator/gen_traffic.py --qps 200
    python3 generator/gen_traffic.py --qps 2000 --duration 1800   # 压测 30 分钟
输出：
    Kafka topic `ods_traffic`，消息为单行 JSON，key 为 user_id
依赖：
    - 先跑 gen_master.py（读 id_pool.json，保证 user_id / product_id 命中 MySQL）
    - kafka-python（WSL 内已装）
说明：
    - 常驻进程，Ctrl+C 优雅退出并打印汇总；--duration 到点自动退出
    - 事件比例：浏览 90% / 加购 8% / 下单点击 2%，与交易数据量级对齐
    - 异常注入（默认值，均可调）：
        重复      5%   同一 event_id 原样发两次（Q08 靶子，J4 按主键去重）
        迟到      1%   event_time 比发送时间早 5~30 分钟，事件本身合法（只影响实时性）
        非法枚举  0.1% event_type 写一个不存在的值（Q05 靶子）
        越界时间  0.1% event_time 早于 30 天（Q04 靶子；只往历史偏，不往未来——未来会把水印推飞，见 E38）
        金额异常  不适用（埋点没有金额字段）
    - 异常互斥：一条事件只注入一种异常，避免脏数据里原因说不清；重复只对合法事件做
    - 会话：同一用户 30 分钟内有新事件就复用 session_id，超过 30 分钟切新会话
"""

import argparse
import json
import random
import signal
import sys
import time
from pathlib import Path

from kafka import KafkaProducer

# ---------------------------------------------------------------- 静态配置

# 事件类型与占比，对应 docs/05 4.3
EVENT_TYPES = ["page_view", "add_cart", "order_submit"]
EVENT_WEIGHTS = [90, 8, 2]

# 非法枚举值：明显不属于三种合法类型，用来触发 Q05
BAD_EVENT_TYPES = ["click_buy", "cart_add", "buy_now", "unknown"]

SESSION_IDLE_MS = 30 * 60 * 1000       # 30 分钟无操作切新会话
PRODUCT_MISS_RATE = 0.2                # 首页浏览不带商品，占浏览事件的 20%


# ---------------------------------------------------------------- 基础工具

def load_pool(path):
    """读取 gen_master 产出的 ID 池；缺文件直接退出，不瞎猜取值范围。"""
    if not path.exists():
        sys.exit(f"找不到 {path}，请先运行 gen_master.py")
    return json.loads(path.read_text(encoding="utf-8"))


def build_session(rng, sessions, user_id, now_ms):
    """按"30 分钟无操作"维护会话，返回 session_id。

    sessions 只存 (session_id, 最后事件时间)。用户量 10 万，dict 常驻内存约几十 MB；
    事件按用户随机散开，绝大多数用户两次事件间隔超过 30 分钟，天然会切新会话。
    """
    last = sessions.get(user_id)
    if last and now_ms - last[1] <= SESSION_IDLE_MS:
        session_id = last[0]
    else:
        session_id = f"s-{user_id}-{rng.getrandbits(24):06x}"
    sessions[user_id] = (session_id, now_ms)
    return session_id


def build_event(rng, pool, seq, run_prefix, sessions, counters):
    """构造一条埋点事件，并按比例注入异常。

    返回 (event, anomaly)，anomaly 取值：None / dup / late / bad_type / bad_time。
    """
    now_ms = int(time.time() * 1000)
    user_id = rng.randint(pool["users"]["min"], pool["users"]["max"])
    event_type = rng.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0]

    product_id = None
    if event_type != "page_view" or rng.random() >= PRODUCT_MISS_RATE:
        product_id = rng.randint(pool["products"]["min"], pool["products"]["max"])

    event_time = now_ms
    session_id = build_session(rng, sessions, user_id, now_ms)

    anomaly = None
    # 异常互斥，按"破坏性从大到小"判定
    if rng.random() < counters["rate_bad_type"]:
        event_type = rng.choice(BAD_EVENT_TYPES)
        anomaly = "bad_type"
    elif rng.random() < counters["rate_bad_time"]:
        # 只往历史越界（31~45 天前，仍在 -90 天动态分区内）。
        # 不要往未来偏：未来时间的事件会被 WHERE 过滤掉，但水印定义在源表上、先于过滤生效，
        # 水印一旦被推到未来，之后所有正常事件都变成"迟到"，分钟窗口直接停摆（E38）
        event_time = now_ms - rng.randint(31, 45) * 24 * 3600 * 1000
        anomaly = "bad_time"
    elif rng.random() < counters["rate_late"]:
        event_time = now_ms - rng.randint(5, 1800) * 1000
        anomaly = "late"
    elif rng.random() < counters["rate_dup"]:
        anomaly = "dup"

    event = {
        "event_id": f"e-{run_prefix}-{seq:09d}",
        "user_id": user_id,
        "product_id": product_id,
        "event_type": event_type,
        "event_time": event_time,
        "session_id": session_id,
    }
    return event, anomaly


def send(producer, topic, event, on_error):
    """按 user_id 做 key 发送，保证同一用户的事件在同一分区内有序。"""
    producer.send(
        topic,
        key=str(event["user_id"]).encode("utf-8"),
        value=json.dumps(event, ensure_ascii=False).encode("utf-8"),
    ).add_errback(on_error)


# ---------------------------------------------------------------- 主流程

def parse_args():
    """命令行参数。--qps 默认值对应 docs/05 第六节的"日常 200 QPS"。"""
    parser = argparse.ArgumentParser(description="持续向 Kafka 写埋点流")
    parser.add_argument("--qps", type=float, default=200.0, help="每秒逻辑事件数")
    parser.add_argument("--duration", type=int, default=0,
                        help="持续秒数；0 表示一直跑到 Ctrl+C")
    parser.add_argument("--dup-rate", type=float, default=0.05, help="重复事件比例")
    parser.add_argument("--late-rate", type=float, default=0.01, help="迟到事件比例")
    parser.add_argument("--bad-type-rate", type=float, default=0.001,
                        help="非法 event_type 比例（Q05）")
    parser.add_argument("--bad-time-rate", type=float, default=0.001,
                        help="event_time 越界比例（Q04）")
    parser.add_argument("--seed", type=int, default=20261125)
    parser.add_argument("--bootstrap-servers", default="127.0.0.1:9092")
    parser.add_argument("--topic", default="ods_traffic")
    parser.add_argument(
        "--pool",
        default=str(Path(__file__).with_name("id_pool.json")),
        help="ID 池路径（gen_master.py 产出）",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    rng = random.Random(args.seed)
    pool = load_pool(Path(args.pool))

    counters = {
        "rate_dup": args.dup_rate,
        "rate_late": args.late_rate,
        "rate_bad_type": args.bad_type_rate,
        "rate_bad_time": args.bad_time_rate,
        # 运行统计
        "logical": 0,
        "sent": 0,
        "dup": 0,
        "late": 0,
        "bad_type": 0,
        "bad_time": 0,
        "error": 0,
    }
    sessions = {}
    first_error = []

    def on_error(exc):
        """记录第一条发送错误，其余只计数，避免刷屏。"""
        counters["error"] += 1
        if not first_error:
            first_error.append(repr(exc))

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        acks="all",
        linger_ms=20,
        batch_size=16384,
        max_block_ms=10000,
    )

    running = {"value": True}

    def stop(signum, _frame):
        # SIGTERM 也走优雅退出，方便脚本 kill
        running["value"] = False

    signal.signal(signal.SIGTERM, stop)

    run_prefix = time.strftime("%Y%m%d%H%M%S")
    started = time.monotonic()
    next_report = started + 10
    next_slot = started
    interval = 1.0 / args.qps if args.qps > 0 else 0

    print(
        f"埋点生成器启动：qps={args.qps} topic={args.topic} "
        f"dup={args.dup_rate} late={args.late_rate} "
        f"bad_type={args.bad_type_rate} bad_time={args.bad_time_rate}"
    )

    try:
        while running["value"]:
            now = time.monotonic()
            elapsed = now - started
            if args.duration and elapsed >= args.duration:
                break

            if now < next_slot:
                time.sleep(min(next_slot - now, 0.05))
                continue

            counters["logical"] += 1
            event, anomaly = build_event(
                rng, pool, counters["logical"], run_prefix, sessions, counters
            )
            send(producer, args.topic, event, on_error)
            counters["sent"] += 1

            if anomaly == "dup":
                # 同一 event_id 原样重发一次，模拟客户端重试
                send(producer, args.topic, event, on_error)
                counters["sent"] += 1
                counters["dup"] += 1
            elif anomaly:
                counters[anomaly] += 1

            next_slot += interval
            # 进程被挂起或机器卡顿后不要"补发"一大串旧事件
            if next_slot < now - 1:
                next_slot = now

            if now >= next_report:
                print(
                    f"[{elapsed:6.1f}s] 事件 {counters['logical']} "
                    f"({counters['logical'] / max(elapsed, 1):.1f}/s) "
                    f"消息 {counters['sent']} 重复 {counters['dup']} "
                    f"迟到 {counters['late']} 非法类型 {counters['bad_type']} "
                    f"越界时间 {counters['bad_time']} 发送错误 {counters['error']}"
                )
                next_report += 10
    except KeyboardInterrupt:
        print("\n收到 Ctrl+C，正在刷出缓冲区…")
    finally:
        producer.flush(timeout=15)
        producer.close(timeout=15)
        elapsed = time.monotonic() - started
        print(
            f"已停止：运行 {elapsed:.1f}s，逻辑事件 {counters['logical']}，"
            f"实际消息 {counters['sent']}（重复 {counters['dup']}），"
            f"迟到 {counters['late']}，非法类型 {counters['bad_type']}，"
            f"越界时间 {counters['bad_time']}，发送错误 {counters['error']}"
        )
        if first_error:
            print(f"首个发送错误：{first_error[0]}")
        if counters["error"]:
            sys.exit(1)


if __name__ == "__main__":
    main()
