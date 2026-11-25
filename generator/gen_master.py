# -*- coding: utf-8 -*-
"""
用途：生成电商数仓的维表主数据，并产出三端共用的 ID 池。
对应文档：docs/05-数据源与造数.md 4.1 / 4.3 / 五（维表缺失注入）
执行：
    MYSQL_PASSWORD=xxx python3 generator/gen_master.py \
        --users 100000 --products 50000 --shops 2000
输出：
    1) MySQL trade.shop_info / user_info / product_info
    2) generator/id_pool.json —— 后三个生成器读它，保证埋点与 MySQL 的 ID 对得上
依赖：Python 3.10+、pymysql
说明：
    - 一次性脚本，可重复执行；默认遇到非空表会退出，需显式加 --truncate 才重建
    - 2% 的商品故意置 status=0（下架），作为"维表缺失"规则的靶子
    - 随机种子固定，同样参数生成的数据一致，便于复现
"""

import argparse
import json
import os
import random
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pymysql

# ---------------------------------------------------------------- 静态字典

# 省份 -> 城市，维表里的地域分布从这里取，保证 city 与 province 不矛盾
PROVINCE_CITIES = {
    "江苏": ["南京", "苏州", "无锡", "常州"],
    "浙江": ["杭州", "宁波", "温州", "嘉兴"],
    "广东": ["广州", "深圳", "东莞", "佛山"],
    "四川": ["成都", "绵阳"],
    "湖北": ["武汉", "宜昌"],
    "北京": ["北京"],
    "上海": ["上海"],
    "山东": ["济南", "青岛"],
    "福建": ["福州", "厦门"],
    "陕西": ["西安"],
}

# 一级类目，product_info.category_id 从这里取
CATEGORIES = [
    (1001, "数码"),
    (1002, "服饰"),
    (1003, "家居"),
    (1004, "美妆"),
    (1005, "食品"),
    (1006, "母婴"),
    (1007, "运动"),
    (1008, "图书"),
    (1009, "家电"),
    (1010, "汽车"),
]

# 类目 -> 商品名词根，用于拼出"看起来像真的"商品名
CATEGORY_WORDS = {
    1001: ["手机", "耳机", "平板", "充电宝", "键盘"],
    1002: ["T恤", "牛仔裤", "卫衣", "连衣裙", "外套"],
    1003: ["四件套", "收纳箱", "台灯", "抱枕", "置物架"],
    1004: ["面霜", "口红", "洗发水", "面膜", "香水"],
    1005: ["坚果", "咖啡", "牛奶", "饼干", "茶叶"],
    1006: ["纸尿裤", "奶瓶", "辅食", "童装", "推车"],
    1007: ["跑鞋", "瑜伽垫", "哑铃", "泳镜", "球拍"],
    1008: ["小说", "教材", "绘本", "词典", "杂志"],
    1009: ["冰箱", "洗衣机", "微波炉", "空调", "吸尘器"],
    1010: ["行车记录仪", "脚垫", "车载支架", "玻璃水", "座套"],
}

SHOP_TYPES = ["旗舰店", "专卖店", "专营店"]
BRANDS = ["启元", "北岸", "青禾", "云栖", "南屿", "长川", "日和", "新序"]


# ---------------------------------------------------------------- 工具函数

def connect(args):
    """建立 MySQL 连接。

    Args:
        args: argparse 解析结果，含 host/port/user/password/db。

    Returns:
        pymysql.Connection，autocommit 关闭，按表提交。
    """
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


def ensure_empty(conn, tables, truncate):
    """确认目标表可用：非空时默认拒绝执行，防止把已有数据搅乱。

    Args:
        conn: MySQL 连接。
        tables: 表名列表。
        truncate: True 时先清空。
    """
    with conn.cursor() as cur:
        for table in tables:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            count = cur.fetchone()[0]
            if count == 0:
                continue
            if not truncate:
                sys.exit(f"{table} 已有 {count} 行；若要重建请加 --truncate")
            cur.execute(f"TRUNCATE TABLE {table}")
            print(f"已清空 {table}（原有 {count} 行）")


def insert_batches(conn, table, columns, rows, chunk_size=2000):
    """分批写入，避免一次性构造过大的 SQL。

    Args:
        conn: MySQL 连接。
        table: 目标表。
        columns: 列名列表。
        rows: 行数据（tuple 列表），顺序与 columns 一致。
        chunk_size: 每批行数。
    """
    if not rows:
        return

    placeholders = ", ".join(["%s"] * len(columns))
    sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
    total = len(rows)

    with conn.cursor() as cur:
        for start in range(0, total, chunk_size):
            batch = rows[start:start + chunk_size]
            cur.executemany(sql, batch)
            print(f"  {table}: {min(start + chunk_size, total)}/{total}")
    conn.commit()


# ---------------------------------------------------------------- 三张维表

def build_shops(rng, count):
    """生成店铺维表行。

    店铺创建时间落在近两年内，update_time 不早于 create_time。
    """
    now = datetime.now()
    rows = []
    for shop_id in range(1, count + 1):
        province, cities = rng.choice(list(PROVINCE_CITIES.items()))
        created = now - timedelta(days=rng.randint(0, 730), seconds=rng.randint(0, 86399))
        updated = min(created + timedelta(days=rng.randint(0, 30)), now)
        rows.append((
            shop_id,
            f"{rng.choice(BRANDS)}{rng.choice(['优品', '精选', '生活', '好物'])}{rng.randint(1, 999)}店",
            rng.choice(SHOP_TYPES),
            province,
            rng.choice(cities),
            created,
            updated,
        ))
    return rows


def build_users(rng, count):
    """生成用户维表行。

    age_range 由 age 推导，避免两者对不上；user_level 按 40/30/18/9/3 分布。
    """
    now = datetime.now()
    levels = [1] * 40 + [2] * 30 + [3] * 18 + [4] * 9 + [5] * 3
    rows = []
    for user_id in range(1, count + 1):
        age = rng.randint(18, 55)
        if age <= 24:
            age_range = "18-24"
        elif age <= 34:
            age_range = "25-34"
        elif age <= 44:
            age_range = "35-44"
        else:
            age_range = "45+"

        province, cities = rng.choice(list(PROVINCE_CITIES.items()))
        registered = now - timedelta(days=rng.randint(0, 1095), seconds=rng.randint(0, 86399))
        rows.append((
            user_id,
            rng.choice([0, 1, 2]),          # 0 未知 / 1 男 / 2 女
            age,
            age_range,
            rng.choice(cities),
            rng.choice(levels),
            registered,
            min(registered + timedelta(days=rng.randint(0, 60)), now),
        ))
    return rows


def build_products(rng, count, offline_rate=0.02):
    """生成商品维表行。

    Args:
        rng: 随机数发生器。
        count: 商品数。
        offline_rate: 下架比例，默认 2%。这批商品仍会被订单引用，
                      用来验证 Lookup Join 未命中时的"未知商品"兜底。

    Returns:
        (rows, offline_ids)：行数据，以及下架商品 ID 列表（写进 id_pool 供核对）。
    """
    now = datetime.now()
    rows = []
    offline_ids = []
    for product_id in range(1, count + 1):
        category_id, category_name = rng.choice(CATEGORIES)
        word = rng.choice(CATEGORY_WORDS[category_id])
        # 价格按类目给区间，避免"图书卖 2999"这种不合理组合
        if category_id in (1001, 1009, 1010):
            price = round(rng.uniform(299, 4999), 2)
        elif category_id in (1004, 1007, 1006):
            price = round(rng.uniform(39, 899), 2)
        else:
            price = round(rng.uniform(9.9, 399), 2)

        status = 1
        if rng.random() < offline_rate:
            status = 0
            offline_ids.append(product_id)

        updated = now - timedelta(days=rng.randint(0, 365), seconds=rng.randint(0, 86399))
        rows.append((
            product_id,
            f"{rng.choice(BRANDS)}{word}{rng.randint(1, 200)}",
            category_id,
            category_name,
            rng.choice(BRANDS),
            price,
            status,
            updated,
        ))
    return rows, offline_ids


# ---------------------------------------------------------------- 主流程

def write_id_pool(path, args, offline_ids):
    """写出 ID 池，供 gen_history / gen_realtime / gen_traffic 复用。

    只记范围（1..N）而不是逐个 ID 列表，文件小且足够；
    埋点侧按范围随机取，天然能命中 MySQL 主键。
    """
    pool = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "seed": args.seed,
        "users": {"min": 1, "max": args.users, "count": args.users},
        "products": {"min": 1, "max": args.products, "count": args.products},
        "shops": {"min": 1, "max": args.shops, "count": args.shops},
        "offline_product_count": len(offline_ids),
        "note": "维表缺失注入：offline_product_count 个商品 status=0，订单仍会引用",
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(pool, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)
    print(f"ID 池已写入 {path}")


def parse_args():
    """解析命令行参数。默认值与 05 文档的量级配置一致。"""
    parser = argparse.ArgumentParser(description="生成电商数仓维表主数据")
    parser.add_argument("--users", type=int, default=100000, help="用户数")
    parser.add_argument("--products", type=int, default=50000, help="商品数")
    parser.add_argument("--shops", type=int, default=2000, help="店铺数")
    parser.add_argument("--seed", type=int, default=20261124, help="随机种子，固定才能复现")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=3306)
    parser.add_argument("--user", default="root")
    parser.add_argument("--password", default=None, help="建议用环境变量 MYSQL_PASSWORD")
    parser.add_argument("--db", default="trade")
    parser.add_argument("--truncate", action="store_true", help="重建前清空三张维表")
    parser.add_argument(
        "--pool-out",
        default=str(Path(__file__).with_name("id_pool.json")),
        help="ID 池输出路径",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    rng = random.Random(args.seed)

    print(f"生成维表：店铺 {args.shops} / 用户 {args.users} / 商品 {args.products}")
    conn = connect(args)
    try:
        ensure_empty(conn, ["shop_info", "user_info", "product_info"], args.truncate)

        # 顺序固定：店铺 -> 用户 -> 商品，任何一步失败都停在原地
        shops = build_shops(rng, args.shops)
        insert_batches(
            conn, "shop_info",
            ["shop_id", "shop_name", "shop_type", "province", "city", "create_time", "update_time"],
            shops,
        )

        users = build_users(rng, args.users)
        insert_batches(
            conn, "user_info",
            ["user_id", "gender", "age", "age_range", "city", "user_level",
             "register_time", "update_time"],
            users,
        )

        products, offline_ids = build_products(rng, args.products)
        insert_batches(
            conn, "product_info",
            ["product_id", "product_name", "category_id", "category_name",
             "brand", "price", "status", "update_time"],
            products,
        )

        write_id_pool(Path(args.pool_out), args, offline_ids)

        # 回读校验，防止"写成功但行数不对"
        with conn.cursor() as cur:
            for table in ("shop_info", "user_info", "product_info"):
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                print(f"校验 {table}: {cur.fetchone()[0]} 行")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
