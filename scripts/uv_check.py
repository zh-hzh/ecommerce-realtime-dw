#!/usr/bin/env python3
"""
UV 对账：实时位图口径 vs 明细去重口径

是什么
  dws_user_1d 的日 UV 是 Doris bitmap（J5 实时写），
  dwd_traffic_detail 的日 UV 是对明细做 COUNT(DISTINCT user_id)。
  两者都该等于"当天看过页面的去重用户数"，对不上就说明某一侧有问题。

干什么
  1. 取 --dt（默认 T-1）两边的 page_view UV
  2. 算差异率，按 0.1% 阈值判 PASS/FAIL
  3. 写 dw.ads_reconcile_diff（metric='uv'，MoW 覆盖写）
  4. 顺带检查实时侧覆盖度：dws_traffic_1min 当天行数远少于 1440 时给出提示

执行
  bash scripts/run_uv_check.sh --dt 2026-11-25

边界（重要）
  这是 Doris 内部的自校验，不是跨引擎对账：
  - 离线链路目前没有埋点数据（Hive 只有交易三张表），所以拿不到独立的离线 UV
  - 因此它能抓"实时侧算错/少算/没跑"，但抓不到"两侧同源、一起错"
  - 真正的跨引擎 UV 对账要先把埋点入 Hive，见 docs/06 十一节、docs/08 D30
"""

import argparse
import os
import sys
from datetime import date, timedelta

import pymysql

THRESHOLD = float(os.getenv("UV_DIFF_THRESHOLD", "0.001"))   # 0.1%
MIN_MINUTES = int(os.getenv("UV_MIN_MINUTES", "1000"))       # 一天 1440 分钟，低于此视为覆盖不足


def parse_args():
    p = argparse.ArgumentParser(description="UV 对账（DWS 位图 vs DWD 明细）")
    p.add_argument("--dt", default=(date.today() - timedelta(days=1)).isoformat())
    p.add_argument("--since", default=None,
                   help="只比对该时刻之后的数据（yyyy-MM-dd HH:MM:SS）。"
                        "用于 J5 中途启动时做同起点比对，否则实时侧覆盖不足必然 FAIL")
    p.add_argument("--host", default=os.getenv("DORIS_HOST", "127.0.0.1"))
    p.add_argument("--port", default=int(os.getenv("DORIS_PORT", "9030")), type=int)
    p.add_argument("--user", default="root")
    p.add_argument("--password", default="")
    return p.parse_args()


def main():
    args = parse_args()
    dt = date.fromisoformat(args.dt).isoformat()
    conn = pymysql.connect(host=args.host, port=args.port, user=args.user,
                           password=args.password, database="dw",
                           autocommit=True, connect_timeout=10)
    with conn.cursor() as cur:
        if args.since:
            # dws_user_1d 是日级 bitmap，没有时间列；限定起点时改用分钟表的 uv bitmap
            cur.execute("SELECT BITMAP_UNION_COUNT(uv) FROM dw.dws_traffic_1min "
                        "WHERE dt=%s AND stat_minute >= %s", (dt, args.since))
        else:
            cur.execute("SELECT BITMAP_UNION_COUNT(uv) FROM dw.dws_user_1d WHERE dt=%s", (dt,))
        realtime_uv = int(cur.fetchone()[0] or 0)

        if args.since:
            cur.execute("SELECT COUNT(DISTINCT user_id) FROM dw.dwd_traffic_detail "
                        "WHERE dt=%s AND event_type='page_view' AND event_time >= %s",
                        (dt, args.since))
        else:
            cur.execute("SELECT COUNT(DISTINCT user_id) FROM dw.dwd_traffic_detail "
                        "WHERE dt=%s AND event_type='page_view'", (dt,))
        detail_uv = int(cur.fetchone()[0] or 0)

        cur.execute("SELECT COUNT(*) FROM dw.dws_traffic_1min WHERE dt=%s", (dt,))
        minutes = int(cur.fetchone()[0] or 0)

        if detail_uv == 0:
            print(f"{dt} 明细侧 UV 为 0，没有可比数据（先确认当天有埋点）", flush=True)
            return 2

        diff = realtime_uv - detail_uv
        rate = abs(diff) / detail_uv
        status = "PASS" if rate <= THRESHOLD else "FAIL"

        cur.execute(
            "INSERT INTO dw.ads_reconcile_diff "
            "(stat_date, metric, realtime_value, offline_value, diff_value, diff_rate, "
            " threshold, status, check_time) VALUES (%s,'uv',%s,%s,%s,%s,%s,%s,NOW(3))",
            (dt, realtime_uv, detail_uv, diff, round(rate, 8), f"{THRESHOLD:.1%}", status))

    print(f"[{status}] {dt} UV：实时(bitmap) {realtime_uv} / 明细(去重) {detail_uv} "
          f"差 {diff}（{rate:.2%}，阈值 {THRESHOLD:.1%}）", flush=True)
    if minutes < MIN_MINUTES:
        print(f"  提示：dws_traffic_1min 当天仅 {minutes} 行（正常约 1440），"
              f"实时侧覆盖不足，先查 J5 是否常驻（槽位是否够）", flush=True)
    return 0 if status == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
