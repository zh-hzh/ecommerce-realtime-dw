#!/usr/bin/env python3
"""
巡检告警检查：把"看着还正常吗"变成一条命令

是什么
  一条命令检查五个关键面，输出 [OK]/[WARN]/[FAIL]，任何 FAIL 返回退出码 1。
  面向"人肉巡检"和"DS 里挂个告警节点"两种用法。

干什么
  1. flink_jobs   所有作业是否都 RUNNING（有作业挂了直接 FAIL）
  2. checkpoint   每个作业的 checkpoint 失败数是否为 0
  3. kafka_lag    消费组 lag 是否超阈值（默认 10 万；压测时 2000 QPS 峰值约 2.1 万）
  4. dirty_rate   当日流内脏数据率是否超 1%
  5. reconcile    最近一次对账是否有 FAIL 指标

执行
  bash scripts/run_alert_check.sh
  ALERT_LAG_MAX=50000 ALERT_DIRTY_RATE_MAX=0.005 bash scripts/run_alert_check.sh

依赖
  Flink 8081、Doris 9030（pymysql）、Kafka bin（kafka-consumer-groups.sh）

边界
  lag 只 WARN 不 FAIL：正常运行时 lag 会随 10 秒 checkpoint 锯齿波动，
  偶尔冲到两万是正常的，超过 10 万才说明消费跟不上。
"""

import json
import os
import subprocess
import sys
import urllib.error
import urllib.request
from datetime import date

FLINK = os.getenv("FLINK_REST", "http://127.0.0.1:8081")
KAFKA_BIN = os.getenv("KAFKA_BIN", "/home/hzh/apps/kafka/bin")
LAG_GROUPS = os.getenv("ALERT_LAG_GROUPS", "j4_traffic_valid,j4_traffic_dirty").split(",")
LAG_MAX = int(os.getenv("ALERT_LAG_MAX", "100000"))
DIRTY_RATE_MAX = float(os.getenv("ALERT_DIRTY_RATE_MAX", "0.01"))
DORIS = dict(host=os.getenv("DORIS_HOST", "127.0.0.1"),
             port=int(os.getenv("DORIS_PORT", "9030")),
             user=os.getenv("DORIS_USER", "root"),
             password=os.getenv("DORIS_PASSWORD", ""),
             database="dw")

results = []
failed = 0


def report(level, name, detail):
    global failed
    if level == "FAIL":
        failed += 1
    results.append((level, name, detail))
    print(f"[{level:<4}] {name:<12} {detail}", flush=True)


def flink_get(path):
    with urllib.request.urlopen(FLINK + path, timeout=10) as r:
        return json.loads(r.read().decode())


def check_flink_jobs():
    try:
        jobs = flink_get("/jobs/overview").get("jobs", [])
    except Exception as exc:
        report("FAIL", "flink_jobs", f"Flink REST 不可达：{exc}")
        return []
    if not jobs:
        report("FAIL", "flink_jobs", "没有任何作业在跑")
        return []
    bad = [j for j in jobs if j["state"] != "RUNNING"]
    if bad:
        report("FAIL", "flink_jobs",
               "非 RUNNING：" + ", ".join(f"{j['name']}={j['state']}" for j in bad))
    else:
        report("OK", "flink_jobs", f"{len(jobs)} 个作业全部 RUNNING")
    return jobs


def check_checkpoints(jobs):
    total_failed, names = 0, []
    for job in jobs:
        try:
            counts = flink_get(f"/jobs/{job['jid']}/checkpoints")["counts"]
        except Exception:
            continue
        if counts.get("failed", 0):
            total_failed += counts["failed"]
            names.append(f"{job['name']}({counts['failed']})")
    if total_failed:
        report("FAIL", "checkpoint", f"失败 {total_failed} 次：{', '.join(names)}")
    else:
        report("OK", "checkpoint", f"{len(jobs)} 个作业本次提交以来 0 失败")


def check_lag():
    worst, detail = 0, []
    for group in LAG_GROUPS:
        try:
            out = subprocess.run(
                [f"{KAFKA_BIN}/kafka-consumer-groups.sh", "--bootstrap-server",
                 "127.0.0.1:9092", "--describe", "--group", group],
                capture_output=True, text=True, timeout=25,
                env=dict(os.environ, KAFKA_HEAP_OPTS="-Xmx256m")).stdout
        except Exception as exc:
            report("FAIL", "kafka_lag", f"{group} 查询失败：{exc}")
            return
        lag = 0
        for line in out.splitlines()[2:]:
            cols = line.split()
            if len(cols) >= 6 and cols[5].lstrip("-").isdigit():
                lag += max(int(cols[5]), 0)
        detail.append(f"{group}={lag}")
        worst = max(worst, lag)
    if worst > LAG_MAX:
        report("WARN", "kafka_lag", f"{', '.join(detail)}，超过阈值 {LAG_MAX}")
    else:
        report("OK", "kafka_lag", f"{', '.join(detail)}（阈值 {LAG_MAX}）")


def check_dirty_rate():
    try:
        import pymysql
        conn = pymysql.connect(autocommit=True, connect_timeout=8, **DORIS)
    except Exception as exc:
        report("FAIL", "dirty_rate", f"Doris 不可达：{exc}")
        return
    today = date.today().isoformat()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT (SELECT COUNT(*) FROM dw.dwd_dirty_record WHERE dt=%s "
                "        AND rule_id NOT IN ('C01','C02','C03','C04','C05')), "
                "       (SELECT COUNT(*) FROM dw.dwd_trade_order_detail WHERE dt=%s) + "
                "       (SELECT COUNT(*) FROM dw.dwd_traffic_detail WHERE dt=%s)",
                (today, today, today))
            dirty, total = cur.fetchone()
    except Exception as exc:
        report("FAIL", "dirty_rate", f"查询失败：{exc}")
        return
    if total == 0:
        report("WARN", "dirty_rate", f"{today} 还没有 DWD 数据")
        return
    rate = dirty / total
    level = "FAIL" if rate >= DIRTY_RATE_MAX else "OK"
    report(level, "dirty_rate",
           f"{today} 脏 {dirty} / 总 {total} = {rate:.4%}（阈值 {DIRTY_RATE_MAX:.2%}）")


def check_reconcile():
    try:
        import pymysql
        conn = pymysql.connect(autocommit=True, connect_timeout=8, **DORIS)
    except Exception as exc:
        report("FAIL", "reconcile", f"Doris 不可达：{exc}")
        return
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(stat_date) FROM dw.ads_reconcile_diff")
            latest = cur.fetchone()[0]
            if not latest:
                report("WARN", "reconcile", "还没有对账结果，先跑 run_reconcile.sh")
                return
            cur.execute("SELECT metric, diff_value FROM dw.ads_reconcile_diff "
                        "WHERE stat_date=%s AND status='FAIL'", (latest,))
            bad = cur.fetchall()
    except Exception as exc:
        report("FAIL", "reconcile", f"查询失败：{exc}")
        return
    if bad:
        detail = ", ".join(f"{m}={d}" for m, d in bad)
        report("FAIL", "reconcile", f"{latest} 有 {len(bad)} 个指标 FAIL：{detail}")
    else:
        report("OK", "reconcile", f"{latest} 五个指标全部 PASS")


def main():
    jobs = check_flink_jobs()
    if jobs:
        check_checkpoints(jobs)
    check_lag()
    check_dirty_rate()
    check_reconcile()

    print("-" * 60, flush=True)
    ok = sum(1 for r in results if r[0] == "OK")
    warn = sum(1 for r in results if r[0] == "WARN")
    print(f"结论：{ok} OK / {warn} WARN / {failed} FAIL", flush=True)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
