#!/usr/bin/env python3
"""
把 nightly_t1 工作流建进 DolphinScheduler（standalone，12345）

是什么
  用 DS 3.4 的 REST API 脚本化创建工作流，代替在 UI 里手点：
  项目 -> 工作流定义 -> 四个 Shell 节点 -> 串行依赖。可重复执行。

干什么
  1. 登录（admin），确保项目存在（缺则建）
  2. 按名字查工作流：已存在就跳过（除非 --recreate 先删再建）
  3. 建 4 个 SHELL 节点并串成链：dim_snapshot -> offline_t1 -> reconcile -> quality_check
     全局参数 dt 默认 $[yyyy-MM-dd-1]，手动触发，失败即阻断（failureStrategy=END）
  4. --start 时立即跑一次，并打印实例链接

执行
  DS_ADMIN_PASSWORD=xxx python3 scripts/ds_build_nightly_t1.py
  DS_ADMIN_PASSWORD=xxx python3 scripts/ds_build_nightly_t1.py --start
  DS_ADMIN_PASSWORD=xxx python3 scripts/ds_build_nightly_t1.py --recreate --start

依赖
  DS standalone 已起（bash scripts/ds_keepalive.sh）；worker 进程环境里有 MYSQL_PASSWORD

说明
  - 只依赖标准库（urllib + cookiejar），不装 DS SDK
  - 节点里的脚本都用仓库内入口，DS 侧只负责顺序与失败阻断
  - 口令只走环境变量 DS_ADMIN_PASSWORD，不写进工作流定义
"""

import argparse
import http.cookiejar
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

REPO = "/mnt/d/projects/ecommerce-realtime-dw"


def parse_args():
    p = argparse.ArgumentParser(description="创建 nightly_t1 工作流")
    p.add_argument("--ds-base", default="http://127.0.0.1:12345/dolphinscheduler")
    p.add_argument("--user", default="admin")
    p.add_argument("--password", default=os.environ.get("DS_ADMIN_PASSWORD", ""))
    p.add_argument("--project", default="ecommerce-realtime-dw")
    p.add_argument("--workflow", default="nightly_t1")
    p.add_argument("--recreate", action="store_true", help="已存在时先删再建")
    p.add_argument("--start", action="store_true", help="建完立即跑一次")
    return p.parse_args()


class DS:
    def __init__(self, base):
        self.base = base.rstrip("/")
        self.op = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))

    def call(self, method, path, params=None):
        url = self.base + path
        data = urllib.parse.urlencode(params, doseq=True).encode() if params else None
        if data:
            url += ("&" if "?" in url else "?") + data.decode()
        req = urllib.request.Request(url, method=method,
                                     headers={"Accept": "application/json"})
        try:
            with self.op.open(req, timeout=60) as r:
                body = json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            sys.exit(f"{method} {path} -> HTTP {e.code}: {e.read().decode()[:300]}")
        if body.get("code") not in (0, None):
            sys.exit(f"{method} {path} -> {body.get('code')} {body.get('msg')}")
        return body.get("data")

    def login(self, user, password):
        self.call("POST", "/login", {"userName": user, "userPassword": password})


TASKS = [
    ("dim_snapshot", f"""set -e
cd {REPO}
if [ -z "${{MYSQL_PASSWORD:-}}" ]; then
  echo "worker 环境没有 MYSQL_PASSWORD：起 DS 时要带 MYSQL_PASSWORD，见 docs/07 阶段 9" >&2
  exit 1
fi
bash scripts/load_dim_shop.sh "$MYSQL_PASSWORD"
bash scripts/load_dim_product.sh "$MYSQL_PASSWORD"
"""),
    ("offline_t1", f"""set -e
cd {REPO}
if [ -z "${{MYSQL_PASSWORD:-}}" ]; then
  echo "worker 环境没有 MYSQL_PASSWORD：起 DS 时要带 MYSQL_PASSWORD，见 docs/07 阶段 9" >&2
  exit 1
fi
bash scripts/run_offline_t1.sh --dt {""}${{dt}}
"""),
    ("reconcile", f"""set -e
cd {REPO}
bash scripts/run_reconcile.sh --dt ${{dt}}
"""),
    ("quality_check", f"""set -e
cd {REPO}
bash scripts/run_quality_check.sh --dt ${{dt}}
"""),
]


def build_payload(task_codes):
    definitions, relations, locations = [], [], {}
    for i, (name, script) in enumerate(TASKS):
        code = task_codes[i]
        definitions.append({
            "code": code,
            "name": name,
            "version": 1,
            "description": "",
            "delayTime": 0,
            "taskType": "SHELL",
            "taskParams": {
                "resourceList": [],
                "localParams": [],
                "rawScript": script,
                "conditionResult": {"successNode": [""], "failedNode": [""]},
                "dependence": {},
                "waitStartTimeout": {},
                "switchResult": {},
            },
            "flag": "YES",
            "taskPriority": "MEDIUM",
            "workerGroup": "default",
            "environmentCode": -1,
            "failRetryTimes": 0,
            "failRetryInterval": 1,
            "timeoutFlag": "CLOSE",
            "timeoutNotifyStrategy": "",
            "timeout": 0,
            "cpuQuota": -1,
            "memoryMax": -1,
            "taskExecuteType": "BATCH",
        })
        locations[str(code)] = {"name": name, "targetarr": "", "x": 120 + 220 * i, "y": 120}
        prev = 0 if i == 0 else task_codes[i - 1]
        relations.append({
            "name": "",
            "preTaskCode": prev,
            "preTaskVersion": 1 if prev else 0,
            "postTaskCode": code,
            "postTaskVersion": 1,
            "conditionType": "NONE",
            "conditionParams": {},
        })
    return definitions, relations, locations


def main():
    args = parse_args()
    if not args.password:
        sys.exit("缺少 DS 管理口令：DS_ADMIN_PASSWORD 或 --password")

    ds = DS(args.ds_base)
    ds.login(args.user, args.password)

    # ---------------------------------------------------------------- 项目
    projects = ds.call("GET", "/projects", {"pageNo": 1, "pageSize": 100, "searchVal": args.project})
    hit = [p for p in (projects or {}).get("totalList", []) if p["name"] == args.project]
    if hit:
        project_code = hit[0]["code"]
        print(f"项目已存在：{args.project} ({project_code})")
    else:
        data = ds.call("POST", "/projects", {"projectName": args.project,
                                             "description": "电商实时数仓：离线 T+1 批编排"})
        project_code = data["code"]
        print(f"项目已创建：{args.project} ({project_code})")

    # ---------------------------------------------------------------- 工作流
    defs = ds.call("GET", f"/projects/{project_code}/workflow-definition",
                   {"pageNo": 1, "pageSize": 100, "searchVal": args.workflow})
    existing = [d for d in (defs or {}).get("totalList", []) if d["name"] == args.workflow]
    wf_code = None
    if existing:
        if not args.recreate:
            wf_code = existing[0]["code"]
            print(f"工作流已存在，跳过创建：{args.workflow} ({wf_code})；要重建加 --recreate")
            if not args.start:
                return
        else:
            ds.call("DELETE", f"/projects/{project_code}/workflow-definition/{existing[0]['code']}")
            print(f"已删除旧工作流：{existing[0]['code']}")

    if wf_code is None:
        base = int(time.time() * 1000) * 1000
        task_codes = [base + i for i in range(len(TASKS))]
        definitions, relations, locations = build_payload(task_codes)

        data = ds.call("POST", f"/projects/{project_code}/workflow-definition", {
            "name": args.workflow,
            "description": "离线 T+1 + 对账 + 批巡检，手动触发，dt 默认 T-1",
            "globalParams": json.dumps([{"prop": "dt", "direct": "IN", "type": "VARCHAR",
                                         "value": "$[yyyy-MM-dd-1]"}], ensure_ascii=False),
            "locations": json.dumps(locations, ensure_ascii=False),
            "timeout": 0,
            "taskRelationJson": json.dumps(relations, ensure_ascii=False),
            "taskDefinitionJson": json.dumps(definitions, ensure_ascii=False),
            "executionType": "SERIAL_WAIT",
        })
        wf_code = data["code"]
        print(f"工作流已创建：{args.workflow} ({wf_code})，{len(TASKS)} 个节点串行")
    print(f"UI: {args.ds_base}/ui/#/projects/{project_code}/workflow/definitions/{wf_code}")

    if args.start:
        # 未上线的工作流不允许手动触发
        ds.call("POST", f"/projects/{project_code}/workflow-definition/{wf_code}/release",
                {"releaseState": "ONLINE"})
        inst = ds.call("POST", f"/projects/{project_code}/executors/start-workflow-instance", {
            "workflowDefinitionCode": wf_code,
            "scheduleTime": "",
            "failureStrategy": "END",
            "execType": "START_PROCESS",
            "startNodeList": "",
            "taskDependType": "TASK_POST",
            "warningType": "NONE",
            "warningGroupId": 0,
            "runMode": "RUN_MODE_SERIAL",
            "workflowInstancePriority": "MEDIUM",
            "workerGroup": "default",
            "environmentCode": -1,
        })
        if isinstance(inst, list) and inst and isinstance(inst[0], int):
            print(f"已触发实例 id：{inst}")
        else:
            first = inst[0] if isinstance(inst, list) and inst else inst
            print(f"已触发实例：name={first.get('name')} id={first.get('id')}")
        print(f"UI: {args.ds_base}/ui/#/projects/{project_code}/workflow/instances/list"
              f"?workflowDefinitionCode={wf_code}")


if __name__ == "__main__":
    main()
