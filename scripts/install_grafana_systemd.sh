#!/usr/bin/env bash
# 是什么：把 Grafana 装成 systemd 服务，开机自启，WSL 重启后不用手动 start
# 干什么：停掉 nohup 版本 -> 复制 unit -> daemon-reload -> enable --now -> 健康检查
# 对应文档：docs/07-操作步骤.md 阶段 11、docs/08-问题索引.md D36
# 执行：需要 sudo 口令，在交互终端里跑
#   wsl -d Ubuntu -u hzh -c "sudo bash /mnt/d/projects/ecommerce-realtime-dw/scripts/install_grafana_systemd.sh"
# 说明：
#   1. 只用系统 systemd（用户级 unit 在本机是空的）；装了之后就别再用 grafana_start.sh 起了
#   2. 卸载：sudo systemctl disable --now grafana && sudo rm /etc/systemd/system/grafana.service
set -euo pipefail

REPO=/mnt/d/projects/ecommerce-realtime-dw
UNIT_SRC="$REPO/deploy/grafana.service"
UNIT_DST=/etc/systemd/system/grafana.service

[ "$(id -u)" -eq 0 ] || { echo "需要 root：用 sudo 跑本脚本" >&2; exit 1; }
[ -f "$UNIT_SRC" ] || { echo "找不到 $UNIT_SRC" >&2; exit 1; }

# 先停掉 nohup 版本，避免 3000 端口冲突
if [ -f /home/hzh/apps/grafana/run/grafana.pid ]; then
  sudo -u hzh bash "$REPO/scripts/grafana_start.sh" stop || true
fi

install -m 0644 "$UNIT_SRC" "$UNIT_DST"
systemctl daemon-reload
systemctl enable --now grafana
sleep 5

systemctl is-enabled grafana
systemctl is-active grafana
curl -sf --max-time 5 http://127.0.0.1:3000/api/health && echo
echo "完成：Grafana 已由 systemd 托管（端口 3000，admin/admin）"
