"""告警接收器 — 模擬 Slack / PagerDuty webhook，把 Alertmanager 告警以易讀格式輸出。

查看告警流：docker compose logs -f alert-logger
"""
from datetime import datetime

from flask import Flask, request

app = Flask(__name__)

ICONS = {"critical": "🔴", "warning": "🟡"}


@app.route("/alerts", methods=["POST"])
def alerts():
    payload = request.get_json(force=True, silent=True) or {}
    for alert in payload.get("alerts", []):
        labels = alert.get("labels", {})
        annotations = alert.get("annotations", {})
        icon = ICONS.get(labels.get("severity", ""), "⚪")
        state = "RESOLVED ✅" if alert.get("status") == "resolved" else "FIRING"
        print(
            f"{icon} [{datetime.now():%H:%M:%S}] {state} "
            f"{labels.get('alertname', '?'):<24} "
            f"pipeline={labels.get('pipeline', '-'):<12} "
            f"| {annotations.get('summary', '')}",
            flush=True,
        )
    return {"status": "ok"}


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001)
