"""告警接收器 — 接收 Alertmanager webhook，輸出至 console 並推播 LINE。

LINE 串接採 Messaging API（LINE Notify 已於 2025/03 終止服務）：
- POST /alerts        Alertmanager webhook（告警入口）
- POST /line/webhook  LINE 平台回呼端點（經 cloudflared tunnel 對外）：
                      把 bot 加為好友後傳任意訊息，bot 會回覆你的 userId，
                      將其填入 .env 的 LINE_TARGET_ID 即完成推播設定
- 未設定 token 時自動退回純 console 模式
- LINE_SEND_RESOLVED=true 時連 RESOLVED 也推播（預設只推 FIRING，節省免費額度）
"""
import base64
import hashlib
import hmac
import os
from datetime import datetime

import requests
from flask import Flask, request

app = Flask(__name__)

LINE_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "").strip()
LINE_SECRET = os.getenv("LINE_CHANNEL_SECRET", "").strip()
LINE_TARGET = os.getenv("LINE_TARGET_ID", "").strip()
SEND_RESOLVED = os.getenv("LINE_SEND_RESOLVED", "false").lower() == "true"
LINE_API = "https://api.line.me/v2/bot/message"

ICONS = {"critical": "🔴", "warning": "🟡"}


# ---------- LINE Messaging API ----------

def line_api(endpoint: str, body: dict) -> None:
    try:
        resp = requests.post(
            f"{LINE_API}/{endpoint}",
            headers={"Authorization": f"Bearer {LINE_TOKEN}"},
            json=body,
            timeout=10,
        )
        if resp.status_code == 200:
            print(f"[line] {endpoint} OK", flush=True)
        else:
            print(f"[line] {endpoint} failed {resp.status_code}: {resp.text[:200]}", flush=True)
    except requests.RequestException as e:
        print(f"[line] {endpoint} error: {e}", flush=True)


def push_line(text: str) -> None:
    line_api("push", {"to": LINE_TARGET,
                      "messages": [{"type": "text", "text": text[:4900]}]})


def verify_line_signature(body: bytes, signature: str) -> bool:
    """驗證 X-Line-Signature（HMAC-SHA256）。未設定 channel secret 時跳過。"""
    if not LINE_SECRET:
        return True
    digest = hmac.new(LINE_SECRET.encode(), body, hashlib.sha256).digest()
    return hmac.compare_digest(base64.b64encode(digest).decode(), signature or "")


# ---------- Alertmanager webhook ----------

def format_alert(alert: dict) -> str:
    labels = alert.get("labels", {})
    annotations = alert.get("annotations", {})
    icon = ICONS.get(labels.get("severity", ""), "⚪")
    state = "✅ RESOLVED" if alert.get("status") == "resolved" else f"{icon} FIRING"
    lines = [
        f"{state} {labels.get('alertname', '?')}",
        f"管道：{labels.get('pipeline', '-')}（{labels.get('severity', '-')}）",
    ]
    if annotations.get("summary"):
        lines.append(annotations["summary"])
    if alert.get("status") != "resolved" and annotations.get("description"):
        lines.append(annotations["description"])
    return "\n".join(lines)


@app.route("/alerts", methods=["POST"])
def alerts():
    payload = request.get_json(force=True, silent=True) or {}
    incoming = payload.get("alerts", [])

    # console 永遠記錄（本地觀察用）
    for alert in incoming:
        labels = alert.get("labels", {})
        icon = ICONS.get(labels.get("severity", ""), "⚪")
        state = "RESOLVED ✅" if alert.get("status") == "resolved" else "FIRING"
        print(
            f"{icon} [{datetime.now():%H:%M:%S}] {state} "
            f"{labels.get('alertname', '?'):<24} "
            f"pipeline={labels.get('pipeline', '-'):<12} "
            f"| {alert.get('annotations', {}).get('summary', '')}",
            flush=True,
        )

    # LINE 推播 —— 只有 Alertmanager 路由標記 ?push=line 的告警（severity=critical）
    # 才會走到這裡，warning 級留在 console/Grafana，避免 alert fatigue 與額度浪費
    if request.args.get("push") == "line":
        to_send = [a for a in incoming
                   if SEND_RESOLVED or a.get("status") != "resolved"]
        if not to_send:
            pass
        elif LINE_TOKEN and LINE_TARGET:
            header = f"🚨 Critical Pipeline 告警（{len(to_send)} 則）"
            push_line(header + "\n" + ("─" * 12) + "\n"
                      + ("\n" + "─" * 12 + "\n").join(format_alert(a) for a in to_send))
        else:
            print(f"[line] 收到 {len(to_send)} 則 critical 告警，"
                  "但未設定 token/target，僅 console 記錄", flush=True)

    return {"status": "ok"}


# ---------- LINE 平台回呼（取得 userId 用）----------

@app.route("/line/webhook", methods=["POST"])
def line_webhook():
    if not verify_line_signature(request.get_data(), request.headers.get("X-Line-Signature")):
        print("[line] webhook signature 驗證失敗", flush=True)
        return {"status": "bad signature"}, 403

    payload = request.get_json(force=True, silent=True) or {}
    for event in payload.get("events", []):
        source = event.get("source", {})
        source_id = (source.get("groupId") or source.get("roomId")
                     or source.get("userId") or "?")
        print(f"[line] webhook event type={event.get('type')} "
              f"source={source.get('type')} id={source_id}", flush=True)

        # 回覆來源 ID，讓使用者直接複製填入 LINE_TARGET_ID
        if event.get("replyToken") and LINE_TOKEN:
            line_api("reply", {
                "replyToken": event["replyToken"],
                "messages": [{
                    "type": "text",
                    "text": ("✅ 已連上告警 bot！\n"
                             f"你的 {source.get('type')} ID：\n{source_id}\n\n"
                             "請填入 .env 的 LINE_TARGET_ID 後重啟 alert-logger"),
                }],
            })
    return {"status": "ok"}


@app.route("/healthz", methods=["GET"])
def healthz():
    return {"status": "ok", "line_configured": bool(LINE_TOKEN and LINE_TARGET)}


if __name__ == "__main__":
    mode = "console + LINE 推播" if (LINE_TOKEN and LINE_TARGET) else "純 console（未設定 LINE token/target）"
    print(f"[alert-logger] 啟動，模式：{mode}", flush=True)
    app.run(host="0.0.0.0", port=5001)
