"""End-to-end run against the real stack: local compose services plus the poc-dennis AWS resources.

    docker compose up --build -d
    python3 demo_aws.py

Takes about four minutes: it waits for EventBridge Scheduler and for Firehose to flush.
"""
import json
import time
import urllib.request
import uuid

from target_arch.decision import in_holdout

API = "http://127.0.0.1:8088"
RUN = uuid.uuid4().hex[:6]


def call(method, path, body=None):
    request = urllib.request.Request(API + path, method=method, headers={"Content-Type": "application/json"},
                                     data=None if body is None else json.dumps(body).encode())
    with urllib.request.urlopen(request, timeout=120) as response:
        return json.load(response)


def event(user, type_, **payload):
    return call("POST", "/events", {"event_id": str(uuid.uuid4()), "user_id": user, "type": type_, "payload": payload})["status"]


def pick(prefix, *scenarios):
    return next(u for u in (f"{prefix}-{RUN}-{i}" for i in range(300)) if not any(in_holdout(u, s) for s in scenarios))


def wait_for(what, check, timeout, every=5):
    started = time.time()
    while time.time() - started < timeout:
        result = check()
        if result:
            print(f"   {what}：等了 {time.time() - started:.0f} 秒")
            return result
        time.sleep(every)
    raise SystemExit(f"timed out waiting for: {what}")


def main():
    asker, paid, fan = pick("asker", "recover_recharge"), pick("paid", "recover_recharge"), pick("fan", "live_notify")
    streamer_a, streamer_b = f"sA-{RUN}", f"sB-{RUN}"
    call("PUT", "/ev-table", {"first_time": {"remind": 3.2, "bonus": 7.4}, "repeat": {"remind": 6.0, "bonus": -4.0}})
    call("PUT", f"/users/{asker}", {"follows": [streamer_a]})
    call("PUT", f"/users/{paid}", {"is_paid": True})
    call("PUT", f"/users/{fan}", {"follows": [streamer_a]})

    print("1. 事件經 Gateway 寫進 Kinesis（poc-dennis-events）")
    event(asker, "login")
    event(asker, "enter_room", streamer_id=streamer_a)
    event(asker, "leave_room", streamer_id=streamer_a, watch_seconds=1800)
    event(asker, "enter_room", streamer_id=streamer_b)
    event(asker, "click_recharge", streamer_id=streamer_b)
    cancel = {"event_id": f"cancel-{RUN}", "user_id": asker, "type": "cancel_recharge",
              "payload": {"step": "choose_plan", "streamer_id": streamer_b, "phone": "0912345678"}}
    print("   同一筆取消事件送兩次：", call("POST", "/events", cancel)["status"], call("POST", "/events", cancel)["status"])
    print("   缺欄位的事件：", event(asker, "cancel_recharge"))
    event(paid, "chat", streamer_id=streamer_b, text="這個也太貴了吧")
    event(paid, "cancel_recharge", step="choose_plan", streamer_id=streamer_b)

    print("\n2. Context Builder 從 Kinesis 讀出來，寫進 Redis 的即時層")
    ctx = wait_for("即時層出現取消事件", lambda: (lambda c: c if c["realtime"] and any(
        e["type"] == "cancel_recharge" for e in c["realtime"]["recent_events"]) else None)(
        call("GET", f"/users/{asker}/context")), 60, every=2)
    print("   ", {k: ctx["realtime"][k] for k in ("current_room", "watch_seconds_1h", "recharge_clicks_1h")})
    print("    近期層（批次還沒跑）：", ctx["recent"])

    print("\n3. 緩衝：到期喚醒交給 EventBridge Scheduler，經 SQS 回來")
    wait_for("兩筆取消儲值的決策", lambda: call("GET", f"/users/{asker}/decisions")
             and call("GET", f"/users/{paid}/decisions"), 300)
    for user in (asker, paid):
        d = call("GET", f"/users/{user}/decisions")[0]
        print(f"    {user:<22} {d['reason']:<10} action={d['action']:<7} EV={d['scores'].get('ev')} "
              f"judge={d['judge'].get('choice')}({'採用' if d['judge'].get('used') else '不採用'}) send={d['send_status']}")

    print("\n4. 主播開播：扇出給追蹤者，並與進行中的介入仲裁")
    event(f"streamer:{streamer_a}", "stream_start", streamer_id=streamer_a)
    wait_for("開播通知的決策", lambda: call("GET", f"/users/{fan}/decisions"), 60, every=2)
    for user in (asker, fan):
        d = call("GET", f"/users/{user}/decisions")[-1]
        print(f"    {user:<22} {d['scenario']:<12} {d['reason']:<32} action={d['action']}")

    print("\n5. Firehose 經 Lambda 去識別化後寫進 S3，Athena 查出來算近期層")
    wait_for("近期層出現這位使用者", lambda: (call("POST", "/batch") and
             call("GET", f"/users/{asker}/context")["recent"]), 240, every=20)
    print("    近期層：", call("GET", f"/users/{asker}/context")["recent"])

    print("\n6. 同步路徑：只讀算好的推薦清單")
    print("   ", fan, call("GET", f"/users/{fan}/recommendations"))
    print("    沒有資料的人", call("GET", "/users/nobody/recommendations"))
    print("\n   ", call("GET", "/status"))


if __name__ == "__main__":
    main()
