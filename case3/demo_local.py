"""Walk the target architecture through one evening, stage by stage.

    python3 demo_local.py          # offline judge
    python3 demo_local.py --jev    # hosted judge model (needs OPENROUTER_API_KEY)

Everything runs in this process. The users, events and EV numbers are made up.
"""
import glob
import json
import shutil
import sys
import tempfile
from datetime import datetime, timedelta, timezone

from engagement.judge import JevJudge, load_api_key
from target_arch import Platform
from target_arch.decision import in_holdout
from target_arch.layers import build_context

TZ = timezone(timedelta(hours=8))
T = datetime(2026, 11, 14, 20, 0, tzinfo=TZ).timestamp()      # tonight, 20:00
DAY, MIN = 86400, 60
EV_TABLE = {"first_time": {"remind": 3.2, "bonus": 7.4}, "repeat": {"remind": 6.0, "bonus": -4.0}}  # assumed values


def pick(prefix: str, scenario: str, holdout: bool) -> str:
    return next(u for u in (f"{prefix}{i}" for i in range(200)) if in_holdout(u, scenario) == holdout)


def title(text: str) -> None:
    print(f"\n{'=' * 4} {text} {'=' * (70 - len(text) * 2)}")


def show(d: dict) -> None:
    line = f"   {d['user_id']:<12} {d['scenario']:<17} {d['reason']:<32} action={d['action']}"
    if d["scores"].get("ev"):
        line += f"  EV={d['scores']['ev']}"
    if d["judge"]:
        line += f"  judge={d['judge'].get('choice')}({'採用' if d['judge'].get('used') else '不採用'})"
    print(line)


def main() -> None:
    judge = JevJudge(load_api_key()) if "--jev" in sys.argv else None
    lake = tempfile.mkdtemp(prefix="lake-")
    p = Platform(lake, judge=judge)
    p.load_ev_table(EV_TABLE)
    n = 0

    def send(ts, user, type_, event_id=None, **payload):
        nonlocal n
        n += 1
        return p.ingest({"event_id": event_id or f"e{n}", "user_id": user, "type": type_, "payload": payload}, ts)

    asker = pick("asker", "recover_recharge", False)          # the user in the question
    paid = pick("paid", "recover_recharge", False)
    friction = pick("friction", "recover_recharge", False)
    held = pick("held", "recover_recharge", True)
    fan = pick("fan", "live_notify", False)
    lapsed = pick("lapsed", "winback", False)
    late = pick("late", "recover_recharge", False)
    p.add_user(asker, follows=["sA"])
    p.add_user(paid, is_paid=True)
    p.add_user(friction)
    p.add_user(held)
    p.add_user("optout", push_opt_out=True)
    p.add_user(fan, follows=["sA"])
    p.add_user(lapsed)
    p.add_user(late)

    title("0. 過去兩週的事件 -> 資料湖 -> 批次特徵（近期層）")
    for d in range(1, 14):
        for user in (asker, paid, friction, held, fan, late):
            send(T - d * DAY, user, "login")
            send(T - d * DAY + 30 * MIN, user, "leave_room", streamer_id="sA", watch_seconds=1200)
        if d >= 8:
            send(T - d * DAY, lapsed, "login")
    send(T - 3 * DAY, paid, "recharge_success", amount=300)
    p.tick(T - DAY)
    print(f"   批次工作算出 {p.run_batch(T - 60)} 位使用者的近期特徵")
    files = sorted(glob.glob(f"{lake}/dt=*/*.jsonl"))
    print(f"   資料湖 {len(files)} 個檔案，其中一筆（已去識別化）：")
    print("   " + open(files[-1], encoding="utf-8").readline().strip())

    title("1. 今晚的事件：Gateway 驗證、去重，寫入串流")
    send(T, asker, "login")
    send(T + 1, asker, "enter_room", streamer_id="sA")
    send(T + 30 * MIN, asker, "leave_room", streamer_id="sA", watch_seconds=1800)
    send(T + 31 * MIN, asker, "enter_room", streamer_id="sB")
    send(T + 32 * MIN, asker, "click_recharge", streamer_id="sB")
    results = [send(T + 33 * MIN, asker, "cancel_recharge", event_id="cancel-asker", step="choose_plan", streamer_id="sB")]
    results.append(send(T + 33 * MIN + 2, asker, "cancel_recharge", event_id="cancel-asker", step="choose_plan"))
    results.append(send(T + 33 * MIN, asker, "cancel_recharge"))                      # no `step`
    for user, text in ((paid, "這個也太貴了吧"), (friction, "付款一直失敗，好麻煩"), (held, "太貴了"), ("optout", "太貴了")):
        send(T + 32 * MIN, user, "chat", streamer_id="sB", text=text)
        send(T + 33 * MIN, user, "cancel_recharge", step="choose_plan", streamer_id="sB")
    print(f"   同一筆取消事件送三次（正常、重送、缺欄位）：{results}")
    print(f"   隔離佇列：{[q['problem'] for q in p.gateway.quarantine]}")
    print(f"   各分片的事件數：{[len(s) for s in p.stream.shards]}（同一個 user_id 一定在同一個分片）")

    title("2. 三個消費者各自讀同一個串流")
    print(f"   讀取前的落後筆數：context-builder={p.context_builder.lag()} firehose={p.lake_writer.lag()} trigger={p.trigger_reader.lag()}")
    out = p.tick(T + 34 * MIN)
    print(f"   讀取後：context-builder={p.context_builder.lag()} firehose={p.lake_writer.lag()} trigger={p.trigger_reader.lag()}")
    pending = p.db.execute("SELECT COUNT(*) FROM triggers WHERE done = 0").fetchone()[0]
    print(f"   今晚 30 筆左右的事件裡，只有取消儲值變成觸發：{pending} 筆，都排在 10 分鐘後；現在的決策數：{len(out['decided'])}")

    title("3. build_context：取消儲值的那位使用者的三層 Context")
    ctx = build_context(p.db, p.realtime, p.secret, asker, T + 34 * MIN, p.cfg)
    rt = dict(ctx["realtime"], recent_events=[f"{e['type']}" for e in ctx["realtime"]["recent_events"]][-6:])
    print(f"   即時層：{json.dumps(rt, ensure_ascii=False)}")
    print(f"   近期層：{json.dumps(ctx['recent'], ensure_ascii=False)}")
    print(f"   靜態層：{ctx['static']}　觸達歷史：{ctx['touch']}")

    title("4. 緩衝結束（取消後 10 分鐘）：四層決策")
    send(T + 38 * MIN, held, "recharge_success", amount=150)      # one of them came back on their own
    for d in p.tick(T + 43 * MIN + 1)["decided"]:
        show(d)

    title("5. 主播 sA 開播：事件型觸發扇出給追蹤者，並與進行中的介入仲裁")
    send(T + 50 * MIN, "streamer:sA", "stream_start", streamer_id="sA")
    for d in p.tick(T + 50 * MIN + 1)["decided"]:
        show(d)

    title("6. 排程型觸發：每日流失掃描")
    print(f"   近期層標為高風險的人數：{p.schedule_churn_scan(T + 60 * MIN)}")
    for d in p.tick(T + 60 * MIN)["decided"]:
        show(d)

    title("7. 請求型觸發（同步）：App 開啟要推薦清單，只讀算好的結果")
    p.run_batch(T + 61 * MIN)
    print(f"   {fan}: {p.recommendations(fan)}")
    print(f"   沒有資料的新用戶: {p.recommendations('nobody')}")

    title("8. 即時層故障：同步路徑退回熱門清單，涉及金錢的動作停用")
    p.realtime.available = False
    print(f"   {fan}: {p.recommendations(fan)}")
    send(T + 62 * MIN, late, "cancel_recharge", step="choose_plan", streamer_id="sB")
    out = p.tick(T + 73 * MIN)
    for d in out["decided"]:
        show(d)
    print(f"   context-builder 落後 {p.context_builder.lag()} 筆（{p.context_builder.last_error}）；"
          f"firehose 落後 {p.lake_writer.lag()} 筆；這一輪送出 {len(out['sent'])} 筆")
    p.realtime.available = True
    out = p.tick(T + 74 * MIN)
    p.context_builder.run(p.realtime.apply)
    print(f"   恢復後：context-builder 落後 {p.context_builder.lag()} 筆，補送 {[(s['user_id'], s['action']) for s in out['sent']]}")

    title("9. Decision log 與冪等")
    for r in p.db.execute("SELECT scenario AS s, reason, action, send_status, COUNT(*) AS n "
                          "FROM decisions GROUP BY 1, 2, 3, 4 ORDER BY 1, 2"):
        print(f"   {r['n']}  {r['s']:<17} {r['reason']:<32} action={r['action']:<13} send={r['send_status']}")
    before = p.db.execute("SELECT COUNT(*) FROM sends").fetchone()[0]
    for i in range(3):
        p.tick(T + 75 * MIN + i)
    print(f"   再執行三次 tick：發送數 {before} -> {p.db.execute('SELECT COUNT(*) FROM sends').fetchone()[0]}")
    shutil.rmtree(lake)


if __name__ == "__main__":
    main()
