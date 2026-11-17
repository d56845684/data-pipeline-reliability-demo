"""Replay a week of simulated traffic through the engine and print the readout.

The users and their behaviour are generated here. TRUE_RATES are the assumed
assumed numbers; the engine never sees them. So this shows that
the mechanism recovers the rates it was fed, not that the idea works on real users.

    python3 simulate.py [users] [seed]
"""
import heapq
import random
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone

from engagement import Config, Engine
from engagement.report import ev_table, experiment_rows, funnel

TRUE_RATES = {  # segment -> arm -> P(recharge within 24h)
    "first_time": {"control": 0.08, "remind": 0.10, "bonus": 0.14},
    "repeat": {"control": 0.30, "remind": 0.32, "bonus": 0.34},
}
CHATS = ["這個也太貴了吧", "餘額不夠想送禮", "付款一直失敗", "主播晚安"]
STEP = 300
DAY = 86400


def run(n_users: int, seed: int) -> None:
    rng = random.Random(seed)
    # Budget is set out of the way so the guardrail does not shrink the sample.
    eng = Engine(cfg=Config(monthly_bonus_budget=n_users))
    cfg = eng.cfg
    t0 = datetime(2026, 11, 1, tzinfo=timezone(timedelta(hours=cfg.tz_offset_hours))).timestamp()
    queue, seq = [], 0

    def push(ts, user_id, type_, payload=None, event_id=None):
        nonlocal seq
        seq += 1
        event = {"event_id": event_id or str(uuid.UUID(int=rng.getrandbits(128))),
                 "user_id": user_id, "type": type_, "payload": payload or {}}
        heapq.heappush(queue, (ts, seq, event))
        return event["event_id"]

    segment = {}
    cancel_count = 0
    for i in range(n_users):
        uid = f"u{i:06d}"
        paid = rng.random() < 0.4
        segment[uid] = "repeat" if paid else "first_time"
        eng.db.execute(
            "INSERT INTO users VALUES (?, ?, ?, ?, ?)",
            (uid, int(paid), int(rng.random() < 0.10), int(rng.random() < 0.01), int(rng.random() < 0.01)),
        )
        ts = t0 + rng.uniform(0, 7 * DAY)
        if rng.random() < 0.3:
            push(ts - 60, uid, "chat", {"text": rng.choice(CHATS)})
        payload = {"step": "choose_plan", "streamer_id": f"s{rng.randrange(200):03d}"}
        if rng.random() < 0.04:
            payload.update(step="payment", error_code="CARD_DECLINED")
        cancel_id = push(ts, uid, "cancel_recharge", payload)
        cancel_count += 1
        if rng.random() < 0.05:                      # the app resends the same event
            push(ts + 2, uid, "cancel_recharge", payload, event_id=cancel_id)
        if rng.random() < 0.05:                      # comes back on their own inside the buffer
            push(ts + rng.uniform(60, cfg.buffer_seconds - 60), uid, "recharge_success")
        if rng.random() < 0.05:                      # cancels again two days later
            push(ts + 2 * DAY, uid, "cancel_recharge", payload)
            cancel_count += 1
    eng.db.commit()

    received = duplicates = 0
    will_convert_anyway = set()
    started = time.time()
    now = t0
    while now <= t0 + 10 * DAY:
        while queue and queue[0][0] <= now:
            ts, _, event = heapq.heappop(queue)
            received += 1
            duplicates += eng.ingest(event, ts) == "duplicate"
        result = eng.tick(now)

        for d in result["decided"]:
            if d["reason"] != "assigned":
                continue
            uid = d["user_id"]
            if rng.random() < TRUE_RATES[d["segment"]]["control"]:
                will_convert_anyway.add(uid)
                push(now + rng.uniform(60, cfg.conversion_window - 60), uid, "recharge_success")
            if d["action"] == "none" and rng.random() < 0.002:
                push(now + rng.uniform(60, DAY), uid, "push_opt_out")

        for s in result["sent"]:
            uid = s["user_id"]
            rates = TRUE_RATES[s["segment"]]
            extra = (rates[s["action"]] - rates["control"]) / (1 - rates["control"])
            deadline = s["decided_at"] + cfg.conversion_window - 60
            if uid not in will_convert_anyway and deadline > now + 60 and rng.random() < extra:
                push(rng.uniform(now + 60, deadline), uid, "recharge_success")
            if rng.random() < 0.008:
                eng.db.execute("UPDATE users SET push_opt_out = 1 WHERE user_id = ?", (uid,))
                push(now + rng.uniform(60, DAY), uid, "push_opt_out")
        now += STEP

    sends_before = eng.db.execute("SELECT COUNT(*) FROM sends").fetchone()[0]
    replay_dupes = sum(
        eng.ingest({"event_id": r["event_id"], "user_id": r["user_id"], "type": "cancel_recharge"}, now) == "duplicate"
        for r in eng.db.execute("SELECT event_id, user_id FROM events WHERE type = 'cancel_recharge'").fetchall()
    )
    eng.tick(now)
    eng.tick(now + DAY)
    sends_after = eng.db.execute("SELECT COUNT(*) FROM sends").fetchone()[0]

    print(f"模擬 {n_users} 位使用者、7 天，耗時 {time.time() - started:.1f} 秒（seed={seed}）\n")
    print("1. 事件攝取")
    print(f"   收到 {received} 筆，其中重送被去重 {duplicates} 筆；取消儲值事件 {cancel_count} 筆\n")

    print("2. 決策紀錄（每一筆取消都有一筆決策，包含不行動）")
    for reason, status, note, n in funnel(eng.db):
        print(f"   {n:>6}  {reason:<32} send={status}{' (' + note + ')' if note else ''}")

    rows = experiment_rows(eng.db, cfg)
    print("\n3. 實驗結果（24 小時內儲值；lift 是相對對照組的差異）")
    print(f"   {'segment':<11}{'arm':<9}{'n':>6}{'sent':>6}{'conv':>6}{'rate':>8}{'true':>7}{'lift':>8}  {'95% CI':<18}{'EV':>7}")
    for r in rows:
        true = TRUE_RATES[r["segment"]][r["arm"]]
        line = f"   {r['segment']:<11}{r['arm']:<9}{r['n']:>6}{r['sent']:>6}{r['conversions']:>6}{r['rate']:>8.1%}{true:>7.0%}"
        if "lift" in r:
            lo, hi = r["ci95"]
            line += f"{r['lift']:>+8.1%}  [{lo:+.1%}, {hi:+.1%}]".ljust(28) + f"{r['ev']:>7.1f}"
        print(line)

    print("\n4. 由實驗結果算出的分群 EV 表（EV 不大於 0 就不行動）")
    for seg, (action, value) in ev_table(rows).items():
        print(f"   {seg:<11} -> {action:<7} EV = {value:.1f}")

    print("\n5. 冪等檢查")
    print(f"   把所有取消事件重送一次：{replay_dupes} 筆全部被判定為重複")
    print(f"   再執行兩次 tick：發送數 {sends_before} -> {sends_after}")


if __name__ == "__main__":
    run(int(sys.argv[1]) if len(sys.argv) > 1 else 30000, int(sys.argv[2]) if len(sys.argv) > 2 else 7)
