"""Properties the target architecture claims, one test each."""
import glob
import json

import pytest

from target_arch import Platform
from target_arch.decision import in_holdout
from target_arch.stream import Consumer, Stream

T = 1_794_657_600.0      # 2026-11-14 20:00 in UTC+8
EV = {"first_time": {"remind": 3.2, "bonus": 7.4}, "repeat": {"remind": 6.0, "bonus": -4.0}}


def treated(scenario="recover_recharge", holdout=False, prefix="u"):
    return next(u for u in (f"{prefix}{i}" for i in range(300)) if in_holdout(u, scenario) == holdout)


@pytest.fixture
def p(tmp_path):
    platform = Platform(str(tmp_path / "lake"))
    platform.load_ev_table(EV)
    platform.n = 0
    return platform


def send(p, ts, user, type_, event_id=None, **payload):
    p.n += 1
    return p.ingest({"event_id": event_id or f"e{p.n}", "user_id": user, "type": type_, "payload": payload}, ts)


def cancel_and_decide(p, user, chat=None, at=T):
    if chat:
        send(p, at - 60, user, "chat", streamer_id="sB", text=chat)
    send(p, at, user, "cancel_recharge", step="choose_plan", streamer_id="sB")
    p.tick(at + 1)
    return p.tick(at + 601)["decided"][0]


def test_one_users_events_stay_in_order_on_one_shard():
    stream = Stream(shards=4)
    for i in range(50):
        stream.put("alice", {"n": i})
        stream.put(f"other{i}", {"n": -1})
    mine = [[r["n"] for r in shard if r["n"] >= 0] for shard in stream.shards]
    assert sorted(len(m) for m in mine) == [0, 0, 0, 50]
    assert max(mine, key=len) == list(range(50))


def test_consumers_are_independent_and_redeliver_after_a_failure():
    stream = Stream(shards=1)
    for i in range(3):
        stream.put("k", {"n": i})
    good, flaky = Consumer(stream, "good"), Consumer(stream, "flaky")
    seen, attempts = [], []

    def fails_once_on_1(record):
        attempts.append(record["n"])
        if record["n"] == 1 and attempts.count(1) == 1:
            raise RuntimeError("boom")

    good.run(lambda r: seen.append(r["n"]))
    flaky.run(fails_once_on_1)
    assert seen == [0, 1, 2] and good.lag() == 0          # not held back by the other consumer
    assert flaky.lag() == 2                               # stopped at the failed record
    flaky.run(fails_once_on_1)
    assert attempts == [0, 1, 1, 2] and flaky.lag() == 0  # same record delivered again, then it moves on


def test_gateway_dedupes_and_quarantines(p):
    assert send(p, T, "u", "cancel_recharge", event_id="c", step="choose_plan") == "accepted"
    assert send(p, T, "u", "cancel_recharge", event_id="c", step="choose_plan") == "duplicate"
    assert send(p, T, "u", "cancel_recharge") == "quarantined"
    assert send(p, T, "u", "made_up_type") == "quarantined"
    assert sum(len(s) for s in p.stream.shards) == 1


def test_realtime_update_is_idempotent(p):
    record = {"event_id": "x", "user_id": "u", "type": "leave_room", "ts": T,
              "payload": {"streamer_id": "sA", "watch_seconds": 600}}
    p.realtime.apply(record)
    p.realtime.apply(record)
    assert p.realtime.get("u", T + 1)["watch_seconds_1h"] == 600


def test_lake_holds_no_user_id_and_no_chat_text(p, tmp_path):
    send(p, T, "alice-real-id", "chat", streamer_id="sA", text="我的電話是 0912345678", phone="0912345678")
    p.tick(T + 1)
    raw = "".join(open(f, encoding="utf-8").read() for f in glob.glob(str(tmp_path / "lake" / "dt=*" / "*.jsonl")))
    assert raw and "alice-real-id" not in raw and "0912345678" not in raw
    assert json.loads(json.loads(raw.splitlines()[0])["payload"]) == {"streamer_id": "sA", "text_length": 16}


def test_only_trigger_events_reach_a_decision(p):
    u = treated()
    p.add_user(u)
    for i, type_ in enumerate(("login", "app_open", "click_recharge")):
        send(p, T + i, u, type_)
    p.tick(T + 10)
    assert p.db.execute("SELECT COUNT(*) FROM triggers").fetchone()[0] == 0
    send(p, T + 20, u, "cancel_recharge", step="choose_plan")
    p.tick(T + 21)
    assert p.db.execute("SELECT COUNT(*) FROM triggers").fetchone()[0] == 1


def test_ev_picks_bonus_for_first_time_and_remind_for_paid(p):
    first, paid = treated(prefix="f"), treated(prefix="p")
    p.add_user(first)
    p.add_user(paid, is_paid=True)
    assert cancel_and_decide(p, first)["action"] == "bonus"
    d = cancel_and_decide(p, paid)
    assert d["action"] == "remind" and d["scores"]["ev"] == {"remind": 6.0, "bonus": -4.0}


def test_nothing_is_done_when_every_ev_is_negative(p):
    u = treated()
    p.add_user(u)
    p.load_ev_table({"first_time": {"remind": -1.0, "bonus": -2.0}})
    d = cancel_and_decide(p, u)
    assert (d["reason"], d["action"]) == ("ev_not_positive", "none")


def test_judge_narrows_the_action_only_when_there_is_chat(p):
    with_chat, without = treated(prefix="a"), treated(prefix="b")
    p.add_user(with_chat)
    p.add_user(without)
    d = cancel_and_decide(p, with_chat, chat="付款一直失敗")
    assert d["judge"]["used"] is True and d["scores"]["candidates"] == ["remind"]
    d = cancel_and_decide(p, without)
    assert d["judge"]["used"] is False and d["action"] == "bonus"


def test_judge_cannot_override_a_guardrail(p):
    p.add_user("optout", push_opt_out=True)
    d = cancel_and_decide(p, "optout", chat="太貴了，有優惠我就儲")
    assert d["reason"] == "blocked:not_opted_out"


def test_holdout_is_logged_with_its_probability_and_gets_nothing(p):
    u = treated(holdout=True)
    p.add_user(u)
    d = cancel_and_decide(p, u)
    row = p.db.execute("SELECT arm, action, propensity FROM decisions WHERE user_id = ?", (u,)).fetchone()
    assert (d["action"], row["arm"], row["propensity"]) == ("none", "holdout", 0.1)
    assert p.db.execute("SELECT COUNT(*) FROM sends").fetchone()[0] == 0


def test_second_scenario_is_arbitrated_while_one_is_in_flight(p):
    u = next(x for x in (f"u{i}" for i in range(300))
             if not in_holdout(x, "recover_recharge") and not in_holdout(x, "live_notify"))
    p.add_user(u, follows=["sA"])
    cancel_and_decide(p, u)
    send(p, T + 700, "streamer:sA", "stream_start", streamer_id="sA")
    d = p.tick(T + 701)["decided"][0]
    assert (d["scenario"], d["reason"]) == ("live_notify", "arbitrated:active_intervention")
    assert p.db.execute("SELECT COUNT(*) FROM sends WHERE user_id = ?", (u,)).fetchone()[0] == 1


def test_churn_scan_triggers_once_per_day(p):
    u = treated("winback")
    p.add_user(u)
    send(p, T - 8 * 86400, u, "login")
    p.tick(T - 86400)
    p.run_batch(T)
    assert p.schedule_churn_scan(T) == 1
    assert p.tick(T + 1)["decided"][0]["action"] == "content_push"
    p.schedule_churn_scan(T + 3600)
    assert p.tick(T + 3601)["decided"] == []


def test_sync_path_falls_back_when_engagement_is_down(p):
    p.realtime.set_recommendations("u", ["sA"])
    assert p.recommendations("u")["streamers"] == ["sA"]
    p.realtime.available = False
    assert p.recommendations("u")["source"].startswith("popular")


def test_outage_disables_money_and_holds_the_send_until_state_can_be_checked(p):
    u = treated()
    p.add_user(u)
    send(p, T, u, "cancel_recharge", step="choose_plan")
    p.tick(T + 1)
    p.realtime.available = False
    out = p.tick(T + 601)
    assert out["decided"][0]["action"] == "remind" and out["decided"][0]["scores"]["degraded"] is True
    assert out["sent"] == [] and p.context_builder.lag() == 0 and p.lake_writer.lag() == 0
    p.realtime.available = True
    assert [s["action"] for s in p.tick(T + 602)["sent"]] == ["remind"]


def test_send_is_skipped_if_the_user_recharged_after_the_decision(p):
    u = treated()
    p.add_user(u)
    late_night = T + 4 * 3600                     # 00:00, inside quiet hours: decided now, sent at 08:00
    d = cancel_and_decide(p, u, at=late_night)
    assert d["action"] == "bonus"
    send(p, late_night + 3600, u, "recharge_success", amount=150)
    p.tick(late_night + 8 * 3600)
    row = p.db.execute("SELECT send_status, send_note FROM decisions WHERE user_id = ?", (u,)).fetchone()
    assert (row["send_status"], row["send_note"]) == ("skipped", "already_recharged")
