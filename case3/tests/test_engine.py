"""The three things that must not go wrong in week one, plus the decision rules."""
import pytest

from engagement import Config, Engine, InvalidEvent
from engagement.decide import assign_arm, next_allowed_send
from engagement.report import ev
from engagement.store import set_setting

NOON = 1_793_505_600.0 - 1_793_505_600.0 % 86400 + 4 * 3600   # 12:00 in UTC+8
CFG = Config()


def user_in_arm(arm: str) -> str:
    return next(u for u in (f"u{i}" for i in range(1000)) if assign_arm(u, CFG) == arm)


def make(user_id: str, **profile) -> Engine:
    eng = Engine(cfg=CFG)
    row = {"is_paid": 0, "push_opt_out": 0, "is_minor": 0, "is_vip": 0, **profile}
    eng.db.execute("INSERT INTO users VALUES (?, ?, ?, ?, ?)",
                   (user_id, row["is_paid"], row["push_opt_out"], row["is_minor"], row["is_vip"]))
    return eng


def cancel(eng, user_id, at=NOON, event_id="c1", **payload):
    return eng.ingest({"event_id": event_id, "user_id": user_id, "type": "cancel_recharge", "payload": payload}, at)


def decisions(eng):
    return [dict(r) for r in eng.db.execute("SELECT * FROM decisions ORDER BY decided_at")]


def sends(eng):
    return eng.db.execute("SELECT COUNT(*) FROM sends").fetchone()[0]


def test_rejects_malformed_events():
    eng = Engine()
    with pytest.raises(InvalidEvent):
        eng.ingest({"event_id": "x", "user_id": "u", "type": "not_a_type"}, NOON)
    with pytest.raises(InvalidEvent):
        eng.ingest({"user_id": "u", "type": "chat"}, NOON)


def test_resent_event_is_dropped_and_decided_once():
    u = user_in_arm("bonus")
    eng = make(u)
    assert cancel(eng, u) == "accepted"
    assert cancel(eng, u, at=NOON + 2) == "duplicate"
    eng.tick(NOON + 601)
    assert len(decisions(eng)) == 1


def test_nothing_happens_before_the_buffer_ends():
    u = user_in_arm("bonus")
    eng = make(u)
    cancel(eng, u)
    eng.tick(NOON + 599)
    assert decisions(eng) == []


def test_user_who_recharges_inside_the_buffer_is_left_alone():
    u = user_in_arm("bonus")
    eng = make(u)
    cancel(eng, u)
    eng.ingest({"event_id": "r1", "user_id": u, "type": "recharge_success"}, NOON + 120)
    eng.tick(NOON + 601)
    assert decisions(eng)[0]["reason"] == "self_converted_in_buffer"
    assert sends(eng) == 0


@pytest.mark.parametrize("profile, reason", [
    ({"push_opt_out": 1}, "blocked:not_opted_out"),
    ({"is_minor": 1}, "blocked:not_minor"),
    ({"is_vip": 1}, "blocked:not_vip"),
])
def test_guardrails_block_and_still_log(profile, reason):
    u = user_in_arm("bonus")
    eng = make(u, **profile)
    cancel(eng, u)
    eng.tick(NOON + 601)
    d = decisions(eng)[0]
    assert (d["reason"], d["action"], d["arm"]) == (reason, "none", None)
    assert sends(eng) == 0


def test_payment_error_is_not_a_marketing_case():
    u = user_in_arm("bonus")
    eng = make(u)
    cancel(eng, u, error_code="CARD_DECLINED")
    eng.tick(NOON + 601)
    assert decisions(eng)[0]["reason"] == "blocked:not_payment_error"


def test_unknown_user_fails_closed():
    eng = Engine(cfg=CFG)
    cancel(eng, "ghost")
    eng.tick(NOON + 601)
    assert decisions(eng)[0]["reason"] == "blocked:profile_found"


def test_control_arm_is_logged_but_never_sent():
    u = user_in_arm("control")
    eng = make(u)
    cancel(eng, u)
    eng.tick(NOON + 601)
    d = decisions(eng)[0]
    assert (d["reason"], d["arm"], d["action"], d["send_status"]) == ("assigned", "control", "none", "none")
    assert d["propensity"] == pytest.approx(1 / 3)
    assert sends(eng) == 0


def test_send_is_idempotent_across_repeated_ticks():
    u = user_in_arm("bonus")
    eng = make(u)
    cancel(eng, u)
    for i in range(5):
        eng.tick(NOON + 601 + i)
    assert sends(eng) == 1
    assert decisions(eng)[0]["send_status"] == "sent"


def test_second_cancel_within_30_days_is_blocked():
    u = user_in_arm("remind")
    eng = make(u)
    cancel(eng, u)
    eng.tick(NOON + 601)
    cancel(eng, u, at=NOON + 2 * 86400, event_id="c2")
    eng.tick(NOON + 2 * 86400 + 601)
    assert [d["reason"] for d in decisions(eng)] == ["assigned", "blocked:cooldown_30d"]
    assert sends(eng) == 1


def test_quiet_hours_defer_the_send_and_recheck_before_sending():
    u = user_in_arm("bonus")
    eng = make(u)
    midnight = NOON + 12 * 3600
    cancel(eng, u, at=midnight)
    eng.tick(midnight + 601)
    d = decisions(eng)[0]
    assert d["send_status"] == "scheduled" and d["send_at"] == midnight + 8 * 3600
    assert sends(eng) == 0

    eng.ingest({"event_id": "r1", "user_id": u, "type": "recharge_success"}, midnight + 3600)
    eng.tick(midnight + 8 * 3600)
    d = decisions(eng)[0]
    assert (d["send_status"], d["send_note"]) == ("skipped", "already_recharged")
    assert sends(eng) == 0


def test_kill_switch_stops_sends_but_decisions_are_still_logged():
    u = user_in_arm("bonus")
    eng = make(u)
    set_setting(eng.db, "kill_switch", "on")
    cancel(eng, u)
    eng.tick(NOON + 601)
    d = decisions(eng)[0]
    assert (d["reason"], d["send_status"], d["send_note"]) == ("assigned", "skipped", "kill_switch")
    assert sends(eng) == 0


def test_judge_runs_in_shadow_only():
    u = user_in_arm("bonus")
    eng = make(u)
    eng.ingest({"event_id": "m1", "user_id": u, "type": "chat", "payload": {"text": "這個也太貴了吧"}}, NOON - 60)
    cancel(eng, u)
    eng.tick(NOON + 601)
    d = decisions(eng)[0]
    assert '"choice": "too_expensive"' in d["judge"] and '"used": false' in d["judge"]
    assert d["action"] == "bonus"            # the arm decides, not the judge


def test_arms_are_stable_and_balanced():
    arms = [assign_arm(f"u{i}", CFG) for i in range(30000)]
    assert assign_arm("u42", CFG) == assign_arm("u42", CFG)
    for arm in CFG.arms:
        assert abs(arms.count(arm) / len(arms) - 1 / 3) < 0.01


def test_quiet_hours_boundaries():
    assert next_allowed_send(NOON, CFG) == NOON
    assert next_allowed_send(NOON + 11 * 3600, CFG) == NOON + 20 * 3600     # 23:00 -> 08:00 next day
    assert next_allowed_send(NOON - 5 * 3600, CFG) == NOON - 4 * 3600       # 07:00 -> 08:00 same day


def test_ev_matches_the_worked_example():
    assert ev(0.10, 0.08, 210, 0, 1) == pytest.approx(3.2)
    assert ev(0.14, 0.08, 210, 30, 1) == pytest.approx(7.4)
    assert ev(0.32, 0.30, 350, 0, 1) == pytest.approx(6.0)
    assert ev(0.34, 0.30, 350, 50, 1) == pytest.approx(-4.0)
