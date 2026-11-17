"""Decision service: the single exit for every scenario. Four fixed layers.

    guardrails -> judge model -> EV scoring -> experiment holdout

Pure function of the context: no database, no clock. The judge is called before
this function by the worker, off the synchronous path.
"""
import hashlib

from engagement.decide import Decision, next_allowed_send

PRIORITY = {"recover_recharge": 0, "live_notify": 1, "winback": 2}
CANDIDATES = {
    "recover_recharge": ["remind", "bonus"],
    "live_notify": ["live_push"],
    "winback": ["content_push"],
}
# What the judge is allowed to change: the type of action, never who is eligible.
JUDGE_NARROWS = {
    "payment_friction": ["remind"],
    "just_browsing": ["content_push"],
    "mistap": ["content_push"],
    "too_expensive": ["remind", "bonus"],
    "wants_to_gift": ["remind", "bonus"],
}
JUDGE_THRESHOLD = 0.8
HOLDOUT = 0.1


def in_holdout(user_id: str, scenario: str) -> bool:
    digest = hashlib.sha256(f"holdout:{scenario}:{user_id}".encode()).digest()
    return int.from_bytes(digest[:8], "big") / 2**64 < HOLDOUT


def guardrails(scenario: str, ctx: dict, cfg) -> list:
    static, touch = ctx["static"] or {}, ctx["touch"]
    checks = [
        ("profile_found", ctx["static"] is not None),
        ("not_opted_out", static.get("push_opt_out") is False),
        ("not_minor", static.get("is_minor") is False),
        ("daily_push_cap", touch["pushes_today"] < cfg.daily_push_cap),
    ]
    if scenario == "recover_recharge":
        checks += [
            ("not_vip", static.get("is_vip") is False),
            ("not_payment_error", not ctx["trigger"].get("error_code")),
            ("cooldown_30d", not touch["recover_enrolled_30d"]),
        ]
    return checks


def judge_is_usable(output: dict, chat: list) -> bool:
    """Found in the PoC: the model is confident even with nothing to read, so the
    threshold alone is not enough. No chat means the answer is not used."""
    return bool(chat) and "error" not in output and (output.get("confidence") or 0) >= JUDGE_THRESHOLD


def decide(scenario: str, ctx: dict, now: float, cfg, ev_table: dict, judge_output: dict | None = None):
    """Returns (Decision, scores). `scores` records the candidates and the EV of each."""
    rt = ctx["realtime"]
    scores = {"degraded": not ctx["realtime_available"]}

    # [0] Has the situation resolved itself?
    events = (rt or {}).get("recent_events", [])
    if scenario == "recover_recharge" and any(
            e["type"] == "recharge_success" and e["ts"] > ctx["trigger"]["cancel_ts"] for e in events):
        return Decision(reason="self_converted_in_buffer"), scores
    if scenario == "live_notify" and rt and rt["current_room"] == ctx["trigger"]["streamer_id"]:
        return Decision(reason="already_watching"), scores
    if scenario == "winback" and rt and rt["last_event_ts"] > now - 86400:
        return Decision(reason="already_back"), scores

    # [1] Guardrails
    checks = guardrails(scenario, ctx, cfg)
    failed = next((rule for rule, passed in checks if not passed), None)
    if failed:
        return Decision(reason=f"blocked:{failed}", guardrails=checks), scores

    # Arbitration: one intervention in flight per user, whatever the scenario.
    if ctx["touch"]["active_intervention"]:
        return Decision(reason="arbitrated:active_intervention", guardrails=checks), scores

    # [2] Judge model: narrows the kind of action when there is something to read.
    candidates = list(CANDIDATES[scenario])
    judge = dict(judge_output or {})
    if judge:
        judge["used"] = judge_is_usable(judge, ctx["trigger"].get("chat", []))
        if judge["used"]:
            candidates = JUDGE_NARROWS.get(judge["choice"], candidates)
    if "bonus" in candidates and (scores["degraded"] or ctx["touch"]["bonus_offers_30d"] >= cfg.monthly_bonus_budget):
        candidates = [c for c in candidates if c != "bonus"]     # money needs fresh state and budget
    scores["candidates"] = candidates

    # [3] EV scoring from the per-segment table. "Do nothing" is always a candidate.
    cells = ev_table.get(ctx["segment"], {})
    scores["ev"] = {c: cells.get(c) for c in candidates}
    known = {c: v for c, v in scores["ev"].items() if v is not None}
    if known:
        action, best = max(known.items(), key=lambda kv: kv[1])
        if best <= 0:
            return Decision(reason="ev_not_positive", guardrails=checks, judge=judge), scores
    elif candidates:
        action = candidates[0]          # no experiment data yet for this action: measured by the holdout
    else:
        return Decision(reason="no_candidate", guardrails=checks, judge=judge), scores

    # [4] Holdout: the only baseline for measuring uplift.
    if in_holdout(ctx["user_id"], scenario):
        return Decision(reason="assigned", arm="holdout", action="none", propensity=HOLDOUT,
                        guardrails=checks, judge=judge), scores
    return Decision(reason="assigned", arm="treated", action=action, propensity=1 - HOLDOUT,
                    send_at=next_allowed_send(now, cfg), guardrails=checks, judge=judge), scores
