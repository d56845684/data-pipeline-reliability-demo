"""decide(context) -> Decision. Pure function: no database, no clock, no model in the path.

Order: buffer -> guardrails -> judge (shadow) -> experiment arm.
"""
import hashlib
from dataclasses import dataclass, field

ARM_ACTION = {"control": "none", "remind": "remind", "bonus": "bonus"}


@dataclass
class Decision:
    reason: str
    action: str = "none"
    arm: str | None = None
    propensity: float | None = None
    send_at: float | None = None
    guardrails: list = field(default_factory=list)
    judge: dict = field(default_factory=dict)


def assign_arm(user_id: str, cfg) -> str:
    digest = hashlib.sha256(f"{cfg.experiment}:{user_id}".encode()).digest()
    return cfg.arms[int.from_bytes(digest[:8], "big") % len(cfg.arms)]


def run_guardrails(ctx: dict, cfg) -> list:
    """Each entry is (rule, passed). Anything unknown fails closed."""
    return [
        ("profile_found", ctx["profile_found"]),
        ("not_opted_out", ctx["push_opt_out"] is False),
        ("not_minor", ctx["is_minor"] is False),
        ("not_vip", ctx["is_vip"] is False),
        ("not_payment_error", not ctx["error_code"]),
        ("cooldown_30d", not ctx["enrolled_in_cooldown"]),
        ("daily_push_cap", ctx["pushes_today"] < cfg.daily_push_cap),
        ("bonus_budget", ctx["bonus_offers_30d"] < cfg.monthly_bonus_budget),
    ]


def next_allowed_send(now: float, cfg) -> float:
    """Quiet hours defer the send to the next morning instead of dropping it."""
    offset = cfg.tz_offset_hours * 3600
    local = now + offset
    hour = local % 86400 // 3600
    if cfg.quiet_end_hour <= hour < cfg.quiet_start_hour:
        return now
    day = local // 86400 * 86400
    if hour >= cfg.quiet_start_hour:
        day += 86400
    return day + cfg.quiet_end_hour * 3600 - offset


def decide(ctx: dict, now: float, cfg, judge) -> Decision:
    if ctx["recharged_since_cancel"]:
        return Decision(reason="self_converted_in_buffer")

    checks = run_guardrails(ctx, cfg)
    failed = next((rule for rule, passed in checks if not passed), None)
    if failed:
        return Decision(reason=f"blocked:{failed}", guardrails=checks)

    shadow = judge.cancel_reason(ctx)
    arm = assign_arm(ctx["user_id"], cfg)
    action = ARM_ACTION[arm]
    return Decision(
        reason="assigned",
        action=action,
        arm=arm,
        propensity=1 / len(cfg.arms),
        send_at=None if action == "none" else next_allowed_send(now, cfg),
        guardrails=checks,
        judge=shadow,
    )
