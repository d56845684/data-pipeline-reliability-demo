"""All tunable numbers in one place. Every value here is an assumption."""
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Config:
    experiment: str = "cancel_recharge_v1"
    rule_version: str = "2026-11-14.1"
    arms: tuple = ("control", "remind", "bonus")

    buffer_seconds: int = 600           # wait after a cancel before deciding
    conversion_window: int = 86400      # a recharge within 24h of the decision counts
    cooldown_days: int = 30             # one enrolment per user per 30 days
    daily_push_cap: int = 2
    monthly_bonus_budget: int = 5000    # number of bonus offers per month
    quiet_start_hour: int = 23          # local time; sends are deferred, not dropped
    quiet_end_hour: int = 8
    tz_offset_hours: int = 8

    # segment -> (margin per recharge, cost of the bonus). Used by report.py only.
    economics: dict = field(default_factory=lambda: {
        "first_time": (210.0, 30.0),
        "repeat": (350.0, 50.0),
    })
    unsubscribe_penalty: float = 100.0  # lambda in the EV formula
