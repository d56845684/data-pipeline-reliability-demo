"""build_context: the single interface the decision reads from.

The MVP queries the existing services at decision time. The target architecture
swaps the body for a feature store without touching the decision code.
"""
import json


def _local_day_start(now: float, tz_offset_hours: int) -> float:
    offset = tz_offset_hours * 3600
    return (now + offset) // 86400 * 86400 - offset


def build_context(db, user_id: str, cancel: dict, now: float, cfg) -> dict:
    profile = db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)).fetchone()

    def count(sql, *args):
        return db.execute(sql, args).fetchone()[0]

    recharged_since_cancel = count(
        "SELECT COUNT(*) FROM events WHERE user_id = ? AND type = 'recharge_success' AND ts > ?",
        user_id, cancel["ts"],
    ) > 0
    chat = [
        json.loads(r["payload"]).get("text", "")
        for r in db.execute(
            "SELECT payload FROM events WHERE user_id = ? AND type = 'chat' "
            "AND ts BETWEEN ? AND ? ORDER BY ts",
            (user_id, cancel["ts"] - 3600, cancel["ts"]),
        )
    ]
    payload = cancel["payload"]
    is_paid = bool(profile["is_paid"]) if profile else None
    return {
        "user_id": user_id,
        "profile_found": profile is not None,
        "segment": None if profile is None else ("repeat" if is_paid else "first_time"),
        "push_opt_out": bool(profile["push_opt_out"]) if profile else None,
        "is_minor": bool(profile["is_minor"]) if profile else None,
        "is_vip": bool(profile["is_vip"]) if profile else None,
        "cancel_ts": cancel["ts"],
        "cancel_step": payload.get("step"),
        "error_code": payload.get("error_code"),
        "streamer_id": payload.get("streamer_id"),
        "recent_chat": chat,
        "recharged_since_cancel": recharged_since_cancel,
        "enrolled_in_cooldown": count(
            "SELECT COUNT(*) FROM decisions WHERE user_id = ? AND reason = 'assigned' AND decided_at > ?",
            user_id, now - cfg.cooldown_days * 86400,
        ) > 0,
        "pushes_today": count(
            "SELECT COUNT(*) FROM sends WHERE user_id = ? AND sent_at >= ?",
            user_id, _local_day_start(now, cfg.tz_offset_hours),
        ),
        "bonus_offers_30d": count(
            "SELECT COUNT(*) FROM decisions WHERE action = 'bonus' AND decided_at > ?",
            now - 30 * 86400,
        ),
    }
