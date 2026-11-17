"""User Context in three layers behind one interface.

realtime: updated on every event, expires          (Redis in the target architecture)
recent:   7/30-day aggregates from the batch job   (relational database)
static:   consent, age, VIP, paid status           (existing services, read at decision time)
"""
import json

from .deid import pseudonym


class RealtimeUnavailable(RuntimeError):
    pass


class RealtimeStore:
    """In-memory stand-in for Redis. Set `available = False` to simulate an outage.

    aws.py subclasses this and overrides only the storage hooks, so the logic is shared.
    """
    ttl = 3 * 86400
    keep = 50

    def __init__(self):
        self._users, self._recommendations, self._live = {}, {}, set()
        self.available = True

    # -- storage hooks ---------------------------------------------------------
    def _check(self) -> None:
        if not self.available:
            raise RealtimeUnavailable("realtime store is down")

    def _load(self, user_id: str) -> dict | None:
        return self._users.get(user_id)

    def _save(self, user_id: str, state: dict) -> None:
        self._users[user_id] = state

    def _set_live(self, streamer_id: str, live: bool) -> None:
        (self._live.add if live else self._live.discard)(streamer_id)

    def live_streamers(self) -> set:
        self._check()
        return set(self._live)

    def get_recommendations(self, user_id: str):
        self._check()
        return self._recommendations.get(user_id)

    def set_recommendations(self, user_id: str, streamers: list) -> None:
        self._check()
        self._recommendations[user_id] = streamers

    # -- logic -------------------------------------------------------------------
    def apply(self, record: dict) -> None:
        """Idempotent: an event that is delivered twice changes nothing the second time."""
        self._check()
        if record["type"] in ("stream_start", "stream_end"):
            self._set_live(record["payload"]["streamer_id"], record["type"] == "stream_start")
            return
        if record["type"] == "push_sent":       # our own feedback is not user activity
            return
        state = self._load(record["user_id"]) or {"events": [], "room": None}
        if any(e["event_id"] == record["event_id"] for e in state["events"]):
            return
        state["events"] = (state["events"] + [{"event_id": record["event_id"], "type": record["type"],
                                               "ts": record["ts"], **record["payload"]}])[-self.keep:]
        state["expires"] = record["ts"] + self.ttl
        if record["type"] == "enter_room":
            state["room"] = record["payload"]["streamer_id"]
        elif record["type"] == "leave_room":
            state["room"] = None
        self._save(record["user_id"], state)

    def get(self, user_id: str, now: float) -> dict | None:
        self._check()
        state = self._load(user_id)
        if not state or state["expires"] < now:
            return None
        events = state["events"]
        hour = [e for e in events if e["ts"] > now - 3600]
        return {
            "current_room": state["room"],
            "last_event_ts": events[-1]["ts"],
            "watch_seconds_1h": sum(e.get("watch_seconds", 0) for e in hour if e["type"] == "leave_room"),
            "recharge_clicks_1h": sum(e["type"] == "click_recharge" for e in hour),
            "recent_events": [{k: v for k, v in e.items() if k != "event_id"} for e in events[-10:]],
        }

    def events(self, user_id: str) -> list:
        self._check()
        state = self._load(user_id)
        return list(state["events"]) if state else []


def build_context(db, realtime: RealtimeStore, secret: bytes, user_id: str, now: float, cfg) -> dict:
    """The only way the decision service reads user state. A missing layer is None, never an error."""
    try:
        rt = realtime.get(user_id, now)
        rt_ok = True
    except RealtimeUnavailable:
        rt, rt_ok = None, False

    row = db.execute("SELECT features FROM recent_features WHERE pseudonym = ?",
                     (pseudonym(user_id, secret),)).fetchone()
    profile = db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)).fetchone()

    def count(sql, *args):
        return db.execute(sql, args).fetchone()[0]

    offset = cfg.tz_offset_hours * 3600
    day_start = (now + offset) // 86400 * 86400 - offset
    return {
        "user_id": user_id,
        "realtime_available": rt_ok,
        "realtime": rt,
        "recent": json.loads(row["features"]) if row else None,
        "static": None if profile is None else {
            "is_paid": bool(profile["is_paid"]), "push_opt_out": bool(profile["push_opt_out"]),
            "is_minor": bool(profile["is_minor"]), "is_vip": bool(profile["is_vip"]),
        },
        "segment": None if profile is None else ("repeat" if profile["is_paid"] else "first_time"),
        "touch": {
            "recover_enrolled_30d": count(
                "SELECT COUNT(*) FROM decisions WHERE user_id = ? AND reason = 'assigned' "
                "AND scenario = 'recover_recharge' AND decided_at > ?",
                user_id, now - cfg.cooldown_days * 86400) > 0,
            "pushes_today": count("SELECT COUNT(*) FROM sends WHERE user_id = ? AND sent_at >= ?", user_id, day_start),
            "bonus_offers_30d": count("SELECT COUNT(*) FROM decisions WHERE action = 'bonus' AND decided_at > ?",
                                      now - 30 * 86400),
            "active_intervention": count(
                "SELECT COUNT(*) FROM decisions WHERE user_id = ? AND reason = 'assigned' AND action != 'none' "
                "AND send_status IN ('scheduled', 'sent') AND decided_at > ?", user_id, now - 86400) > 0,
        },
    }
