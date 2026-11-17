"""The one service of the MVP: ingest -> trigger -> decide -> log -> send."""
import json
import uuid

from .config import Config
from .context import build_context
from .decide import decide
from .judge import KeywordJudge
from .store import connect, get_setting

EVENT_TYPES = {"cancel_recharge", "recharge_success", "chat", "enter_room", "push_opt_out"}
TRIGGER_TYPES = {"cancel_recharge"}     # only these start a decision
_NAMESPACE = uuid.UUID("6f1f3f0e-2b7a-4d53-9f43-0d1c5d9a7e11")


class InvalidEvent(ValueError):
    pass


class Engine:
    def __init__(self, db=None, cfg: Config | None = None, judge=None):
        self.db = db or connect()
        self.cfg = cfg or Config()
        self.judge = judge or KeywordJudge()

    # -- Event Gateway: validate, dedupe, then trigger -------------------------
    def ingest(self, event: dict, now: float) -> str:
        for key in ("event_id", "user_id", "type"):
            if not isinstance(event.get(key), str) or not event[key]:
                raise InvalidEvent(f"missing or invalid field: {key}")
        if event["type"] not in EVENT_TYPES:
            raise InvalidEvent(f"unknown event type: {event['type']}")
        payload = event.get("payload") or {}
        if not isinstance(payload, dict):
            raise InvalidEvent("payload must be an object")

        cur = self.db.execute(
            "INSERT OR IGNORE INTO events (event_id, user_id, type, ts, payload) VALUES (?, ?, ?, ?, ?)",
            (event["event_id"], event["user_id"], event["type"], now, json.dumps(payload, ensure_ascii=False)),
        )
        if cur.rowcount == 0:
            return "duplicate"
        if event["type"] in TRIGGER_TYPES:
            self.db.execute(
                "INSERT OR IGNORE INTO pending (trigger_event_id, user_id, due_at) VALUES (?, ?, ?)",
                (event["event_id"], event["user_id"], now + self.cfg.buffer_seconds),
            )
        self.db.commit()
        return "accepted"

    # -- Polling worker: decide what is due, then send what is due -------------
    def tick(self, now: float) -> dict:
        decided = self._decide_due(now)
        sent = self._send_due(now)
        self.db.commit()
        return {"decided": decided, "sent": sent}

    def _decide_due(self, now: float) -> list:
        out = []
        due = self.db.execute(
            "SELECT p.trigger_event_id, p.user_id, e.ts, e.payload FROM pending p "
            "JOIN events e ON e.event_id = p.trigger_event_id "
            "WHERE p.done = 0 AND p.due_at <= ? ORDER BY p.due_at",
            (now,),
        ).fetchall()
        for row in due:
            cancel = {"ts": row["ts"], "payload": json.loads(row["payload"])}
            ctx = build_context(self.db, row["user_id"], cancel, now, self.cfg)
            d = decide(ctx, now, self.cfg, self.judge)
            # Derived from the trigger, so a retry writes the same id.
            decision_id = str(uuid.uuid5(_NAMESPACE, row["trigger_event_id"]))
            self.db.execute(
                "INSERT OR IGNORE INTO decisions (decision_id, trigger_event_id, user_id, decided_at, "
                "reason, arm, action, propensity, send_at, send_status, context, guardrails, judge, rule_version) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (decision_id, row["trigger_event_id"], row["user_id"], now, d.reason, d.arm, d.action,
                 d.propensity, d.send_at, "none" if d.action == "none" else "scheduled",
                 json.dumps(ctx, ensure_ascii=False), json.dumps(d.guardrails),
                 json.dumps(d.judge, ensure_ascii=False), self.cfg.rule_version),
            )
            self.db.execute("UPDATE pending SET done = 1 WHERE trigger_event_id = ?", (row["trigger_event_id"],))
            out.append({"decision_id": decision_id, "user_id": row["user_id"], "reason": d.reason,
                        "arm": d.arm, "action": d.action, "segment": ctx["segment"]})
        return out

    def _send_due(self, now: float) -> list:
        out = []
        kill = get_setting(self.db, "kill_switch", "off") == "on"
        due = self.db.execute(
            "SELECT decision_id, user_id, action, decided_at, context FROM decisions "
            "WHERE send_status = 'scheduled' AND send_at <= ?",
            (now,),
        ).fetchall()
        for row in due:
            skip = "kill_switch" if kill else self._recheck(row)
            if skip:
                self._mark(row["decision_id"], "skipped", skip)
                continue
            # The provider dedupes on decision_id, so a crash between these two
            # statements leads to a retry that sends nothing twice.
            self.db.execute(
                "INSERT OR IGNORE INTO sends (decision_id, user_id, action, sent_at) VALUES (?, ?, ?, ?)",
                (row["decision_id"], row["user_id"], row["action"], now),
            )
            self._mark(row["decision_id"], "sent", None)
            out.append({"decision_id": row["decision_id"], "user_id": row["user_id"], "action": row["action"],
                        "decided_at": row["decided_at"], "segment": json.loads(row["context"])["segment"]})
        return out

    def _recheck(self, row) -> str | None:
        """State may have changed between the decision and the send."""
        recharged = self.db.execute(
            "SELECT 1 FROM events WHERE user_id = ? AND type = 'recharge_success' AND ts > ? LIMIT 1",
            (row["user_id"], json.loads(row["context"])["cancel_ts"]),
        ).fetchone()
        if recharged:
            return "already_recharged"
        user = self.db.execute("SELECT push_opt_out FROM users WHERE user_id = ?", (row["user_id"],)).fetchone()
        if user is None or user["push_opt_out"]:
            return "opted_out"
        return None

    def _mark(self, decision_id: str, status: str, note: str | None) -> None:
        self.db.execute(
            "UPDATE decisions SET send_status = ?, send_note = ? WHERE decision_id = ?",
            (status, note, decision_id),
        )
