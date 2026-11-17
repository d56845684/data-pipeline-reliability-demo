"""Wires the target architecture together.

    Gateway -> Stream -+-> Context Builder -> realtime layer
                       +-> Firehose -> data lake -> batch -> recent layer
                       +-> Trigger (only specific events)
    Scheduler ---------> Trigger
    Trigger -> Decision Service -> Decision log -> Action Executor -> channels
    App request -------> reads a precomputed list (the only synchronous path)
"""
import json
import uuid

from engagement.config import Config
from engagement.judge import KeywordJudge
from engagement.store import connect, get_setting

from . import decision
from .gateway import Gateway
from .deid import pseudonym
from .lake import Firehose, build_recent_layer, read_local_lake
from .layers import RealtimeStore, RealtimeUnavailable, build_context
from .stream import Stream

SCHEMA = """
CREATE TABLE IF NOT EXISTS triggers (       -- stand-in for EventBridge Scheduler: due work, survives restarts
    trigger_id TEXT PRIMARY KEY,
    scenario TEXT NOT NULL, user_id TEXT NOT NULL, due_at DOUBLE PRECISION NOT NULL,
    payload TEXT NOT NULL, done INTEGER NOT NULL DEFAULT 0,
    held INTEGER NOT NULL DEFAULT 0     -- 1 while an external scheduler owns the wake-up
);
CREATE TABLE IF NOT EXISTS recent_features (pseudonym TEXT PRIMARY KEY, features TEXT NOT NULL, computed_at DOUBLE PRECISION NOT NULL);
CREATE TABLE IF NOT EXISTS follows (user_id TEXT, streamer_id TEXT, PRIMARY KEY (user_id, streamer_id));  -- existing service
CREATE TABLE IF NOT EXISTS ev_table (segment TEXT, action TEXT, ev DOUBLE PRECISION NOT NULL, PRIMARY KEY (segment, action));
"""
CHANNELS = {"remind": ["push"], "bonus": ["push", "bonus_grant"], "live_push": ["push"], "content_push": ["push"]}
POPULAR = ["s_hot1", "s_hot2", "s_hot3"]
_NAMESPACE = uuid.UUID("0b0f6f52-7a0e-4b53-8a55-2f0c53a1f4d7")


class Platform:
    def __init__(self, lake_dir: str | None = None, cfg: Config | None = None, judge=None,
                 secret: bytes = b"poc-secret", db=None, stream=None, realtime=None,
                 scheduler=None, lake_reader=None, first_time=None):
        """Every component can be swapped. The defaults run in this process; aws.py supplies
        Kinesis, Redis, Postgres, EventBridge Scheduler and Athena behind the same interfaces."""
        self.cfg = cfg or Config()
        self.judge = judge or KeywordJudge()
        self.secret = secret
        self.db = db or connect()
        self.db.executescript(SCHEMA)
        self.db.commit()
        self.stream = stream or Stream()
        self.gateway = Gateway(self.stream, first_time)
        self.realtime = realtime or RealtimeStore()
        self.scheduler = scheduler
        # Locally this process also plays Firehose. On AWS, Firehose reads the stream itself.
        self.firehose = Firehose(lake_dir, secret) if lake_dir else None
        self.lake_reader = lake_reader or (lambda: read_local_lake(lake_dir))
        # Independent consumers of the same stream, each with its own checkpoint.
        self.context_builder = self.stream.consumer("context-builder")
        self.trigger_reader = self.stream.consumer("trigger")
        self.lake_writer = self.stream.consumer("firehose") if self.firehose else None

    # -- ingestion ---------------------------------------------------------------
    def ingest(self, event: dict, now: float) -> str:
        return self.gateway.ingest(event, now)

    # -- triggers ----------------------------------------------------------------
    def _schedule(self, trigger_id, scenario, user_id, due_at, payload, delayed=False):
        """Delayed work goes to the external scheduler when there is one; it calls back with release()."""
        held = int(bool(delayed and self.scheduler))
        cur = self.db.execute(
            "INSERT INTO triggers (trigger_id, scenario, user_id, due_at, payload, held) VALUES (?, ?, ?, ?, ?, ?) "
            "ON CONFLICT (trigger_id) DO NOTHING",
            (trigger_id, scenario, user_id, due_at, json.dumps(payload, ensure_ascii=False), held))
        if held and cur.rowcount:
            self.scheduler.schedule(trigger_id, due_at)

    def release(self, trigger_id: str) -> None:
        """The scheduler says the time has come."""
        self.db.execute("UPDATE triggers SET held = 0 WHERE trigger_id = ?", (trigger_id,))
        self.db.commit()

    def _on_event(self, record: dict) -> None:
        """Event-type trigger. Most events only update the context and never reach a decision."""
        p = record["payload"]
        if record["type"] == "cancel_recharge":
            self._schedule(record["event_id"], "recover_recharge", record["user_id"],
                           record["ts"] + self.cfg.buffer_seconds,
                           {"cancel_ts": record["ts"], "step": p["step"], "error_code": p.get("error_code"),
                            "streamer_id": p.get("streamer_id")}, delayed=True)
        elif record["type"] == "stream_start":
            for row in self.db.execute("SELECT user_id FROM follows WHERE streamer_id = ?", (p["streamer_id"],)):
                self._schedule(f"{record['event_id']}:{row['user_id']}", "live_notify", row["user_id"],
                               record["ts"], {"streamer_id": p["streamer_id"]})

    def schedule_churn_scan(self, now: float) -> int:
        """Schedule-type trigger: finds what did not happen. One trigger per user per day."""
        day, found = int(now // 86400), 0
        for user in self.db.execute("SELECT user_id FROM users").fetchall():
            row = self.db.execute("SELECT features FROM recent_features WHERE pseudonym = ?",
                                  (pseudonym(user["user_id"], self.secret),)).fetchone()
            if row and json.loads(row["features"])["churn_risk"] == "high":
                self._schedule(f"winback:{user['user_id']}:{day}", "winback", user["user_id"], now, {})
                found += 1
        self.db.commit()
        return found

    def recommendations(self, user_id: str) -> dict:
        """Request-type trigger, the only synchronous path. Reads a result, computes nothing."""
        try:
            ready = self.realtime.get_recommendations(user_id)
            if ready:
                return {"source": "precomputed", "streamers": ready}
            return {"source": "popular", "streamers": POPULAR}
        except RealtimeUnavailable:
            return {"source": "popular (engagement unavailable)", "streamers": POPULAR}

    # -- batch -------------------------------------------------------------------
    def run_batch(self, now: float) -> int:
        users = build_recent_layer(self.lake_reader(), self.db, now)
        try:
            live = self.realtime.live_streamers()
        except RealtimeUnavailable:
            return users
        for user in self.db.execute("SELECT user_id FROM users").fetchall():
            row = self.db.execute("SELECT features FROM recent_features WHERE pseudonym = ?",
                                  (pseudonym(user["user_id"], self.secret),)).fetchone()
            followed = [r["streamer_id"] for r in self.db.execute(
                "SELECT streamer_id FROM follows WHERE user_id = ?", (user["user_id"],))]
            liked = json.loads(row["features"])["top_streamers"] if row else []
            ranked = [s for s in dict.fromkeys(followed + liked) if s in live]
            if ranked:
                self.realtime.set_recommendations(user["user_id"], (ranked + POPULAR)[:5])
        return users

    # -- the worker ----------------------------------------------------------------
    def tick(self, now: float) -> dict:
        self.context_builder.run(self.realtime.apply)
        if self.firehose:
            self.lake_writer.run(self.firehose.handle)
            self.firehose.flush()
        self.trigger_reader.run(self._on_event)
        self.db.commit()
        decided = self._decide_due(now)
        sent = self._send_due(now)
        self.db.commit()
        return {"decided": decided, "sent": sent}

    def _decide_due(self, now: float) -> list:
        # A held trigger waits for the scheduler; if the wake-up never arrives it is picked up 3 minutes late.
        due = self.db.execute(
            "SELECT * FROM triggers WHERE done = 0 AND due_at <= ? AND (held = 0 OR due_at + 180 <= ?)",
            (now, now)).fetchall()
        out = []
        # Several triggers can hit one user at once: the higher priority scenario goes first.
        for row in sorted(due, key=lambda r: (decision.PRIORITY[r["scenario"]], r["due_at"])):
            ctx = build_context(self.db, self.realtime, self.secret, row["user_id"], now, self.cfg)
            ctx["scenario"], ctx["trigger"] = row["scenario"], json.loads(row["payload"])
            judge_output = None
            if row["scenario"] == "recover_recharge":
                try:
                    chat = [e["text"] for e in self.realtime.events(row["user_id"])
                            if e["type"] == "chat" and ctx["trigger"]["cancel_ts"] - 3600 <= e["ts"] <= ctx["trigger"]["cancel_ts"]]
                except RealtimeUnavailable:
                    chat = []
                ctx["trigger"]["chat"] = chat
                judge_output = self.judge.cancel_reason({
                    "recent_chat": chat, "segment": ctx["segment"],
                    "cancel_step": ctx["trigger"]["step"], "error_code": ctx["trigger"]["error_code"]})
            d, scores = decision.decide(row["scenario"], ctx, now, self.cfg, self.ev_table(), judge_output)
            decision_id = str(uuid.uuid5(_NAMESPACE, row["trigger_id"]))
            self.db.execute(
                "INSERT INTO decisions (decision_id, trigger_event_id, user_id, decided_at, scenario, reason, arm, action, "
                "propensity, send_at, send_status, context, guardrails, judge, scores, rule_version) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
                (decision_id, row["trigger_id"], row["user_id"], now, row["scenario"], d.reason, d.arm, d.action, d.propensity, d.send_at,
                 "none" if d.action == "none" else "scheduled", json.dumps(ctx, ensure_ascii=False),
                 json.dumps(d.guardrails), json.dumps(d.judge, ensure_ascii=False), json.dumps(scores),
                 self.cfg.rule_version))
            self.db.execute("UPDATE triggers SET done = 1 WHERE trigger_id = ?", (row["trigger_id"],))
            out.append({"decision_id": decision_id, "user_id": row["user_id"], "scenario": row["scenario"],
                        "reason": d.reason, "arm": d.arm, "action": d.action, "scores": scores, "judge": d.judge})
        return out

    def _send_due(self, now: float) -> list:
        out = []
        kill = get_setting(self.db, "kill_switch", "off") == "on"
        for row in self.db.execute(
                "SELECT decision_id, user_id, action, context FROM decisions "
                "WHERE send_status = 'scheduled' AND send_at <= ?", (now,)).fetchall():
            ctx = json.loads(row["context"])
            try:
                skip = "kill_switch" if kill else self._recheck(row["user_id"], ctx)
            except RealtimeUnavailable:
                continue                                  # cannot verify: keep it scheduled, do not guess
            if skip:
                self.db.execute("UPDATE decisions SET send_status = 'skipped', send_note = ? WHERE decision_id = ?",
                                (skip, row["decision_id"]))
                continue
            self.db.execute("INSERT INTO sends (decision_id, user_id, action, sent_at) VALUES (?, ?, ?, ?) "
                            "ON CONFLICT (decision_id) DO NOTHING",
                            (row["decision_id"], row["user_id"], row["action"], now))
            self.db.execute("UPDATE decisions SET send_status = 'sent' WHERE decision_id = ?", (row["decision_id"],))
            # Feedback re-enters through the gateway, tied to the decision that caused it.
            self.ingest({"event_id": f"sent:{row['decision_id']}", "user_id": row["user_id"], "type": "push_sent",
                         "payload": {"decision_id": row["decision_id"], "channels": CHANNELS[row["action"]]}}, now)
            out.append({"decision_id": row["decision_id"], "user_id": row["user_id"], "action": row["action"],
                        "channels": CHANNELS[row["action"]]})
        return out

    def _recheck(self, user_id: str, ctx: dict) -> str | None:
        user = self.db.execute("SELECT push_opt_out FROM users WHERE user_id = ?", (user_id,)).fetchone()
        if user is None or user["push_opt_out"]:
            return "opted_out"
        if ctx["scenario"] == "recover_recharge" and any(
                e["type"] == "recharge_success" and e["ts"] > ctx["trigger"]["cancel_ts"]
                for e in self.realtime.events(user_id)):
            return "already_recharged"
        return None

    # -- small helpers ---------------------------------------------------------------
    def ev_table(self) -> dict:
        table = {}
        for r in self.db.execute("SELECT segment, action, ev FROM ev_table"):
            table.setdefault(r["segment"], {})[r["action"]] = r["ev"]
        return table

    def load_ev_table(self, table: dict) -> None:
        for segment, actions in table.items():
            for action, value in actions.items():
                self.db.execute("INSERT INTO ev_table (segment, action, ev) VALUES (?, ?, ?) "
                                "ON CONFLICT (segment, action) DO UPDATE SET ev = excluded.ev", (segment, action, value))
        self.db.commit()

    def add_user(self, user_id: str, is_paid=False, push_opt_out=False, is_minor=False, is_vip=False, follows=()):
        self.db.execute(
            "INSERT INTO users (user_id, is_paid, push_opt_out, is_minor, is_vip) VALUES (?, ?, ?, ?, ?) "
            "ON CONFLICT (user_id) DO UPDATE SET is_paid = excluded.is_paid, push_opt_out = excluded.push_opt_out, "
            "is_minor = excluded.is_minor, is_vip = excluded.is_vip",
            (user_id, int(is_paid), int(push_opt_out), int(is_minor), int(is_vip)))
        for streamer in follows:
            self.db.execute("INSERT INTO follows (user_id, streamer_id) VALUES (?, ?) ON CONFLICT DO NOTHING",
                            (user_id, streamer))
        self.db.commit()
