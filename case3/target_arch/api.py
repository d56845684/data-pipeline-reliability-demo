"""The synchronous side: accept events, and serve reads of results that were computed elsewhere."""
import json
import time

from fastapi import FastAPI
from pydantic import BaseModel

from engagement.store import get_setting, set_setting

from .aws import build_platform
from .layers import build_context

platform, _ = build_platform()
app = FastAPI(title="Engagement platform PoC")


class Event(BaseModel):
    event_id: str
    user_id: str
    type: str
    payload: dict = {}


class User(BaseModel):
    is_paid: bool = False
    push_opt_out: bool = False
    is_minor: bool = False
    is_vip: bool = False
    follows: list[str] = []


@app.post("/events")
def post_event(event: Event):
    """Event Gateway. Returns as soon as the event is on the stream."""
    return {"status": platform.ingest(event.model_dump(), time.time())}


@app.get("/users/{user_id}/recommendations")
def get_recommendations(user_id: str):
    """The only synchronous path: reads a list that is already there, or the popular list."""
    return platform.recommendations(user_id)


@app.put("/users/{user_id}")
def put_user(user_id: str, user: User):
    """Stand-in for the existing account, consent and follow services."""
    platform.add_user(user_id, user.is_paid, user.push_opt_out, user.is_minor, user.is_vip, user.follows)
    return {"user_id": user_id}


@app.get("/users/{user_id}/context")
def get_context(user_id: str):
    return build_context(platform.db, platform.realtime, platform.secret, user_id, time.time(), platform.cfg)


@app.get("/users/{user_id}/decisions")
def get_decisions(user_id: str):
    rows = platform.db.execute(
        "SELECT decision_id, scenario, decided_at, reason, arm, action, propensity, send_status, send_note, "
        "guardrails, judge, scores, rule_version FROM decisions WHERE user_id = ? ORDER BY decided_at", (user_id,)).fetchall()
    return [{k: (json.loads(r[k]) if k in ("guardrails", "judge", "scores") else r[k]) for k in r.keys()} for r in rows]


@app.put("/ev-table")
def put_ev_table(table: dict[str, dict[str, float]]):
    """Output of the experiment loaded into the decision service."""
    platform.load_ev_table(table)
    return platform.ev_table()


@app.post("/batch")
def post_batch():
    """The hourly or daily job, on demand: Athena over the lake -> recent layer and recommendation lists."""
    now = time.time()
    return {"users_in_recent_layer": platform.run_batch(now)}


@app.post("/churn-scan")
def post_churn_scan():
    return {"high_risk": platform.schedule_churn_scan(time.time())}


@app.put("/kill-switch/{state}")
def put_kill_switch(state: str):
    set_setting(platform.db, "kill_switch", "on" if state == "on" else "off")
    return {"kill_switch": get_setting(platform.db, "kill_switch")}


@app.get("/status")
def get_status():
    def count(sql):
        return platform.db.execute(sql).fetchone()[0]

    return {
        "triggers_waiting": count("SELECT COUNT(*) FROM triggers WHERE done = 0"),
        "decisions": count("SELECT COUNT(*) FROM decisions"),
        "sends": count("SELECT COUNT(*) FROM sends"),
        "checkpoints": [{"consumer": r["consumer"], "shard": r["shard_id"]} for r in
                        platform.db.execute("SELECT consumer, shard_id FROM checkpoints ORDER BY consumer").fetchall()],
        "quarantined_since_start": len(platform.gateway.quarantine),
    }
