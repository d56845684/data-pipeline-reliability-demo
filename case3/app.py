"""HTTP wrapper around the engine, for a live demo.

    BUFFER_SECONDS=10 JUDGE=jev uvicorn app:app --port 8000

A background task polls every second, the same way the MVP polls its pending table.
"""
import asyncio
import json
import os
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from engagement import Config, Engine, InvalidEvent
from engagement.judge import JevJudge, load_api_key
from engagement.report import ev_table, experiment_rows, funnel
from engagement.store import connect, get_setting, set_setting

engine = Engine(
    db=connect(os.environ.get("DB_PATH", ":memory:")),
    cfg=Config(buffer_seconds=int(os.environ.get("BUFFER_SECONDS", "600"))),
    # JUDGE=jev calls the hosted judge model (shadow only); default is the offline stub.
    judge=JevJudge(load_api_key()) if os.environ.get("JUDGE") == "jev" else None,
)


@asynccontextmanager
async def lifespan(_: FastAPI):
    async def poll():
        while True:
            engine.tick(time.time())
            await asyncio.sleep(1)

    task = asyncio.create_task(poll())
    yield
    task.cancel()


app = FastAPI(title="Engagement MVP PoC", lifespan=lifespan)


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


@app.put("/users/{user_id}")
def put_user(user_id: str, user: User):
    """Stand-in for the existing account and consent services."""
    engine.db.execute(
        "INSERT OR REPLACE INTO users VALUES (?, ?, ?, ?, ?)",
        (user_id, user.is_paid, user.push_opt_out, user.is_minor, user.is_vip),
    )
    engine.db.commit()
    return {"user_id": user_id, **user.model_dump()}


@app.post("/events")
def post_event(event: Event):
    try:
        return {"status": engine.ingest(event.model_dump(), time.time())}
    except InvalidEvent as exc:
        raise HTTPException(status_code=422, detail=str(exc))


@app.get("/users/{user_id}/decisions")
def get_decisions(user_id: str):
    rows = engine.db.execute("SELECT * FROM decisions WHERE user_id = ? ORDER BY decided_at", (user_id,)).fetchall()
    return [
        {**dict(r), **{k: json.loads(r[k]) for k in ("context", "guardrails", "judge")}}
        for r in rows
    ]


@app.get("/report")
def get_report():
    rows = experiment_rows(engine.db, engine.cfg)
    return {"funnel": funnel(engine.db), "experiment": rows, "ev_table": ev_table(rows)}


@app.put("/kill-switch/{state}")
def put_kill_switch(state: str):
    if state not in ("on", "off"):
        raise HTTPException(status_code=422, detail="state must be on or off")
    set_setting(engine.db, "kill_switch", state)
    return {"kill_switch": get_setting(engine.db, "kill_switch")}
