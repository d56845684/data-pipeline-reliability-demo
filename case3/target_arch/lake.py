"""Batch path: Firehose -> data lake -> batch features -> recent layer.

Locally, JSONL files stand in for S3. On AWS the same rows come back from Athena
(see aws.py). Either way the aggregation below is the same code.
"""
import glob
import json
import os
from collections import defaultdict
from datetime import datetime, timezone

from .deid import deidentify


class Firehose:
    """Local stand-in for Data Firehose plus its de-identification transform."""

    def __init__(self, directory: str, secret: bytes):
        self.directory, self.secret = directory, secret
        self.buffer, self.parts = [], 0

    def handle(self, record: dict) -> None:
        self.buffer.append(deidentify(record, self.secret))

    def flush(self) -> int:
        by_day = defaultdict(list)
        for row in self.buffer:
            by_day[datetime.fromtimestamp(row["ts"], timezone.utc).strftime("%Y-%m-%d")].append(row)
        for day, rows in by_day.items():
            folder = os.path.join(self.directory, f"dt={day}")
            os.makedirs(folder, exist_ok=True)
            self.parts += 1
            with open(os.path.join(folder, f"part-{self.parts:05d}.jsonl"), "w", encoding="utf-8") as fh:
                fh.writelines(json.dumps(r, ensure_ascii=False) + "\n" for r in rows)
        written, self.buffer = len(self.buffer), []
        return written


def read_local_lake(directory: str):
    for path in sorted(glob.glob(os.path.join(directory, "dt=*", "*.jsonl"))):
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                yield json.loads(line)


def build_recent_layer(events, db, now: float) -> int:
    """Hourly or daily job. Recomputes from raw events, so the logic can change and be rerun."""
    seen, users = set(), defaultdict(lambda: {
        "watch_7d": 0, "watch_30d": 0, "days_7d": set(), "days_prev_7d": set(), "last_login": None,
        "recharge_90d": 0.0, "streamers": defaultdict(float),
    })
    for e in events:
        if e["event_id"] in seen:               # the lake is at-least-once too
            continue
        seen.add(e["event_id"])
        age = (now - float(e["ts"])) / 86400
        if age < 0:
            continue
        u, p = users[e["user_key"]], json.loads(e["payload"])
        if e["type"] == "login":
            u["last_login"] = max(u["last_login"] or 0, float(e["ts"]))
            (u["days_7d"] if age < 7 else u["days_prev_7d"] if age < 14 else set()).add(int(age))
        elif e["type"] == "leave_room" and age < 30:
            u["watch_30d"] += p["watch_seconds"]
            u["watch_7d"] += p["watch_seconds"] if age < 7 else 0
            u["streamers"][p["streamer_id"]] += p["watch_seconds"]
        elif e["type"] == "click_recharge" and age < 30:
            u["streamers"][p.get("streamer_id", "?")] += 1800   # paying intent outweighs watching
        elif e["type"] == "recharge_success" and age < 90:
            u["recharge_90d"] += p["amount"]

    for key, u in users.items():
        idle_days = None if u["last_login"] is None else (now - u["last_login"]) / 86400
        if idle_days is not None and idle_days >= 5:
            risk = "high"
        elif len(u["days_prev_7d"]) >= 2 and len(u["days_7d"]) * 2 <= len(u["days_prev_7d"]):
            risk = "medium"
        else:
            risk = "low"
        features = {
            "watch_seconds_7d": u["watch_7d"], "watch_seconds_30d": u["watch_30d"],
            "active_days_7d": len(u["days_7d"]), "active_days_prev_7d": len(u["days_prev_7d"]),
            "days_since_login": None if idle_days is None else round(idle_days, 1),
            "recharge_amount_90d": u["recharge_90d"],
            "top_streamers": [s for s, _ in sorted(u["streamers"].items(), key=lambda kv: -kv[1]) if s != "?"][:3],
            "churn_risk": risk,                  # a rule, not a model
        }
        db.execute(
            "INSERT INTO recent_features (pseudonym, features, computed_at) VALUES (?, ?, ?) "
            "ON CONFLICT (pseudonym) DO UPDATE SET features = excluded.features, computed_at = excluded.computed_at",
            (key, json.dumps(features), now))
    db.commit()
    return len(users)
