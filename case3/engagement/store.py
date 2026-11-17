"""SQLite stands in for the single Postgres database of the MVP."""
import sqlite3

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    event_id TEXT PRIMARY KEY,          -- the dedupe key
    user_id  TEXT NOT NULL,
    type     TEXT NOT NULL,
    ts       DOUBLE PRECISION NOT NULL, -- server time, epoch seconds
    payload  TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS events_user ON events (user_id, type, ts);

-- Stand-in for the existing account, consent and billing services.
CREATE TABLE IF NOT EXISTS users (
    user_id      TEXT PRIMARY KEY,
    is_paid      INTEGER NOT NULL,
    push_opt_out INTEGER NOT NULL DEFAULT 0,
    is_minor     INTEGER NOT NULL DEFAULT 0,
    is_vip       INTEGER NOT NULL DEFAULT 0
);

-- Delayed work: one row per trigger event, picked up by polling.
CREATE TABLE IF NOT EXISTS pending (
    trigger_event_id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    due_at  DOUBLE PRECISION NOT NULL,
    done    INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS pending_due ON pending (done, due_at);

-- Decision log. Every decision is written, including "do nothing".
CREATE TABLE IF NOT EXISTS decisions (
    decision_id      TEXT PRIMARY KEY,
    trigger_event_id TEXT NOT NULL UNIQUE,
    user_id      TEXT NOT NULL,
    decided_at   DOUBLE PRECISION NOT NULL,
    scenario     TEXT NOT NULL DEFAULT 'recover_recharge',
    reason       TEXT NOT NULL,         -- 'assigned' or why nothing was done
    arm          TEXT,                  -- experiment arm, only when assigned
    action       TEXT NOT NULL,         -- none / remind / bonus
    propensity   DOUBLE PRECISION,                -- probability this action was chosen
    send_at      DOUBLE PRECISION,
    send_status  TEXT NOT NULL,         -- none / scheduled / sent / skipped
    send_note    TEXT,
    context      TEXT NOT NULL,         -- snapshot used for the decision
    guardrails   TEXT NOT NULL,
    judge        TEXT NOT NULL,         -- judge output and whether it was used
    scores       TEXT NOT NULL DEFAULT '{}',  -- candidates and EV per action (target architecture)
    rule_version TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS decisions_user ON decisions (user_id, decided_at);
CREATE INDEX IF NOT EXISTS decisions_send ON decisions (send_status, send_at);
CREATE INDEX IF NOT EXISTS decisions_action ON decisions (action, decided_at);

-- Stand-in for the push provider. The primary key is the idempotency key.
CREATE TABLE IF NOT EXISTS sends (
    decision_id TEXT PRIMARY KEY,
    user_id  TEXT NOT NULL,
    action   TEXT NOT NULL,
    sent_at  DOUBLE PRECISION NOT NULL
);
CREATE INDEX IF NOT EXISTS sends_user ON sends (user_id, sent_at);

CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
"""


def connect(path: str = ":memory:") -> sqlite3.Connection:
    db = sqlite3.connect(path, check_same_thread=False)
    db.row_factory = sqlite3.Row
    db.executescript(SCHEMA)
    return db


def get_setting(db, key: str, default: str = "") -> str:
    row = db.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
    return row["value"] if row else default


def set_setting(db, key: str, value: str) -> None:
    db.execute(
        "INSERT INTO settings (key, value) VALUES (?, ?) "
        "ON CONFLICT (key) DO UPDATE SET value = excluded.value",
        (key, value),
    )
    db.commit()
