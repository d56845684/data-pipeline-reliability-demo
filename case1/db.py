"""PostgreSQL 存取層 — 倉儲落地（冪等 upsert）+ ETL 執行歷史（動態基線資料來源）。"""
import json
import time
from datetime import date

import psycopg2
from psycopg2.extras import execute_values

import config

DDL = """
CREATE TABLE IF NOT EXISTS etl_run_stats (
    pipeline      text        NOT NULL,
    business_date date        NOT NULL,
    status        int         NOT NULL,  -- 0 ok / 1 warning / 2 failed / 3 missing
    rows_loaded   int         NOT NULL DEFAULT 0,
    duration_s    real        NOT NULL DEFAULT 0,
    zscore        real        NOT NULL DEFAULT 0,
    scenario      text,
    checks        jsonb,
    run_at        timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (pipeline, business_date)
);

CREATE TABLE IF NOT EXISTS warehouse_records (
    pipeline           text NOT NULL,
    record_id          text NOT NULL,
    customer_id_masked text,
    amount             numeric,
    category           text,
    region             text,
    event_date         date,
    loaded_at          timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (pipeline, record_id)
);
"""


def connect(retries: int = 30, delay: float = 2.0):
    last = None
    for _ in range(retries):
        try:
            conn = psycopg2.connect(config.PG_DSN)
            conn.autocommit = True
            return conn
        except psycopg2.OperationalError as e:  # postgres 還在啟動
            last = e
            time.sleep(delay)
    raise last


def init_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(DDL)


def get_same_daytype_history(conn, pipeline: str, business_date: date, limit: int = 8) -> list[int]:
    """取同期（同為平日或同為週末）且成功的歷史筆數，供動態基線使用。"""
    is_weekend = business_date.weekday() >= 5
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT rows_loaded FROM etl_run_stats
            WHERE pipeline = %s
              AND status IN (0, 1)
              AND business_date < %s
              AND (EXTRACT(ISODOW FROM business_date) >= 6) = %s
            ORDER BY business_date DESC LIMIT %s
            """,
            (pipeline, business_date, is_weekend, limit),
        )
        return [r[0] for r in cur.fetchall()]


def get_last_success_epoch(conn, pipeline: str) -> float | None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT EXTRACT(EPOCH FROM max(run_at)) FROM etl_run_stats "
            "WHERE pipeline = %s AND status IN (0, 1)",
            (pipeline,),
        )
        row = cur.fetchone()
        return float(row[0]) if row and row[0] else None


def upsert_records(conn, pipeline: str, rows: list[dict]) -> int:
    """冪等載入：重跑/回補不會產生重複資料（對應 runbook 的冪等回補設計）。"""
    if not rows:
        return 0
    values = [
        (pipeline, r["record_id"], r["customer_id_masked"] or None,
         r["amount"] or None, r["category"], r["region"], r["event_date"])
        for r in rows
    ]
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO warehouse_records
                (pipeline, record_id, customer_id_masked, amount, category, region, event_date)
            VALUES %s
            ON CONFLICT (pipeline, record_id) DO UPDATE SET
                customer_id_masked = EXCLUDED.customer_id_masked,
                amount = EXCLUDED.amount,
                loaded_at = now()
            """,
            values,
            page_size=1000,
        )
    return len(values)


def record_run(conn, pipeline: str, business_date: date, status: int, rows_loaded: int,
               duration_s: float, zscore: float, scenario: str | None, checks: list[dict]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO etl_run_stats
                (pipeline, business_date, status, rows_loaded, duration_s, zscore, scenario, checks, run_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now())
            ON CONFLICT (pipeline, business_date) DO UPDATE SET
                status = EXCLUDED.status,
                rows_loaded = EXCLUDED.rows_loaded,
                duration_s = EXCLUDED.duration_s,
                zscore = EXCLUDED.zscore,
                scenario = EXCLUDED.scenario,
                checks = EXCLUDED.checks,
                run_at = now()
            """,
            (pipeline, business_date, status, rows_loaded, duration_s, zscore,
             scenario, json.dumps(checks)),
        )
