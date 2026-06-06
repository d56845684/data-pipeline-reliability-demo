"""共用層 — RabbitMQ 連線/拓撲宣告 + PostgreSQL 冪等寫入。"""
import json
import time

import pika
import psycopg2
from psycopg2.extras import execute_values

import config

DDL = """
CREATE TABLE IF NOT EXISTS c2_chunks (
    file_id     text NOT NULL,
    chunk_idx   int  NOT NULL,
    tenant      text NOT NULL,
    upload_ts   double precision NOT NULL,
    e2e_seconds real,
    tokens      int,
    embedding   real[],               -- 模擬 embedding 向量（production 為 Milvus/pgvector）
    embedded_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (file_id, chunk_idx)   -- 冪等鍵：重送/重放不會重複入庫
);
-- 既有表的前向遷移（demo 環境簡化處理；production 走 migration 工具）
ALTER TABLE c2_chunks ADD COLUMN IF NOT EXISTS tokens int;
ALTER TABLE c2_chunks ADD COLUMN IF NOT EXISTS embedding real[];
"""


# ---------- RabbitMQ ----------

def connect_rabbit(retries: int = 30, delay: float = 2.0):
    last = None
    for _ in range(retries):
        try:
            conn = pika.BlockingConnection(pika.URLParameters(config.RABBIT_URL))
            return conn, conn.channel()
        except pika.exceptions.AMQPConnectionError as e:
            last = e
            time.sleep(delay)
    raise last


def declare_topology(ch) -> None:
    """宣告全部佇列（共用 + 每租戶 + DLQ），與模式無關——切換模式不需重建。"""
    ch.exchange_declare(exchange=config.DLX_EXCHANGE, exchange_type="fanout", durable=True)
    ch.queue_declare(queue=config.DLQ_QUEUE, durable=True)
    ch.queue_bind(queue=config.DLQ_QUEUE, exchange=config.DLX_EXCHANGE)

    dlx_args = {"x-dead-letter-exchange": config.DLX_EXCHANGE}
    ch.queue_declare(queue=config.UPLOAD_QUEUE, durable=True)
    ch.queue_declare(queue=config.VECTOR_SHARED_QUEUE, durable=True, arguments=dlx_args)
    for tenant in config.ALL_TENANTS:
        ch.queue_declare(queue=f"c2.vector.{tenant}", durable=True, arguments=dlx_args)


def publish(ch, queue: str, message: dict) -> None:
    ch.basic_publish(
        exchange="",
        routing_key=queue,
        body=json.dumps(message),
        properties=pika.BasicProperties(delivery_mode=2),  # persistent
    )


# ---------- PostgreSQL ----------

def connect_pg(retries: int = 30, delay: float = 2.0):
    last = None
    for _ in range(retries):
        try:
            conn = psycopg2.connect(config.PG_DSN)
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(DDL)
            return conn
        except psycopg2.OperationalError as e:
            last = e
            time.sleep(delay)
    raise last


def insert_chunks(pg, rows: list[tuple]) -> set[tuple]:
    """冪等批次寫入。rows: (file_id, chunk_idx, tenant, upload_ts, e2e_seconds, tokens, embedding)

    回傳實際新插入的 (file_id, chunk_idx)——沒回傳的代表重複（冪等跳過）。
    """
    if not rows:
        return set()
    with pg.cursor() as cur:
        inserted = execute_values(
            cur,
            """
            INSERT INTO c2_chunks (file_id, chunk_idx, tenant, upload_ts, e2e_seconds, tokens, embedding)
            VALUES %s
            ON CONFLICT (file_id, chunk_idx) DO NOTHING
            RETURNING file_id, chunk_idx
            """,
            rows,
            fetch=True,
        )
    return {(r[0], r[1]) for r in inserted}
