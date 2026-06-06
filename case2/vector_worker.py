"""向量化 worker — 消費 chunk 任務，經模擬 LLM embedding 推論後冪等寫入 PostgreSQL。

處理流程（一次 flush = 一個批次的完整生命週期）：
    buffer 累積 chunk → 批次 embedding 推論（耗時 = base + token 數，偶發 timeout 自動重試）
    → 批次冪等寫入 PG（含向量）→ 逐筆 ack

可靠性設計（對應 STAR Case 2 的修復項）：
- 批次推論 + 批次寫入：BATCH_WRITES=true 時 16 chunk/批，攤平推論固定 overhead 與
  DB RPC 成本（約 3 倍吞吐）；false 時退化為逐 chunk 處理（事故前架構）
- 冪等寫入：PK (file_id, chunk_idx) + ON CONFLICT DO NOTHING，重送/重放不重複入庫
- DLQ：毒訊息 basic_nack(requeue=False) → dead-letter exchange → c2.vector.dlq
- 公平消化：PER_TENANT_QUEUES=true 時同時消費所有租戶佇列、prefetch 小，
  單一租戶的積壓不會阻塞其他租戶
- e2e 延遲（上傳→入庫）按租戶上報 histogram，供 P95 監控與告警
"""
import hashlib
import json
import random
import time

from prometheus_client import Counter, Histogram, start_http_server

import common
import config
import embedding

CHUNKS = Counter("c2_chunks_processed_total", "chunks embedded+stored", ["tenant"])
DUPES = Counter("c2_duplicates_skipped_total", "idempotent skips", ["tenant"])
POISON = Counter("c2_poison_dlq_total", "poison messages dead-lettered", ["tenant"])
UNEXPECTED = Counter("c2_unexpected_failures_total",
                     "unexpected processing failures dead-lettered", ["tenant", "kind"])
E2E = Histogram("c2_chunk_e2e_seconds", "upload -> stored e2e latency", ["tenant"],
                buckets=(1, 2, 5, 10, 20, 40, 60, 120, 240, 480, 900))
EMBED_SECONDS = Histogram("c2_embed_seconds", "embedding inference duration per batch",
                          buckets=(.02, .05, .1, .2, .4, .8, 1.6))
EMBED_TOKENS = Counter("c2_embed_tokens_total", "tokens embedded")
EMBED_RETRIES = Counter("c2_embed_retries_total", "embedding timeout retries")
FLUSH_SECONDS = Histogram("c2_db_flush_seconds", "db flush duration",
                          buckets=(.005, .01, .025, .05, .1, .25, .5, 1))
BATCH_SIZE = Histogram("c2_db_batch_rows", "rows per db flush",
                       buckets=(1, 5, 10, 25, 50, 100))

FLUSH_MAX_ROWS = config.EMBED_BATCH_SIZE if config.BATCH_WRITES else 1
FLUSH_MAX_AGE = 0.5

_rng = random.Random()
_buffer: list[tuple[int, dict]] = []   # (delivery_tag, chunk message)
_last_flush = time.monotonic()


def flush(ch, pg) -> None:
    global _buffer, _last_flush
    _last_flush = time.monotonic()
    if not _buffer:
        return
    batch, _buffer = _buffer, []

    try:
        _process_batch(ch, pg, batch)
    except Exception as e:
        # flush 失敗（如 DB 暫時不可用）→ 整批 nack requeue，等待重投遞（冪等保證不重複）
        print(f"[vector] flush 失敗，整批 requeue：{e}", flush=True)
        for tag, _ in batch:
            ch.basic_nack(tag, requeue=True)


def _process_batch(ch, pg, batch) -> None:
    # 1) 批次 embedding 推論（模擬 vLLM 批次呼叫）
    texts = [msg["text"] for _, msg in batch]
    t0 = time.monotonic()
    vectors, tokens, retries = embedding.embed_batch(texts, _rng)
    EMBED_SECONDS.observe(time.monotonic() - t0)
    EMBED_TOKENS.inc(tokens)
    if retries:
        EMBED_RETRIES.inc(retries)
        print(f"[vector] embedding timeout，重試 {retries} 次後成功", flush=True)

    # 2) 批次冪等寫入（含向量）
    now = time.time()
    rows = [
        (msg["file_id"], msg["chunk_idx"], msg["tenant"], msg["upload_ts"],
         now - msg["upload_ts"], embedding.estimate_tokens(msg["text"]), vec)
        for (_, msg), vec in zip(batch, vectors)
    ]
    t0 = time.monotonic()
    inserted_keys = common.insert_chunks(pg, rows)
    FLUSH_SECONDS.observe(time.monotonic() - t0)
    BATCH_SIZE.observe(len(rows))

    # 3) 落庫後才 ack（at-least-once + 冪等 = 不丟不重）
    for tag, msg in batch:
        if (msg["file_id"], msg["chunk_idx"]) in inserted_keys:
            CHUNKS.labels(msg["tenant"]).inc()
            E2E.labels(msg["tenant"]).observe(now - msg["upload_ts"])
        else:
            DUPES.labels(msg["tenant"]).inc()
        ch.basic_ack(tag)


REQUIRED_FIELDS = ("file_id", "chunk_idx", "tenant", "upload_ts", "text")


def _is_sticky_failure(file_id: str, chunk_idx: int) -> bool:
    """特定輸入觸發的未知 bug：由內容 hash 決定 → 同一則訊息每次處理都失敗。

    模擬「重放也救不回來」的殘餘案例（如某種邊界字元讓解析器崩潰），
    哨兵重放額度耗盡後會留在 DLQ 等人工 debug。
    """
    digest = hashlib.md5(f"{file_id}#{chunk_idx}".encode()).digest()
    return int.from_bytes(digest[:2], "big") / 65535.0 < config.STICKY_FAIL_PROB


def make_handler(pg):
    def on_message(ch, method, _props, body):
        try:
            msg = json.loads(body)
        except json.JSONDecodeError:
            msg = {}
        missing = [f for f in REQUIRED_FIELDS if f not in msg]

        # 毒訊息或 schema 不相容（如部署前殘留的舊版訊息）→ DLQ，絕不 crash worker
        if msg.get("poison") or missing:
            tenant = msg.get("tenant", "unknown")
            POISON.labels(tenant).inc()
            ch.basic_nack(method.delivery_tag, requeue=False)   # → DLX → DLQ
            reason = "poison" if msg.get("poison") else f"missing fields {missing}"
            print(f"[vector] ☠️ {reason}: "
                  f"{msg.get('file_id', '?')}#{msg.get('chunk_idx', '?')} → DLQ", flush=True)
            return

        # 預期外的隨機失敗 —— 進 DLQ 交給哨兵分流
        if _rng.random() < config.TRANSIENT_FAIL_PROB:
            # 暫時性故障（embedding 服務 5xx / OOM）：重放即成功
            UNEXPECTED.labels(msg["tenant"], "transient").inc()
            ch.basic_nack(method.delivery_tag, requeue=False)
            print(f"[vector] 💥 預期外暫時性失敗 {msg['file_id']}#{msg['chunk_idx']} "
                  "→ DLQ（哨兵將自動重放）", flush=True)
            return
        if _is_sticky_failure(msg["file_id"], msg["chunk_idx"]):
            # 特定輸入觸發的未知 bug：每次重放都會再失敗
            UNEXPECTED.labels(msg["tenant"], "sticky").inc()
            ch.basic_nack(method.delivery_tag, requeue=False)
            print(f"[vector] 🐛 未知 bug（特定輸入觸發）{msg['file_id']}#{msg['chunk_idx']} "
                  "→ DLQ（重放仍會失敗，額度耗盡後留人工）", flush=True)
            return

        _buffer.append((method.delivery_tag, msg))
        if len(_buffer) >= FLUSH_MAX_ROWS:
            flush(ch, pg)
    return on_message


def main() -> None:
    start_http_server(config.METRICS_PORT)
    pg = common.connect_pg()
    conn, ch = common.connect_rabbit()
    common.declare_topology(ch)

    queues = config.vector_queues()
    # 隔離模式 prefetch 小（公平輪詢）；共用模式 prefetch 大（批次效率）
    ch.basic_qos(prefetch_count=4 if config.PER_TENANT_QUEUES else 100)
    handler = make_handler(pg)
    for q in queues:
        ch.basic_consume(queue=q, on_message_callback=handler)

    mode = f"租戶隔離 ×{len(queues)} 佇列" if config.PER_TENANT_QUEUES else "共用單一佇列（事故架構）"
    print(f"[vector] 啟動：{mode}，embedding/DB 批次大小：{FLUSH_MAX_ROWS}，"
          f"向量維度：{config.EMBED_DIM}", flush=True)

    ch_ref, pg_ref = ch, pg
    while True:
        conn.process_data_events(time_limit=0.2)
        if time.monotonic() - _last_flush >= FLUSH_MAX_AGE:
            flush(ch_ref, pg_ref)


if __name__ == "__main__":
    main()
