"""前處理 worker — 消費上傳佇列，把檔案切割成 chunk 任務發佈到向量化佇列。

檔案 → chunk 的放大效應就發生在這層：一份大檔展開成 40-80 個 chunk 任務。
發佈目標由 PER_TENANT_QUEUES 決定（共用佇列 vs 租戶隔離佇列）。
"""
import json
import random
import time

from prometheus_client import Counter, start_http_server

import common
import config

FILES_SPLIT = Counter("c2_files_split_total", "files split", ["tenant"])
CHUNKS_EMITTED = Counter("c2_chunks_emitted_total", "chunk tasks emitted", ["tenant"])

_WORDS = ("contract clause liability premium policy claim settlement coverage "
          "renewal endorsement deductible underwriting actuarial reinsurance "
          "compliance audit risk exposure portfolio valuation amortization").split()
_rng = random.Random()


def make_chunk_text(file_id: str, idx: int) -> str:
    """產生該 chunk 的文字內容（200-600 字元，模擬文件切割後的段落）。"""
    _rng.seed(f"{file_id}#{idx}")
    n_words = _rng.randint(30, 90)
    return " ".join(_rng.choice(_WORDS) for _ in range(n_words))


def on_message(ch, method, _props, body):
    msg = json.loads(body)
    tenant = msg["tenant"]
    n_chunks = msg["n_chunks"]

    # 模擬切割/清理成本
    time.sleep(0.05 + config.PREPROCESS_MS / 1000 * n_chunks)

    target_queue = config.vector_queue_for(tenant)
    for idx in range(n_chunks):
        common.publish(ch, target_queue, {
            "file_id": msg["file_id"],
            "chunk_idx": idx,
            "tenant": tenant,
            "upload_ts": msg["upload_ts"],
            "text": make_chunk_text(msg["file_id"], idx),   # 供 embedding 的實際內容
            # 毒檔案：讓其中一個 chunk 帶毒，模擬向量化階段才爆的損毀內容
            "poison": msg.get("poison", False) and idx == 0,
        })
    FILES_SPLIT.labels(tenant).inc()
    CHUNKS_EMITTED.labels(tenant).inc(n_chunks)
    ch.basic_ack(method.delivery_tag)


def main() -> None:
    start_http_server(config.METRICS_PORT)
    conn, ch = common.connect_rabbit()
    common.declare_topology(ch)
    ch.basic_qos(prefetch_count=8)
    ch.basic_consume(queue=config.UPLOAD_QUEUE, on_message_callback=on_message)
    mode = "租戶隔離佇列" if config.PER_TENANT_QUEUES else "共用單一佇列（事故架構）"
    print(f"[preprocess] 啟動，發佈模式：{mode}", flush=True)
    ch.start_consuming()


if __name__ == "__main__":
    main()
