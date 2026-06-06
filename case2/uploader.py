"""多租戶上傳模擬器 — 8 個一般租戶以固定節奏上傳小檔（5-10 chunks）。

事故注入（burst/poison/duplicate）由 inject.py 直接發佈，與本服務解耦。
"""
import random
import time
import uuid

from prometheus_client import Counter, start_http_server

import common
import config

FILES_UPLOADED = Counter("c2_files_uploaded_total", "files uploaded", ["tenant"])


def make_file_message(tenant: str, n_chunks: int, rng: random.Random,
                      poison: bool = False, file_id: str | None = None) -> dict:
    return {
        "file_id": file_id or f"{tenant}-{uuid.uuid4().hex[:12]}",
        "tenant": tenant,
        "n_chunks": n_chunks,
        "upload_ts": time.time(),
        "poison": poison,
    }


def main() -> None:
    rng = random.Random(config.SEED)
    start_http_server(config.METRICS_PORT)
    conn, ch = common.connect_rabbit()
    common.declare_topology(ch)
    print(f"[uploader] 啟動：{len(config.TENANTS)} 個一般租戶，"
          f"每 {config.UPLOAD_INTERVAL_SECONDS}s 一次上傳", flush=True)

    while True:
        tenant = rng.choice(config.TENANTS)
        msg = make_file_message(tenant, rng.randint(5, 10), rng)
        common.publish(ch, config.UPLOAD_QUEUE, msg)
        FILES_UPLOADED.labels(tenant).inc()
        conn.process_data_events(time_limit=config.UPLOAD_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
