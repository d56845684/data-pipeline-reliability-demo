"""多租戶上傳模擬器 — 8 個一般租戶以固定節奏上傳小檔（5-10 chunks），
並隨機對不同租戶注入異常（poison / duplicate / mini_burst）。

大型事故（megacorp 100 份大檔）仍由 inject.py 手動觸發供現場演示。
"""
import random
import time
import uuid

from prometheus_client import Counter, start_http_server

import common
import config

FILES_UPLOADED = Counter("c2_files_uploaded_total", "files uploaded", ["tenant"])
INJECTED = Counter("c2_injected_scenarios_total", "random injected scenarios",
                   ["scenario", "tenant"])


def make_file_message(tenant: str, n_chunks: int,
                      poison: bool = False, file_id: str | None = None) -> dict:
    return {
        "file_id": file_id or f"{tenant}-{uuid.uuid4().hex[:12]}",
        "tenant": tenant,
        "n_chunks": n_chunks,
        "upload_ts": time.time(),
        "poison": poison,
    }


def choose_scenario(rng: random.Random) -> str | None:
    roll = rng.random()
    cumulative = 0.0
    for name, prob in config.ERROR_SCENARIOS.items():
        cumulative += prob * config.ERROR_MULTIPLIER
        if roll < cumulative:
            return name
    return None


def inject_scenario(ch, scenario: str, rng: random.Random) -> None:
    """對隨機租戶觸發異常（含 megacorp 在內都可能中獎）。"""
    tenant = rng.choice(config.ALL_TENANTS)
    INJECTED.labels(scenario, tenant).inc()

    if scenario == "poison":
        common.publish(ch, config.UPLOAD_QUEUE,
                       make_file_message(tenant, rng.randint(3, 8), poison=True))
        print(f"[uploader] ☠️ (injected: poison) {tenant} 上傳了損毀檔 → 將進 DLQ", flush=True)

    elif scenario == "duplicate":
        msg = make_file_message(tenant, rng.randint(5, 10))
        common.publish(ch, config.UPLOAD_QUEUE, msg)
        common.publish(ch, config.UPLOAD_QUEUE, msg)   # 上游重送
        print(f"[uploader] ♊ (injected: duplicate) {tenant} 同檔案被投遞兩次", flush=True)

    elif scenario == "mini_burst":
        n_files = rng.randint(10, 25)
        for _ in range(n_files):
            common.publish(ch, config.UPLOAD_QUEUE,
                           make_file_message(tenant, rng.randint(20, 40)))
        print(f"[uploader] 💥 (injected: mini_burst) {tenant} 突發上傳 {n_files} 份檔案", flush=True)


def main() -> None:
    rng = random.Random(config.SEED)
    start_http_server(config.METRICS_PORT)
    conn, ch = common.connect_rabbit()
    common.declare_topology(ch)
    print(f"[uploader] 啟動：{len(config.TENANTS)} 個一般租戶，"
          f"每 {config.UPLOAD_INTERVAL_SECONDS}s 一次上傳，"
          f"隨機異常注入 ×{config.ERROR_MULTIPLIER}", flush=True)

    while True:
        tenant = rng.choice(config.TENANTS)
        common.publish(ch, config.UPLOAD_QUEUE, make_file_message(tenant, rng.randint(5, 10)))
        FILES_UPLOADED.labels(tenant).inc()

        scenario = choose_scenario(rng)
        if scenario:
            inject_scenario(ch, scenario, rng)

        conn.process_data_events(time_limit=config.UPLOAD_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
