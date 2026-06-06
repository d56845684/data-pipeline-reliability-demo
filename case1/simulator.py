"""模擬器主迴圈 — 每 TICK_SECONDS 模擬一個營業日：producer 落檔 → ETL 處理 → 指標上報。

- 開場先快速跑 WARMUP_DAYS 天的乾淨資料（無錯誤、不 sleep），讓動態基線立即可用
- late_arrival 的檔案由 pending 佇列在到期日落地，當日 ETL 會偵測到檔案缺失
- 支援現場演示：inject.py 寫入 force_error.json，下一個 tick 對指定管道強制注入
"""
import json
import os
import random
import time
from datetime import date, timedelta
from pathlib import Path

import config
import db
import etl_job
import producer


def read_force_request() -> dict | None:
    """讀取並消費 inject.py 留下的強制注入請求。"""
    force_file = Path(config.FORCE_FILE)
    if not force_file.exists():
        return None
    try:
        request = json.loads(force_file.read_text())
    except json.JSONDecodeError:
        request = None
    force_file.unlink(missing_ok=True)
    return request


def simulate_day(conn, business_date: date, rng: random.Random,
                 pending: dict, error_multiplier: float, force: dict | None) -> None:
    for pipeline in config.PIPELINES:
        force_scenario = None
        if force and force.get("pipeline") in (pipeline, "*"):
            force_scenario = force.get("scenario")

        result = producer.produce(
            pipeline, business_date, rng,
            force_error=force_scenario, error_multiplier=error_multiplier,
        )

        # late_arrival：把檔案材料掛進 pending，到期日才落地
        if result["delay_days"] > 0:
            deliver_on = business_date + timedelta(days=result["delay_days"])
            pending.setdefault(pipeline, []).append(
                (deliver_on, business_date, result["_pending"]))

        # 先落地所有到期的遲到檔案，並立即回補（模擬 backfill）
        due = [(d, bd, mat) for (d, bd, mat) in pending.get(pipeline, []) if d <= business_date]
        pending[pipeline] = [(d, bd, mat) for (d, bd, mat) in pending.get(pipeline, [])
                             if d > business_date]
        for _, original_date, (rows, header, file_mode) in due:
            path = producer.landing_path(pipeline, original_date)
            producer.write_file(path, rows, header, file_mode)
            print(f"[simulator] 遲到檔案落地，回補 {pipeline} {original_date}", flush=True)
            etl_job.run(conn, pipeline, original_date, path, scenario="late_arrival")

        # 處理當日檔案（late_arrival 當日檔案不存在 -> MISSING）
        path = result["path"] if result["delay_days"] == 0 else None
        scenario = result["scenario"] if result["delay_days"] == 0 else "late_arrival"
        etl_job.run(conn, pipeline, business_date, path, scenario=scenario)


def main() -> None:
    print(f"[simulator] 啟動：{len(config.PIPELINES)} 條管道，"
          f"tick={config.TICK_SECONDS}s/日，warmup={config.WARMUP_DAYS} 天", flush=True)
    rng = random.Random(config.SEED)
    conn = db.connect()
    db.init_schema(conn)

    business_date = date.fromisoformat(config.START_DATE)
    pending: dict = {}

    # 暖機：快速建立動態基線（無錯誤注入）
    for _ in range(config.WARMUP_DAYS):
        simulate_day(conn, business_date, rng, pending, error_multiplier=0.0, force=None)
        business_date += timedelta(days=1)
    print(f"[simulator] 暖機完成，基線就緒。開始即時模擬（含隨機錯誤注入）", flush=True)

    while True:
        force = read_force_request()
        if force:
            print(f"[simulator] 強制注入：{force}", flush=True)
        simulate_day(conn, business_date, rng, pending,
                     error_multiplier=config.ERROR_MULTIPLIER, force=force)
        business_date += timedelta(days=1)
        time.sleep(config.TICK_SECONDS)


if __name__ == "__main__":
    main()
