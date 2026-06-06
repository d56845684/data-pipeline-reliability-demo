"""Producer 常駐服務 — 模擬上游核心系統，以固定節奏持續落檔。

- 每 PRODUCE_INTERVAL_SECONDS 秒產生一個營業日的批次檔（8 條管道）
- 與 ETL 完全解耦：只負責寫檔，不知道下游如何處理（貼近真實上游系統）
- 啟動時若落地區為空，先快速產生 WARMUP_DAYS 天乾淨資料供 ETL 建立基線；
  否則從既有檔案的最新日期接續，重啟不會重置日曆
- 支援 inject.py 強制注入錯誤情境（現場演示用）
"""
import json
import random
import time
from datetime import date, timedelta
from pathlib import Path

import config
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


def find_resume_date() -> date | None:
    """從落地區既有檔案找出最新營業日，重啟後接續產生。"""
    latest = None
    for pipeline in config.PIPELINES:
        pipeline_dir = Path(config.DATA_DIR) / pipeline
        if not pipeline_dir.exists():
            continue
        for f in pipeline_dir.glob("*.csv"):
            try:
                business_date = date.fromisoformat(f.stem)
            except ValueError:
                continue
            latest = business_date if latest is None else max(latest, business_date)
    return latest


def produce_day(business_date: date, rng: random.Random, pending: dict,
                error_multiplier: float, force: dict | None) -> None:
    for pipeline in config.PIPELINES:
        force_scenario = None
        if force and force.get("pipeline") in (pipeline, "*"):
            force_scenario = force.get("scenario")

        result = producer.produce(
            pipeline, business_date, rng,
            force_error=force_scenario, error_multiplier=error_multiplier,
        )

        if result["delay_days"] > 0:
            deliver_on = business_date + timedelta(days=result["delay_days"])
            pending.setdefault(pipeline, []).append(
                (deliver_on, business_date, result["_pending"]))
            print(f"[producer] {business_date} {pipeline:<12} late_arrival，"
                  f"檔案將於 {deliver_on} 落地", flush=True)
        elif result["scenario"]:
            print(f"[producer] {business_date} {pipeline:<12} "
                  f"rows={result['rows']:>5} (injected: {result['scenario']})", flush=True)

        # 落地所有到期的遲到檔案
        due = [(d, bd, mat) for (d, bd, mat) in pending.get(pipeline, []) if d <= business_date]
        pending[pipeline] = [(d, bd, mat) for (d, bd, mat) in pending.get(pipeline, [])
                             if d > business_date]
        for _, original_date, (rows, header, file_mode) in due:
            path = producer.landing_path(pipeline, original_date)
            producer.write_file(path, rows, header, file_mode)
            print(f"[producer] 遲到檔案落地：{pipeline} {original_date}", flush=True)


def main() -> None:
    rng = random.Random(config.SEED)
    pending: dict = {}

    resume_from = find_resume_date()
    if resume_from is None:
        business_date = date.fromisoformat(config.START_DATE)
        print(f"[producer] 啟動：{len(config.PIPELINES)} 條管道，"
              f"interval={config.PRODUCE_INTERVAL_SECONDS}s/日，"
              f"先暖機 {config.WARMUP_DAYS} 天乾淨資料", flush=True)
        for _ in range(config.WARMUP_DAYS):
            produce_day(business_date, rng, pending, error_multiplier=0.0, force=None)
            business_date += timedelta(days=1)
        print("[producer] 暖機資料就緒，開始固定節奏產生（含隨機錯誤注入）", flush=True)
    else:
        business_date = resume_from + timedelta(days=1)
        print(f"[producer] 接續既有日曆，從 {business_date} 開始", flush=True)

    while True:
        force = read_force_request()
        if force:
            print(f"[producer] 強制注入：{force}", flush=True)
        produce_day(business_date, rng, pending,
                    error_multiplier=config.ERROR_MULTIPLIER, force=force)
        print(f"[producer] {business_date} 批次落檔完成", flush=True)
        business_date += timedelta(days=1)
        time.sleep(config.PRODUCE_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
