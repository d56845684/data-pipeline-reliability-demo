"""ETL 常駐服務 — 掃描落地區，自動處理新到檔案，並偵測缺檔。

與 producer 完全解耦（透過落地區檔案介面），更貼近真實批次架構：
- 每 ETL_SCAN_SECONDS 秒掃描一次落地區
- 新檔案（無執行記錄）→ 執行 ETL
- 遲到檔案（執行記錄為 MISSING 但檔案出現了）→ 自動回補（backfill）
- 缺檔偵測：落後於全局最新營業日（frontier）且無檔案 → 記為 MISSING 並告警
"""
import time
from datetime import date
from pathlib import Path

import config
import db
import etl_job
from metrics import STATUS_MISSING


def scan_landing() -> dict[str, dict[date, Path]]:
    """回傳 {pipeline: {business_date: path}}。"""
    files: dict[str, dict[date, Path]] = {}
    for pipeline in config.PIPELINES:
        pipeline_dir = Path(config.DATA_DIR) / pipeline
        files[pipeline] = {}
        if not pipeline_dir.exists():
            continue
        for f in pipeline_dir.glob("*.csv"):
            try:
                files[pipeline][date.fromisoformat(f.stem)] = f
            except ValueError:
                continue
    return files


def load_run_index(conn) -> dict[tuple[str, date], int]:
    """既有執行記錄 {(pipeline, business_date): status}。"""
    with conn.cursor() as cur:
        cur.execute("SELECT pipeline, business_date, status FROM etl_run_stats")
        return {(p, d): s for p, d, s in cur.fetchall()}


def scan_and_process(conn) -> int:
    files = scan_landing()
    all_dates = {d for per_pipeline in files.values() for d in per_pipeline}
    if not all_dates:
        return 0
    frontier = max(all_dates)
    runs = load_run_index(conn)
    processed = 0

    for pipeline in config.PIPELINES:
        # 1) 處理新檔案；遲到檔案（先前記為 MISSING）自動回補
        for business_date in sorted(files[pipeline]):
            status = runs.get((pipeline, business_date))
            if status is None or status == STATUS_MISSING:
                if status == STATUS_MISSING:
                    print(f"[etl] 遲到檔案出現，回補 {pipeline} {business_date}", flush=True)
                etl_job.run(conn, pipeline, business_date, files[pipeline][business_date])
                processed += 1

        # 2) 缺檔偵測：嚴格落後 frontier 的日期沒有檔案也沒有記錄 -> MISSING
        for business_date in sorted(d for d in all_dates if d < frontier):
            if business_date not in files[pipeline] and (pipeline, business_date) not in runs:
                etl_job.run(conn, pipeline, business_date, None)
                processed += 1

    return processed


def main() -> None:
    print(f"[etl] 啟動：每 {config.ETL_SCAN_SECONDS}s 掃描落地區 {config.DATA_DIR}", flush=True)
    conn = db.connect()
    db.init_schema(conn)
    while True:
        try:
            scan_and_process(conn)
        except Exception as e:  # 單次掃描失敗不中斷服務
            print(f"[etl] scan error: {e}", flush=True)
        time.sleep(config.ETL_SCAN_SECONDS)


if __name__ == "__main__":
    main()
