"""DAG 工廠 — 依 config.PIPELINES 動態生成每條管道的 ETL DAG。

對應 STAR 設計「所有 ETL DAG 由同一個 DAG 工廠生成，框架層自動具備監控」：
- scan_new_files：掃描落地區，回傳未處理/待回補的營業日（遲到檔案自動回補）
- process_file：dynamic task mapping，每個檔案一個 task instance（可見、可重試）
  blocking 檢核失敗會讓 task 轉紅（retry 因冪等設計可安全重跑）
- detect_missing：缺檔偵測（落後全局 frontier 且無檔案 → MISSING + 告警）

排程可靠性設計：retry + backoff、max_active_runs=1 防重疊、所有指標仍經
框架層（etl_job → metrics）上報 Pushgateway，Airflow 只負責編排。
"""
import sys
from datetime import date, timedelta
from pathlib import Path

import pendulum

sys.path.insert(0, "/opt/case1")

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException

import config  # noqa: E402  (from /opt/case1)

DEFAULT_ARGS = {
    "owner": "dataops",
    "retries": 1,
    "retry_delay": timedelta(seconds=20),
}


def _connect():
    import db
    conn = db.connect(retries=3, delay=2.0)
    db.init_schema(conn)
    return conn


def _global_frontier() -> date | None:
    """全管道落地檔的最新營業日（缺檔偵測的基準線）。"""
    frontier = None
    for pipeline in config.PIPELINES:
        pipeline_dir = Path(config.DATA_DIR) / pipeline
        if not pipeline_dir.exists():
            continue
        for f in pipeline_dir.glob("*.csv"):
            try:
                d = date.fromisoformat(f.stem)
            except ValueError:
                continue
            frontier = d if frontier is None else max(frontier, d)
    return frontier


def build_dag(pipeline: str):
    @dag(
        dag_id=f"etl_{pipeline}",
        description=f"批次 ETL：{pipeline}（品質檢核 + 冪等載入 + 指標上報）",
        schedule=timedelta(minutes=1),
        start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
        catchup=False,
        max_active_runs=1,          # 防止排程重疊重複處理
        default_args=DEFAULT_ARGS,
        tags=["dataops", "case1", config.PIPELINES[pipeline]["criticality"]],
    )
    def etl_pipeline_dag():

        @task
        def scan_new_files() -> list[str]:
            """未處理的新檔 + 先前 MISSING 但檔案已到的回補對象。"""
            from metrics import STATUS_MISSING
            conn = _connect()
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT business_date, status FROM etl_run_stats WHERE pipeline = %s",
                    (pipeline,),
                )
                runs = {d: s for d, s in cur.fetchall()}

            todo = []
            pipeline_dir = Path(config.DATA_DIR) / pipeline
            for f in sorted(pipeline_dir.glob("*.csv")) if pipeline_dir.exists() else []:
                try:
                    business_date = date.fromisoformat(f.stem)
                except ValueError:
                    continue
                status = runs.get(business_date)
                if status is None or status == STATUS_MISSING:
                    todo.append(business_date.isoformat())
            return todo

        @task
        def process_file(business_date: str) -> None:
            """單一營業日的 ETL。blocking 失敗 → task 失敗（Airflow UI 轉紅）。"""
            import etl_job
            from metrics import STATUS_FAILED
            conn = _connect()
            path = Path(config.DATA_DIR) / pipeline / f"{business_date}.csv"
            status = etl_job.run(conn, pipeline, date.fromisoformat(business_date), path)
            if status == STATUS_FAILED:
                raise AirflowException(
                    f"{pipeline} {business_date}: blocking 檢核失敗或檔案損毀，"
                    "已中斷載入（詳見 etl_run_stats.checks 與 Grafana）")

        @task(trigger_rule="all_done")
        def detect_missing() -> None:
            """落後全局 frontier 且無檔案、無執行記錄 → 記為 MISSING 觸發告警。"""
            import etl_job
            frontier = _global_frontier()
            if frontier is None:
                return
            conn = _connect()
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT business_date FROM etl_run_stats WHERE pipeline = %s",
                    (pipeline,),
                )
                recorded = {d for (d,) in cur.fetchall()}
            pipeline_dir = Path(config.DATA_DIR) / pipeline
            on_disk = set()
            for f in pipeline_dir.glob("*.csv") if pipeline_dir.exists() else []:
                try:
                    on_disk.add(date.fromisoformat(f.stem))
                except ValueError:
                    continue
            check_from = min(on_disk) if on_disk else frontier
            d = check_from
            while d < frontier:
                if d not in on_disk and d not in recorded:
                    etl_job.run(conn, pipeline, d, None)
                d += timedelta(days=1)

        process_file.expand(business_date=scan_new_files()) >> detect_missing()

    return etl_pipeline_dag()


for _pipeline in config.PIPELINES:
    globals()[f"dag_etl_{_pipeline}"] = build_dag(_pipeline)
