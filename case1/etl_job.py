"""ETL job — 讀取落地檔 → 品質檢核（blocking/warning）→ 冪等載入倉儲 → 上報指標。

狀態機：
- 檔案缺失           -> STATUS_MISSING（不載入）
- 解析失敗（損毀）    -> STATUS_FAILED（不載入）
- blocking 檢核失敗  -> STATUS_FAILED（不載入，防止污染下游）
- warning 檢核失敗   -> STATUS_WARNING（照常載入，告警追蹤）
- 全部通過           -> STATUS_OK
"""
import argparse
import csv
import time
from datetime import date
from pathlib import Path

import config
import db
import metrics
import quality_checks as qc
from metrics import STATUS_FAILED, STATUS_MISSING, STATUS_OK, STATUS_WARNING


def parse_file(path: Path) -> tuple[list[str], list[dict]]:
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames or []
        rows = list(reader)
    if not header:
        raise ValueError("empty or unreadable file")
    return header, rows


def compute_freshness_days(rows: list[dict], business_date: date) -> float:
    dates = [r["event_date"] for r in rows if r.get("event_date")]
    if not dates:
        return 0.0
    newest = max(dates)
    try:
        return float((business_date - date.fromisoformat(newest)).days)
    except ValueError:
        return 0.0


def run(conn, pipeline: str, business_date: date, path: Path | None,
        scenario: str | None = None) -> int:
    """執行單一管道單一營業日的 ETL。回傳 job status。"""
    start = time.time()
    criticality = config.PIPELINES[pipeline]["criticality"]
    history = db.get_same_daytype_history(conn, pipeline, business_date)

    header: list[str] = []
    rows: list[dict] = []
    report = qc.QualityReport()
    rows_loaded = 0
    zscore = 0.0
    freshness = 0.0
    null_ratios: dict[str, float] = {}

    if path is None or not path.exists():
        status = STATUS_MISSING
        parse_error = None
    else:
        try:
            header, rows = parse_file(path)
            parse_error = None
        except Exception as e:
            parse_error = str(e)

        if parse_error:
            status = STATUS_FAILED
            report.blocking.append(qc.CheckResult(
                "file_parse", "blocking", False, 1, 0, parse_error[:120]))
        else:
            report = qc.run_checks(header, rows, history)
            zscore, _ = qc.rowcount_zscore(len(rows), history)
            freshness = compute_freshness_days(rows, business_date)
            null_ratios = {
                c.name.removeprefix("null_ratio_"): c.value
                for c in report.warning if c.name.startswith("null_ratio_")
            }
            if report.blocking_failed:
                status = STATUS_FAILED        # 不載入，防止污染下游
            else:
                rows_loaded = db.upsert_records(conn, pipeline, rows)
                status = STATUS_WARNING if report.warning_failed else STATUS_OK

    duration = time.time() - start
    db.record_run(conn, pipeline, business_date, status, rows_loaded, duration,
                  zscore, scenario,
                  [{"name": c.name, "severity": c.severity, "passed": c.passed,
                    "value": round(c.value, 4), "message": c.message}
                   for c in report.all_checks()])

    metrics.push_run_metrics(
        pipeline=pipeline,
        criticality=criticality,
        status=status,
        rows_source=len(rows),
        rows_loaded=rows_loaded,
        duration_s=duration,
        freshness_days=freshness,
        zscore=zscore,
        null_ratios=null_ratios,
        check_results=report.all_checks(),
        last_success_epoch=db.get_last_success_epoch(conn, pipeline),
    )

    status_name = {0: "OK", 1: "WARNING", 2: "FAILED", 3: "MISSING"}[status]
    failed_checks = [c.name for c in report.all_checks() if not c.passed]
    print(f"[etl] {business_date} {pipeline:<12} {status_name:<8} "
          f"rows={len(rows):>5} loaded={rows_loaded:>5} z={zscore:+.1f}"
          + (f" failed_checks={failed_checks}" if failed_checks else "")
          + (f" (injected: {scenario})" if scenario else ""),
          flush=True)
    return status


def main() -> None:
    parser = argparse.ArgumentParser(description="執行單一管道的批次 ETL")
    parser.add_argument("--pipeline", required=True, choices=list(config.PIPELINES))
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    args = parser.parse_args()

    business_date = date.fromisoformat(args.date)
    path = Path(config.DATA_DIR) / args.pipeline / f"{business_date.isoformat()}.csv"
    conn = db.connect()
    db.init_schema(conn)
    run(conn, args.pipeline, business_date, path if path.exists() else None)


if __name__ == "__main__":
    main()
