"""Prometheus 指標上報層 — 批次 job 為短生命週期任務，採 Pushgateway push 模式。

對應 STAR 文件 Case 1 的「框架層注入、零侵入接入」：所有 ETL job 走同一個
push_run_metrics()，新管道天生具備可觀測性。
"""
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

import config

# job status 數值定義（Grafana value mapping 同步使用）
STATUS_OK = 0
STATUS_WARNING = 1
STATUS_FAILED = 2
STATUS_MISSING = 3


def push_run_metrics(
    pipeline: str,
    criticality: str,
    status: int,
    rows_source: int,
    rows_loaded: int,
    duration_s: float,
    freshness_days: float,
    zscore: float,
    null_ratios: dict[str, float],
    check_results: list,           # list[CheckResult]
    last_success_epoch: float | None,
) -> None:
    registry = CollectorRegistry()

    Gauge("etl_job_status",
          "0=ok 1=warning 2=failed 3=missing",
          ["criticality"], registry=registry).labels(criticality).set(status)

    rows_g = Gauge("etl_rows", "rows by stage", ["stage"], registry=registry)
    rows_g.labels("source").set(rows_source)
    rows_g.labels("loaded").set(rows_loaded)

    Gauge("etl_duration_seconds", "job duration", registry=registry).set(duration_s)
    Gauge("etl_freshness_days", "business date lag of newest event", registry=registry).set(freshness_days)
    Gauge("etl_rowcount_zscore", "row count z-score vs same-daytype baseline", registry=registry).set(zscore)

    null_g = Gauge("etl_null_ratio", "null ratio of key columns", ["column"], registry=registry)
    for column, ratio in null_ratios.items():
        null_g.labels(column).set(ratio)

    check_g = Gauge("etl_check_failed", "1 = check failed", ["check", "severity"], registry=registry)
    for c in check_results:
        check_g.labels(c.name, c.severity).set(0 if c.passed else 1)

    if last_success_epoch is not None:
        Gauge("etl_last_success_timestamp", "epoch of last successful run",
              registry=registry).set(last_success_epoch)

    try:
        push_to_gateway(
            config.PUSHGATEWAY_URL,
            job="case1_etl",
            grouping_key={"pipeline": pipeline},
            registry=registry,
        )
    except Exception as e:  # pushgateway 短暫不可用不應中斷 ETL 本體
        print(f"[metrics] push failed for {pipeline}: {e}", flush=True)
