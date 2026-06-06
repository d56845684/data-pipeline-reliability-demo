# Case 1 — 批次 ETL 資料品質與延遲監控體系

模擬保險數據湖場景：8 條每日批次管道由 Airflow 排程，producer 以固定節奏落檔並隨機注入
8 種典型異常，ETL 框架以兩級品質檢核攔截，指標全鏈路可觀測、告警分級路由。

## 架構圖

```mermaid
flowchart LR
    subgraph PRODUCER["上游模擬（producer）"]
        P["producer_service<br/>每 15s = 1 營業日<br/>暖機 14 天建基線"]
        EI["error_injector<br/>8 種隨機異常<br/>row_drop / null_spike / schema_drift /<br/>duplicate_pk / encryption_leak /<br/>late_arrival / corrupt_file / volume_surge"]
        P --- EI
    end

    subgraph LANDING["落地區（shared volume）"]
        L["/data/landing/&lt;pipeline&gt;/&lt;date&gt;.csv<br/>tmp + atomic rename"]
    end

    subgraph AIRFLOW["Airflow 排程層（DAG 工廠 ×8 管道，每分鐘）"]
        SCAN["scan_new_files<br/>新檔 + 待回補"]
        PROC["process_file<br/>dynamic task mapping<br/>每檔獨立可重試"]
        MISS["detect_missing<br/>落後 frontier 即告警"]
        SENT["mark_run_failed<br/>one_failed 哨兵"]
        SCAN --> PROC --> MISS
        PROC --> SENT
    end

    subgraph ETL["ETL 框架層（case1/*.py）"]
        QC["quality_checks<br/>Blocking：schema / 主鍵重複 / 暗碼格式<br/>Warning：空值率 / 筆數 z-score / 時長劣化"]
        DB[("PostgreSQL warehouse<br/>冪等 upsert<br/>ON CONFLICT DO UPDATE")]
        QC -->|"通過載入<br/>blocking 失敗則中斷"| DB
    end

    subgraph OBS["可觀測性"]
        PG2["Pushgateway"]
        PROM["Prometheus<br/>13 條告警規則"]
        AM["Alertmanager<br/>severity 路由"]
        AL["alert-logger"]
        LINE["LINE 推播<br/>（僅 critical）"]
        GRAF["Grafana<br/>ETL 品質 + DB 健康儀表板"]
        PGEXP["postgres-exporter"]
        PG2 --> PROM --> AM --> AL --> LINE
        PROM --> GRAF
        PGEXP --> PROM
    end

    P -->|"atomic CSV"| L
    L -->|"每分鐘掃描"| SCAN
    PROC --> QC
    QC -->|"push metrics"| PG2
    DB -.-> PGEXP
    DB -->|"執行歷史 → 動態基線"| QC
```

## 監控訊號 → 告警對應

```mermaid
flowchart TB
    subgraph signals["偵測層（每次 ETL 執行上報）"]
        S1["etl_job_status<br/>0=OK 1=Warn 2=Fail 3=Missing"]
        S2["etl_rowcount_zscore<br/>同期基線 ±3σ"]
        S3["etl_duration_ratio<br/>vs 過往 N 次平均 +10%"]
        S4["etl_null_ratio / etl_check_failed"]
        S5["etl_freshness_days /<br/>etl_last_success_timestamp"]
    end
    subgraph alerts["告警（嚴重度路由）"]
        C["🔴 critical → LINE/PagerDuty<br/>ETLJobFailed / BlockingCheckFailed /<br/>PipelineStale / PostgresDown / PGDeadlocks"]
        W["🟡 warning → console/Grafana<br/>RowCountAnomaly / DurationDegraded /<br/>NullRatioHigh / FileMissing / FreshnessLag"]
    end
    S1 & S4 --> C
    S2 & S3 & S5 --> W
```

## 核心設計

| 設計 | 實作 | 解決的問題 |
|------|------|-----------|
| 框架層 metrics 注入 | `metrics.push_run_metrics()`，所有 DAG 共用 | 新管道零成本接入監控 |
| Pushgateway push 模式 | 批次 job 結束時推送 | 短生命週期任務不適合 pull |
| 動態基線（同 weekday 類型 ±3σ） | `db.get_same_daytype_history()` | 抓「靜默掉量」這種無聲異常 |
| 時長劣化偵測（+10%） | `check_duration_baseline()` | 抓「沒失敗但變慢」 |
| 兩級檢核 | blocking 中斷載入 / warning 照常載入 | 防污染下游 vs 避免過度阻斷 |
| 冪等載入 | `ON CONFLICT DO UPDATE` | 重跑/回補不產生重複 |
| 缺檔偵測 + 自動回補 | frontier 比對 + MISSING 狀態覆寫 | 遲到檔案到位即自動補跑 |

## 快速操作

```bash
docker compose exec producer python inject.py encryption_leak policies   # 注入事故
docker compose logs -f producer                                          # 看注入記錄
# Airflow UI: http://localhost:18080（dataops/dataops）
# Grafana:    http://localhost:3000/d/case1-etl ・ /d/case1-db
```
