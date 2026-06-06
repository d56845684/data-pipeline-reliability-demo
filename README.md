# Data Pipeline Reliability — Demo

兩個 production-grade 資料管道可靠性的實作案例，以可實際運行的 demo 重現核心設計：
批次 ETL 品質監控體系，與多租戶佇列檔案處理（規劃中）。

## Case 1 — 批次 ETL 資料品質與延遲監控體系

模擬保險數據湖場景：8 條每日批次管道（保單/理賠/收付…），**data producer 隨機注入
七種典型異常**，ETL 框架以兩級品質檢核攔截，指標經 Pushgateway 進 Prometheus，
Grafana 視覺化 + Alertmanager 告警分級路由。

```
┌──────────┐  CSV   ┌─────────┐ quality checks ┌────────────┐
│ producer  │ ─────► │ ETL job │ ─────────────► │ PostgreSQL │ (冪等 upsert)
│ (錯誤注入) │ 落地檔  │         │                │ (warehouse) │
└──────────┘        └────┬────┘                └────────────┘
                          │ push metrics
                          ▼
                   ┌─────────────┐    ┌────────────┐    ┌─────────┐
                   │ Pushgateway │ ─► │ Prometheus │ ─► │ Grafana │
                   └─────────────┘    │ (告警規則)   │    └─────────┘
                                      └─────┬──────┘
                                            ▼
                                     ┌──────────────┐   ┌──────────────┐
                                     │ Alertmanager │ ─►│ alert-logger │ (模擬 Slack/PagerDuty)
                                     └──────────────┘   └──────────────┘
```

### 快速開始

```bash
git clone https://github.com/d56845684/data-pipeline-reliability-demo.git
cd data-pipeline-reliability-demo
docker compose up -d --build
```

啟動後模擬器先快速跑 14 個「乾淨營業日」建立動態基線，之後每 **15 秒 = 1 個營業日**，
每天每條管道有機率被隨機注入錯誤。

| 服務 | URL |
|------|-----|
| Grafana 儀表板 | http://localhost:3000/d/case1-etl |
| Prometheus（Alerts 頁籤看告警） | http://localhost:19090/alerts |
| Alertmanager | http://localhost:19093 |
| Pushgateway（原始指標） | http://localhost:19091 |

```bash
docker compose logs -f simulator      # 看每日 ETL 執行結果
docker compose logs -f alert-logger   # 看告警流（模擬 Slack/PagerDuty 通知）
```

### Producer 的錯誤注入（七種情境）

| 情境 | 模擬的真實事故 | 被哪一層抓到 |
|------|---------------|-------------|
| `row_drop` | 上游靜默少送 40–70% 資料 | Warning：同期動態基線 z-score |
| `null_spike` | 關鍵欄位空值率飆升 20–50% | Warning：空值率閾值 |
| `schema_drift` | 上游改版偷改欄位名 | **Blocking**：schema 一致性 |
| `duplicate_pk` | 上游重送導致主鍵重複 | **Blocking**：主鍵重複檢核 |
| `encryption_leak` | 暗碼欄位混入明碼（合規風險） | **Blocking**：加密格式檢核 |
| `late_arrival` | 檔案晚到 1–2 個營業日 | 檔案缺失偵測 + 自動回補 |
| `corrupt_file` | 檔案損毀無法解析 | 解析失敗 → job FAILED |

Blocking 失敗 → **中斷載入**（防止污染下游）+ critical 告警；
Warning 失敗 → 照常載入 + warning 告警追蹤。

### 現場演示：手動觸發事故

```bash
# 對 policies 管道注入「明碼洩漏」（下一個模擬日生效）
docker compose exec simulator python inject.py encryption_leak policies

# 對所有管道注入「靜默掉量」
docker compose exec simulator python inject.py row_drop "*"

# 可用情境：row_drop / null_spike / schema_drift / duplicate_pk /
#          encryption_leak / late_arrival / corrupt_file
```

注入後 15 秒內可在 Grafana 狀態矩陣看到變色、Prometheus Alerts 轉 FIRING、
alert-logger 印出通知。

### 也可單獨執行 producer / ETL（不經模擬器）

```bash
docker compose exec simulator python producer.py --pipeline claims --date 2026-03-01 --force-error duplicate_pk
docker compose exec simulator python etl_job.py  --pipeline claims --date 2026-03-01
```

### 查倉儲與執行歷史（PostgreSQL）

```bash
docker compose exec postgres psql -U etl -d warehouse \
  -c "SELECT pipeline, business_date, status, rows_loaded, zscore, scenario
      FROM etl_run_stats ORDER BY business_date DESC, pipeline LIMIT 20;"
```

### 設計對應（真實場景 ↔ demo 實作）

| 真實場景中的設計 | demo 中的實作 |
|------------------|--------------|
| 框架層 metrics 注入（零侵入） | `metrics.push_run_metrics()` 統一上報 |
| Pushgateway 解短生命週期問題 | push 模式 + `honor_labels` |
| 兩級品質檢核 | `quality_checks.py` blocking/warning |
| 動態基線（同期 ±3σ） | `db.get_same_daytype_history()` + z-score |
| 告警分級路由 | Alertmanager critical 即時 / warning 聚合 |
| 冪等回補 | `ON CONFLICT DO UPDATE` + late_arrival 自動補跑 |

### 調整參數

`docker-compose.yml` 的 simulator 環境變數：

- `TICK_SECONDS`：模擬一天的真實秒數（演示時可調 5 秒加快）
- `ERROR_MULTIPLIER`：錯誤頻率倍數（0 = 全健康；2.0 = 高頻事故）
- `SEED`：固定隨機種子，演示可重現

## Case 2 — 多租戶佇列檔案處理（規劃中）

RabbitMQ + 多租戶 worker + DLQ + 冪等處理，重現「大客戶突發上傳拖垮全體」事故與修復。

```bash
docker compose down -v   # 全部清掉重來
```
