# Data Pipeline Reliability — Demo

兩個 production-grade 資料管道可靠性的實作案例，以可實際運行的 demo 重現核心設計：
批次 ETL 品質監控體系，與多租戶佇列檔案處理（規劃中）。

## Case 1 — 批次 ETL 資料品質與延遲監控體系

模擬保險數據湖場景：8 條每日批次管道（保單/理賠/收付…），**data producer 隨機注入
七種典型異常**，ETL 框架以兩級品質檢核攔截，指標經 Pushgateway 進 Prometheus，
Grafana 視覺化 + Alertmanager 告警分級路由。

producer 與 ETL **完全解耦**（透過落地區檔案介面溝通）：producer 以固定節奏
（15 秒 = 1 個營業日）持續落檔；**Apache Airflow** 作為排程層，由 DAG 工廠為
每條管道動態生成 DAG（每分鐘掃描），以 **dynamic task mapping** 讓每個檔案成為
獨立可重試的 task——處理新檔、偵測缺檔、回補遲到檔案，blocking 檢核失敗的檔案
在 Airflow UI 直接轉紅。

```
┌────────────┐ atomic CSV ┌───────────────────┐ quality checks ┌────────────┐
│  producer  │ ─────────► │ Airflow (排程層)    │ ─────────────► │ PostgreSQL │ (冪等 upsert)
│ (固定節奏    │  落地區     │ DAG 工廠×8 條管道   │                │ (warehouse) │◄─ postgres-exporter
│  +錯誤注入)  │            │ scan→process→缺檔  │                └────────────┘   (DB 健康監控)
└────────────┘            └────────┬──────────┘
                                   │ push metrics（框架層 etl_job → metrics）
                                   ▼
                            ┌─────────────┐    ┌────────────┐    ┌─────────┐
                            │ Pushgateway │ ─► │ Prometheus │ ─► │ Grafana │
                            └─────────────┘    │ (告警規則)   │    └─────────┘
                                               └─────┬──────┘
                                                     ▼
                                              ┌──────────────┐   ┌──────────────┐
                                              │ Alertmanager │ ─►│ alert-logger │ ─► LINE（critical only）
                                              └──────────────┘   └──────────────┘
```

### 快速開始

```bash
git clone https://github.com/d56845684/data-pipeline-reliability-demo.git
cd data-pipeline-reliability-demo
docker compose up -d --build
```

首次啟動時 producer 先快速產生 14 個「乾淨營業日」建立動態基線，之後以固定節奏
每 **15 秒落檔 1 個營業日**，每天每條管道有機率被隨機注入錯誤。
（重啟不會重置日曆——producer 會從落地區既有檔案的最新日期接續）

| 服務 | URL |
|------|-----|
| Grafana — ETL 品質儀表板 | http://localhost:3000/d/case1-etl |
| Grafana — DB 健康儀表板 | http://localhost:3000/d/case1-db |
| Airflow UI | http://localhost:18080（帳號 `admin`，密碼見下方指令） |
| Prometheus（Alerts 頁籤看告警） | http://localhost:19090/alerts |
| Alertmanager | http://localhost:19093 |
| Pushgateway（原始指標） | http://localhost:19091 |

```bash
# Airflow standalone 的 admin 密碼
docker compose exec airflow cat standalone_admin_password.txt
```

```bash
docker compose logs -f producer       # 看上游落檔與錯誤注入
docker compose logs -f airflow        # 看排程與 ETL 執行（或直接開 Airflow UI）
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
# 對 policies 管道注入「明碼洩漏」（下一個營業日生效）
docker compose exec producer python inject.py encryption_leak policies

# 對所有管道注入「靜默掉量」
docker compose exec producer python inject.py row_drop "*"

# 可用情境：row_drop / null_spike / schema_drift / duplicate_pk /
#          encryption_leak / late_arrival / corrupt_file
```

注入後 15 秒內可在 Grafana 狀態矩陣看到變色、Prometheus Alerts 轉 FIRING、
alert-logger 印出通知。

### 也可單次手動執行 producer / ETL

```bash
docker compose exec producer python producer.py --pipeline claims --date 2026-03-01 --force-error duplicate_pk
docker compose exec airflow  python /opt/case1/etl_job.py --pipeline claims --date 2026-03-01
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
| Airflow DAG 工廠（零侵入接入） | `airflow/dags/etl_quality_dags.py` 依 config 動態生成 8 條 DAG |
| 排程層可靠性 | retry + backoff、max_active_runs、dynamic task mapping 單檔重試 |
| 框架層 metrics 注入（零侵入） | `metrics.push_run_metrics()` 統一上報 |
| Pushgateway 解短生命週期問題 | push 模式 + `honor_labels` |
| 兩級品質檢核 | `quality_checks.py` blocking/warning |
| 動態基線（同期 ±3σ） | `db.get_same_daytype_history()` + z-score |
| 告警分級路由 | Alertmanager critical 即時 / warning 聚合 |
| 冪等回補 | `ON CONFLICT DO UPDATE` + late_arrival 自動補跑 |
| 標準化應對流程 | [`runbooks/`](runbooks/)：告警對應的值班手冊 + postmortem 模板 |

### LINE 告警推播設定（選用）

告警預設輸出至 console（`docker compose logs -f alert-logger`）。要推播到 LINE，
採 **Messaging API**（LINE Notify 已於 2025/03 終止服務）：

**1. 建立 LINE channel 並取得憑證**

- 到 [LINE Developers Console](https://developers.line.biz/console/) 建立 Provider →
  建立 **Messaging API** channel（會同時產生一個官方帳號 bot）
- `Messaging API` 頁籤 → 發行 **Channel access token (long-lived)**
- `Basic settings` 頁籤 → 複製 **Channel secret**

**2. 設定環境變數**

```bash
cp .env.example .env
# 填入 LINE_CHANNEL_ACCESS_TOKEN 與 LINE_CHANNEL_SECRET
docker compose up -d --build alert-logger
```

**3. 用 Cloudflare Tunnel 開通 webhook（取得你的 userId）**

LINE 平台需要一個公開的 HTTPS 端點才能回呼。腳本用 Cloudflare Quick Tunnel
（免帳號）把本機 alert-logger 暴露出去：

```bash
./scripts/line-webhook-url.sh
# 輸出形如 https://xxxx.trycloudflare.com/line/webhook
```

把這個 URL 貼到 LINE Console 的 **Webhook settings** → Verify → 開啟 Use webhook。

**4. 取得推播目標 ID 並完成設定**

用 LINE 掃 channel 的 QR code 加 bot 好友，傳任意訊息——bot 會直接回覆你的
**userId**。填入 `.env` 的 `LINE_TARGET_ID` 後重啟：

```bash
docker compose up -d --build alert-logger
```

**5. 測試**

```bash
docker compose exec producer python inject.py encryption_leak policies
# 約 20 秒內 LINE 收到 🔴 FIRING ETLBlockingCheckFailed 推播
```

> **推播範圍**：由 Alertmanager 路由層控制，只有 `severity=critical` 的告警
>（ETL 失敗、blocking 檢核失敗、管道停擺）會推播 LINE；warning 級（筆數偏離、
> 空值率、時效落後）只進 console 與 Grafana——避免 alert fatigue。
> 免費方案每月 500 則推播；預設只推 FIRING 不推 RESOLVED，
> `repeat_interval` 也已設 5m 防止重複轟炸。
> Quick Tunnel 網址每次重啟會變，需重新貼到 LINE Console；
> 長期使用建議改 named tunnel（需 Cloudflare 帳號）。

### 調整參數

`docker-compose.yml` 環境變數：

- producer：`PRODUCE_INTERVAL_SECONDS`（落檔節奏，演示可調 5 秒加快）、
  `ERROR_MULTIPLIER`（錯誤頻率倍數，0 = 全健康；2.0 = 高頻事故）、`SEED`（可重現）
- airflow：DAG `schedule=timedelta(minutes=1)`（`airflow/dags/etl_quality_dags.py`）

## Case 2 — 多租戶佇列檔案處理（規劃中）

RabbitMQ + 多租戶 worker + DLQ + 冪等處理，重現「大客戶突發上傳拖垮全體」事故與修復。

```bash
docker compose down -v   # 全部清掉重來
```
