# Data Pipeline Reliability — Demo

兩個 production-grade 資料管道可靠性案例的**示意性重現**：批次 ETL 品質監控體系，
與多租戶佇列檔案處理。

> ⚠️ 本 repo 為簡化的概念展示（demo），僅呈現核心設計思路與大致流程，
> 並非任何原專案的實際程式碼或完整內容；所有資料均由模擬器產生。

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
| `volume_surge` | 資料量暴增 5–10 倍（補帳/重送） | Warning：筆數基線 z-score + **執行時間劣化**（> 過往 N 次平均 +10%） |

Blocking 失敗 → **中斷載入**（防止污染下游）+ critical 告警；
Warning 失敗 → 照常載入 + warning 告警追蹤。

> **為什麼量與時長要分開偵測**：實測中 volume_surge（7 倍資料量）因 batch upsert
> 高效，執行時間僅多零點幾秒——靠時長抓不到，但筆數基線（z-score）立刻命中；
> 反之，資源爭用造成的變慢（資料量正常、時長 1.8 倍）只有時長基線能抓到。
> 兩個維度互補，缺一不可。時長告警的閾值/樣本數/最小時長下限可由
> `DURATION_RATIO_THRESHOLD`、`DURATION_BASELINE_RUNS`、`DURATION_MIN_SECONDS` 調校。

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

## Case 2 — 多租戶佇列檔案處理（RabbitMQ + LLM embedding）

重現 SaaS 平台的「大客戶突發上傳拖垮全體租戶」事故，以及修復後的架構對比。

```
8 個一般租戶（每 3s 上傳小檔）─┐
megacorp（burst：100 份大檔）──┤
                              ▼
                        c2.upload 佇列
                              │ preprocess worker（切割：1 檔 → 5-80 chunks，含文字內容）
                              ▼
              ┌─ 共用佇列 c2.vector.shared（PER_TENANT_QUEUES=false，事故架構）
              └─ 租戶隔離 c2.vector.<tenant> ×9（=true，修復後）
                              │ vector worker：批次 LLM embedding 推論
                              │（耗時 = 30ms overhead + 0.05ms/token，偶發 timeout 自動重試）
                              ▼
                  PostgreSQL c2_chunks（冪等 batch upsert，含 384 維向量）
                              │ 毒訊息 → DLX → c2.vector.dlq（人工檢視後 replay）
```

| 服務 | URL |
|------|-----|
| Grafana — Case 2 儀表板 | http://localhost:3000/d/case2-queue |
| RabbitMQ Management | http://localhost:15672（guest/guest） |

### 事故重現（noisy neighbor）

```bash
# 預設為「事故架構」（共用單一佇列）。注入大客戶突發上傳：
docker compose exec case2-uploader python inject.py burst        # megacorp 100 份大檔 → ~6000 chunks

# 觀察 Grafana case2 儀表板：佇列深度飆升、所有租戶 P95 同步惡化、
# C2VectorBacklogHigh / C2TenantLatencyHigh 告警 FIRING
```

### 修復後對比（租戶隔離佇列）

```bash
PER_TENANT_QUEUES=true docker compose up -d case2-preprocess case2-vector
docker compose exec case2-uploader python inject.py burst

# 再看儀表板：只有 megacorp 的佇列積壓與延遲上升，其他租戶 P95 不受影響
```

### 隨機異常注入（自動，隨機租戶）

uploader 每次上傳擲骰，異常會隨機落在任一租戶（含 megacorp）：

| 情境 | 機率/次 | 效果 |
|------|--------|------|
| `poison` | 2% | 損毀檔 → 向量化失敗 → DLQ（`C2DLQNotEmpty` 告警） |
| `duplicate` | 3% | 同檔案投遞兩次 → 冪等鍵跳過，計數器可見 |
| `mini_burst` | 1% | 隨機租戶突發上傳 10–25 份中大型檔案 |

`ERROR_MULTIPLIER=0` 可關閉隨機注入（演示前清場用）。

### 手動事故注入演示

```bash
# 毒訊息 → DLQ。哨兵（case2-dlq-sentinel）每 30s 自動分流：transient 自動重放、
# schema 不相容自動隔離；只有 poison 等需人工決策的才留在 DLQ 觸發告警（見 runbooks/RB-04）
docker compose exec case2-uploader python inject.py poison
docker compose exec case2-vector   python dlq_tool.py inspect      # 分類：transient / poison / schema
docker compose exec case2-vector   python dlq_tool.py replay      # 可修復的重放、不相容的自動隔離
docker compose exec case2-vector   python dlq_tool.py quarantine  # 全部隔離入 PG 存證（審計+通知租戶）

# 上游重複投遞 → 冪等鍵跳過（儀表板「冪等跳過」計數 +N）
docker compose exec case2-uploader python inject.py duplicate

# 容量不足時水平擴容 worker（Prometheus 以 DNS 自動發現新 replica）
docker compose up -d --scale case2-vector=4

# 修復前的 DB 寫入方式對比（逐 chunk 推論 + 逐筆 INSERT，吞吐約降為 1/3）
BATCH_WRITES=false docker compose up -d case2-vector
```

### 設計對應（STAR Case 2 ↔ demo 實作）

| STAR 中的修復項 | demo 實作 |
|----------------|-----------|
| 租戶公平性（佇列拆分 + 輪詢） | `PER_TENANT_QUEUES` 切換，prefetch 小值公平消化 |
| 冪等處理 + DLQ + 三段式處置 | PK `(file_id, chunk_idx)` + `ON CONFLICT DO NOTHING`、DLX、`dlq_tool.py`（inspect/replay/quarantine）+ RB-04 |
| 批次寫入調校 | 批次 embedding（攤平推論 overhead）+ `execute_values` batch upsert |
| 佇列深度告警與監控 | rabbitmq_prometheus per-queue 指標 + 租戶級 P95 histogram |
| 彈性伸縮 | `--scale case2-vector=N` + Prometheus DNS 服務發現 |

```bash
docker compose down -v   # 全部清掉重來
```
