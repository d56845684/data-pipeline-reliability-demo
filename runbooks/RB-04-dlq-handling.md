# RB-04：DLQ 訊息處置（佇列管道）

**觸發告警**：`C2DLQNotEmpty`（critical）

> 核心原則：**DLQ 是分流區（triage area），不是儲存區**。
> 訊息進 DLQ 代表「自動重試救不回來，需要人做決策」——目標是在 SLA 內
> 把 DLQ 清空：能修的修完重放，不能修的隔離存證後通知上游。

## 1. 檢視與分類（15 分鐘內）

```bash
docker compose exec case2-vector python dlq_tool.py inspect
```

| 分類 | 含義 | 處置 |
|------|------|------|
| `transient_or_unknown` | 暫時性故障（DB 抖動、依賴服務逾時） | 確認依賴已恢復 → **replay 原樣重放** |
| `poison_content` | 內容本身損毀（客戶上傳的壞檔） | 根因在上游 → **quarantine 隔離** + 通知租戶重新上傳；若是我方解析 bug → 修復部署後 replay |
| `schema_incompatible` | 舊版/異常訊息格式（如部署期間殘留的 in-flight 訊息） | **只能 quarantine**——盲目 replay 會立刻再進 DLQ 形成迴圈 |

## 2. 處置

```bash
# 可修復的重放（冪等鍵保證已入庫的 chunk 不會重複）；schema 不相容自動改走隔離
docker compose exec case2-vector python dlq_tool.py replay

# 或全部隔離入庫存證（c2_dlq_quarantine 表，含完整 payload 供審計）
docker compose exec case2-vector python dlq_tool.py quarantine
```

隔離後續處理：

```sql
SELECT tenant, reason, count(*) FROM c2_dlq_quarantine
GROUP BY tenant, reason ORDER BY count DESC;
```

- `poison_content` → 依租戶彙整，通知客戶成功/失敗清單，請其修復後重新上傳
- 同一租戶反覆出現 → 檢討該租戶的上傳內容驗證是否該前移到 API 層（fail fast）

## 3. 驗證

- `dlq_tool.py inspect` 回報「DLQ 是空的」、`C2DLQNotEmpty` 告警 RESOLVED
- replay 的訊息：確認對應租戶佇列消化完、chunk 正常入庫（無重複——冪等鍵保證）

## 4. 預防與升級

- **replay 永遠假設根因已修復**——修復前 replay 只是把訊息在 DLQ 和工作佇列之間搬運
- DLQ 訊息年齡 > 1 個工作天仍未處置 → 升級 team lead
- 30 天內同類原因第 2 次出現 → 發起 postmortem，把驗證邏輯前移（上傳 API 即拒收）
- 部署前確認佇列中 in-flight 訊息與新版 schema 的相容性（必要時雙版本相容一個發布週期）
