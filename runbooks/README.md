# Runbooks — 標準化異常應對流程

對應 Prometheus 告警的值班處理手冊。原則：**任何值班人員照著步驟走，
都能在不依賴資深成員的情況下完成判斷 → 止血 → 恢復 → 記錄**。

## 告警 ↔ Runbook 對照

| 告警 | severity | Runbook |
|------|----------|---------|
| `ETLFileMissing` / `ETLFreshnessLag` | warning | [RB-01 來源檔案缺失/遲到](RB-01-source-file-missing.md) |
| `ETLBlockingCheckFailed`（encryption_format） | critical | [RB-02 暗碼欄位明碼洩漏](RB-02-encryption-leak.md) |
| `ETLJobFailed` / 需重跑回補 | critical | [RB-03 重跑與回補 SOP](RB-03-backfill-sop.md) |
| `ETLRowCountAnomaly` / `ETLNullRatioHigh` | warning | RB-01 的判斷流程 + 通知上游 |
| `C2DLQNotEmpty` | critical | [RB-04 DLQ 訊息處置](RB-04-dlq-handling.md) |
| `C2VectorBacklogCritical` | critical | 擴容 worker（`--scale case2-vector=N`）+ RB-04 的升級原則 |

## 值班約定

- **critical**（LINE/PagerDuty 推播）：15 分鐘內回應，1 小時內止血
- **warning**（console/Grafana）：當日內判斷處理，可累積至日會檢視
- 每次 critical 事故結案後 3 個工作日內產出 postmortem：[模板](postmortem-template.md)
- Runbook 走不通的情況 → 升級資深成員，事後**必須**回頭補強該 runbook
