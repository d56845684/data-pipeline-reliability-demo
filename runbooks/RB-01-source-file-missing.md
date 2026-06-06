# RB-01：來源檔案缺失 / 遲到

**觸發告警**：`ETLFileMissing`（warning）、`ETLFreshnessLag`（warning）、`ETLPipelineStale`（critical）

## 1. 影響評估（5 分鐘內）

1. 查 Grafana 健康矩陣：確認是**單一管道**還是**多管道同時**缺檔
   - 多管道同時缺 → 大概率是來源系統或傳輸層全域問題，直接跳「全域缺檔」分支
2. 確認管道的 criticality（P1 法遵申報相關 → 同時通知 team lead）
3. 確認下游今日是否有依賴此資料的排程（報表/申報截止時間）

## 2. 判斷步驟

```bash
# 確認落地區實際狀態（檔案是否存在、大小是否異常）
ls -la /data/landing/<pipeline>/
# 查最近的執行歷史
psql -c "SELECT business_date, status, rows_loaded, run_at FROM etl_run_stats
         WHERE pipeline='<pipeline>' ORDER BY business_date DESC LIMIT 7;"
```

| 現象 | 判斷 | 動作 |
|------|------|------|
| 檔案完全沒到 | 上游未產出或傳輸失敗 | 聯絡上游系統值班（聯絡簿見 wiki），確認預計到檔時間 |
| 檔案存在但大小異常小 | 上游產出不完整 | 視同缺檔處理，要求上游重送，**不可**讓 ETL 處理半成品 |
| 檔案晚到且已落地 | 遲到 | ETL 會自動回補（偵測到檔案即補跑），確認回補後狀態轉 OK 即可 |

## 3. 止血

- 下游有截止時間壓力：以**前一日資料 + 明確標註**先行，並通知下游使用者資料時效
- 上游預計到檔時間超過 SLA：升級處理，通知受影響的下游 owner

## 4. 恢復確認

- 檔案到齊後確認 ETL 自動回補成功（`etl_run_stats` 該日 status = 0/1）
- Grafana 健康矩陣轉綠、`ETLFileMissing` 告警 RESOLVED

## 5. 記錄

- 在事故 channel 記錄：缺檔原因、到檔時間、影響範圍
- 同一上游 30 天內第 2 次缺檔 → 發起跨團隊檢討，列入 postmortem 流程
