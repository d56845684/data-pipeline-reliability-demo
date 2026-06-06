# RB-02：暗碼欄位明碼洩漏（最高風險）

**觸發告警**：`ETLBlockingCheckFailed{check="encryption_format"}`（critical）

> ⚠️ 本情境涉及個資合規風險，處理優先級最高。blocking 檢核已自動**中斷載入**，
> 受污染資料不會進入倉儲——但落地區的原始檔案本身含明碼，仍需處置。

## 1. 立即確認（15 分鐘內）

```bash
# 確認該批次確實未載入（loaded 應為 0）
psql -c "SELECT status, rows_loaded, checks FROM etl_run_stats
         WHERE pipeline='<pipeline>' AND business_date='<date>';"
# 抽驗落地檔的暗碼欄位格式（確認洩漏範圍，不要外傳輸出內容）
head -5 /data/landing/<pipeline>/<date>.csv
```

- 確認**只有落地區**含明碼資料，倉儲與下游無污染
- 若發現倉儲已有歷史污染（先前版本無此檢核）→ 立即升級 team lead + 資安窗口

## 2. 止血

1. 限制落地區該檔案的存取權限（只留處理人員）
2. 通知上游系統值班：加密流程異常，要求停止後續批次直到修復
3. 在事故 channel 開 incident，標記 P1

## 3. 恢復

1. 上游修復加密流程後重送正確檔案
2. 確認重送檔案通過 `encryption_format` 檢核、正常載入
3. 安全銷毀含明碼的問題檔案（依資料銷毀程序），記錄銷毀時間與經手人

## 4. 事後（必做）

- 3 個工作日內產出 [postmortem](postmortem-template.md)
- RCA 必須回答：上游加密流程**為什麼**會失效（改版？設定？金鑰輪替？）、
  為什麼上游自己的測試沒抓到
- 檢討項固定包含：上游發版前是否需要 DataOps 參與 schema/加密格式回歸驗證
