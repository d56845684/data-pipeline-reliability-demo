# RB-03：重跑與回補（Backfill）SOP

**適用情境**：`ETLJobFailed` 修復後重跑、缺檔補到後回補、歷史資料修正

> 核心原則：**所有載入皆為冪等**（`INSERT ... ON CONFLICT DO UPDATE`，
> 以 pipeline + record_id 為冪等鍵），重跑任意次數不會產生重複資料。
> 因此回補的風險不在資料正確性，而在**資源排擠**與**下游時序**。

## 1. 回補前檢查

- [ ] 確認根因已修復（上游重送了正確檔案 / 程式已修正），否則重跑只會再失敗一次
- [ ] 確認回補範圍：哪些 pipeline、哪些 business_date
- [ ] 範圍超過 7 個營業日 → 通知下游 owner（聚合報表可能短暫出現修正中的數字）

## 2. 執行

```bash
# 單一管道單日重跑（ETL 服務也會自動偵測落地檔並補跑，手動僅用於指定重跑）
docker compose exec etl python etl_job.py --pipeline <pipeline> --date <YYYY-MM-DD>
```

- **大範圍回補（> 30 天）**：分批執行（每批 ≤ 7 天，批間隔開），避免與當日正常批次搶資源
- 回補期間關注 Grafana `etl_duration_seconds` 與資料庫負載，異常即暫停

## 3. 驗證（不可省略）

```bash
# 1) 執行狀態全綠
psql -c "SELECT business_date, status, rows_loaded FROM etl_run_stats
         WHERE pipeline='<pipeline>' AND business_date BETWEEN '<from>' AND '<to>'
         ORDER BY business_date;"
# 2) 與來源筆數對賬（source vs loaded 一致）
# 3) 抽驗 1-2 個營業日的關鍵欄位分布是否合理
```

## 4. 收尾

- 在事故/工單記錄回補範圍、執行時間、驗證結果
- 通知下游 owner 資料已修復，註明影響的日期區間
- 若回補起因是程式 bug → 確認修復已上版控並補自動化測試
