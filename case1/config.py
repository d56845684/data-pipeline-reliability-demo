"""全域設定 — 可由環境變數覆寫。"""
import os

# ---- 模擬節奏 ----
TICK_SECONDS = float(os.getenv("TICK_SECONDS", "15"))   # 每 N 秒 = 模擬一個營業日
WARMUP_DAYS = int(os.getenv("WARMUP_DAYS", "14"))       # 開場先快速跑 N 天無錯誤資料，建立動態基線
SEED = int(os.getenv("SEED", "42"))
START_DATE = os.getenv("START_DATE", "2026-01-01")

# ---- 錯誤注入 ----
# 每條 pipeline 每日抽一次籤，命中即注入該情境（機率可用 ERROR_MULTIPLIER 整體放大/縮小）
ERROR_MULTIPLIER = float(os.getenv("ERROR_MULTIPLIER", "1.0"))
ERROR_SCENARIOS = {
    "row_drop": 0.06,          # 上游靜默少送資料（最危險的隱性異常）
    "null_spike": 0.06,        # 關鍵欄位空值率飆升
    "schema_drift": 0.04,      # 上游偷改欄位名（amount -> amt）
    "duplicate_pk": 0.04,      # 主鍵重複
    "encryption_leak": 0.04,   # 暗碼欄位混入明碼（合規最高風險）
    "late_arrival": 0.05,      # 檔案晚到 1-2 個營業日
    "corrupt_file": 0.03,      # 檔案損毀無法解析
}

# ---- 外部服務 ----
DATA_DIR = os.getenv("DATA_DIR", "/data/landing")
FORCE_FILE = os.getenv("FORCE_FILE", "/data/force_error.json")
PUSHGATEWAY_URL = os.getenv("PUSHGATEWAY_URL", "pushgateway:9091")
PG_DSN = os.getenv(
    "PG_DSN", "postgresql://etl:etl@postgres:5432/warehouse"
)

# ---- 模擬的批次管道（保險業務域）----
PIPELINES = {
    "policies":    {"base_rows": 5000, "criticality": "P1"},
    "claims":      {"base_rows": 1800, "criticality": "P1"},
    "payments":    {"base_rows": 3200, "criticality": "P1"},
    "customers":   {"base_rows": 2400, "criticality": "P2"},
    "agents":      {"base_rows": 600,  "criticality": "P2"},
    "vehicles":    {"base_rows": 1500, "criticality": "P2"},
    "reinsurance": {"base_rows": 300,  "criticality": "P2"},
    "quotes":      {"base_rows": 4000, "criticality": "P3"},
}

# 所有管道共用的標準 schema（落地檔為 CSV）
EXPECTED_COLUMNS = [
    "record_id",          # 主鍵
    "customer_id_masked", # 暗碼欄位，格式 ENC$<16 hex>
    "amount",
    "category",
    "region",
    "event_date",
]
