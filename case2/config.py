"""Case 2 設定 — 多租戶 SaaS 檔案處理管道（RabbitMQ）。"""
import os

RABBIT_URL = os.getenv("RABBIT_URL", "amqp://guest:guest@rabbitmq:5672/%2F")
PG_DSN = os.getenv("PG_DSN", "postgresql://etl:etl@postgres:5432/warehouse")

# ---- 架構開關（事故重現 ↔ 修復後對比）----
# false = 修復前：所有租戶共用單一向量化佇列（noisy neighbor 事故架構）
# true  = 修復後：每租戶獨立佇列 + 輪詢公平消化
PER_TENANT_QUEUES = os.getenv("PER_TENANT_QUEUES", "false").lower() == "true"
# true = 修復後：batch upsert；false = 修復前：逐筆 INSERT
BATCH_WRITES = os.getenv("BATCH_WRITES", "true").lower() == "true"

# ---- LLM embedding 模擬（耗時 = base + token 數 × per-token）----
EMBED_BASE_MS = float(os.getenv("EMBED_BASE_MS", "30"))          # 每次推論呼叫固定 overhead
EMBED_MS_PER_TOKEN = float(os.getenv("EMBED_MS_PER_TOKEN", "0.05"))
EMBED_DIM = int(os.getenv("EMBED_DIM", "384"))                   # 向量維度
EMBED_TIMEOUT_PROB = float(os.getenv("EMBED_TIMEOUT_PROB", "0.02"))  # 偶發推論逾時機率
EMBED_BATCH_SIZE = int(os.getenv("EMBED_BATCH_SIZE", "16"))      # 批次推論大小（BATCH_WRITES=false 時退化為 1）

PREPROCESS_MS = float(os.getenv("PREPROCESS_MS", "5"))   # 切割每 chunk 耗時
UPLOAD_INTERVAL_SECONDS = float(os.getenv("UPLOAD_INTERVAL_SECONDS", "3"))
METRICS_PORT = int(os.getenv("METRICS_PORT", "8000"))
SEED = int(os.getenv("SEED", "7"))

# ---- 隨機異常注入（uploader 每次上傳擲骰，命中即對隨機租戶觸發）----
ERROR_MULTIPLIER = float(os.getenv("ERROR_MULTIPLIER", "1.0"))
ERROR_SCENARIOS = {
    "poison": 0.02,       # 損毀檔案 → 向量化失敗 → DLQ
    "duplicate": 0.03,    # 上游重送，同一檔案投遞兩次 → 冪等跳過
    "mini_burst": 0.01,   # 單一租戶突發上傳 10-25 份中大型檔案
}

# ---- 預期外處理失敗（vector worker 端，訊息進 DLQ）----
# 暫時性故障（embedding 服務 5xx / worker OOM）：重放即成功 → 驗證哨兵 auto-replay
TRANSIENT_FAIL_PROB = float(os.getenv("TRANSIENT_FAIL_PROB", "0.004"))
# 黏性失敗（特定輸入觸發的未知 bug，由內容 hash 決定）：每次重放都失敗
# → 耗盡哨兵重放額度後留在 DLQ 等人工 → 驗證 human-residual 路徑
STICKY_FAIL_PROB = float(os.getenv("STICKY_FAIL_PROB", "0.0003"))

# ---- 租戶 ----
TENANTS = ["acme", "globex", "initech", "umbrella", "stark", "wayne", "hooli", "dunder"]
BIG_TENANT = "megacorp"          # 事故主角：一次上傳上百份大檔的大客戶
ALL_TENANTS = TENANTS + [BIG_TENANT]

# ---- 佇列拓撲 ----
UPLOAD_QUEUE = "c2.upload"
VECTOR_SHARED_QUEUE = "c2.vector.shared"
DLX_EXCHANGE = "c2.dlx"
DLQ_QUEUE = "c2.vector.dlq"


def vector_queue_for(tenant: str) -> str:
    return f"c2.vector.{tenant}" if PER_TENANT_QUEUES else VECTOR_SHARED_QUEUE


def vector_queues() -> list[str]:
    if PER_TENANT_QUEUES:
        return [f"c2.vector.{t}" for t in ALL_TENANTS]
    return [VECTOR_SHARED_QUEUE]
