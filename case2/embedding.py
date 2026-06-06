"""模擬 LLM embedding 推論服務（如 vLLM/Ollama 部署的 embedding 模型）。

延遲模型貼近真實 GPU 批次推論：
    耗時 = EMBED_BASE_MS（固定 overhead：請求/排程/前處理）
         + 總 token 數 × EMBED_MS_PER_TOKEN（吞吐項）

→ 批次化能攤平固定 overhead：batch=16 時單 chunk 平均成本約為逐筆呼叫的 1/3。
另以 EMBED_TIMEOUT_PROB 模擬偶發推論逾時，呼叫端自動指數退避重試。
"""
import hashlib
import math
import random
import time

import config


def estimate_tokens(text: str) -> int:
    return max(1, len(text) // 4)


def _deterministic_vector(text: str) -> list[float]:
    """以內容 hash 為種子產生正規化向量（同樣輸入永遠同樣向量，重放可驗證）。"""
    seed = int.from_bytes(hashlib.sha256(text.encode()).digest()[:8], "big")
    rng = random.Random(seed)
    v = [rng.gauss(0, 1) for _ in range(config.EMBED_DIM)]
    norm = math.sqrt(sum(x * x for x in v)) or 1.0
    return [round(x / norm, 6) for x in v]


def embed_batch(texts: list[str], rng: random.Random,
                max_retries: int = 3) -> tuple[list[list[float]], int, int]:
    """批次推論。回傳 (vectors, total_tokens, retries)。"""
    total_tokens = sum(estimate_tokens(t) for t in texts)
    retries = 0
    for attempt in range(max_retries + 1):
        # 推論耗時（無論成功與否都付出）
        time.sleep((config.EMBED_BASE_MS + total_tokens * config.EMBED_MS_PER_TOKEN) / 1000)
        if rng.random() >= config.EMBED_TIMEOUT_PROB or attempt == max_retries:
            return [_deterministic_vector(t) for t in texts], total_tokens, retries
        retries += 1
        time.sleep(0.2 * (2 ** attempt))   # 指數退避後重試
    raise RuntimeError("unreachable")
