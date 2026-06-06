"""錯誤注入框架 — data producer 透過此模組隨機（或強制）對批次資料注入七種典型異常。

設計原則：
- 每個情境是一個獨立函式，回傳 (rows, header, file_mode, delay_days)
- 注入發生在「上游落檔之前」，模擬真實世界中上游系統的各種失誤
- 機率由 config.ERROR_SCENARIOS 控制，也可用 force 參數指定情境做現場演示
"""
import random

from config import ERROR_SCENARIOS, EXPECTED_COLUMNS

FILE_NORMAL = "normal"
FILE_CORRUPT = "corrupt"


def choose_scenario(rng: random.Random, multiplier: float = 1.0, force: str | None = None) -> str | None:
    """每日抽籤：回傳要注入的情境名稱，或 None（健康日）。"""
    if force:
        if force not in ERROR_SCENARIOS:
            raise ValueError(f"unknown scenario: {force}, choose from {list(ERROR_SCENARIOS)}")
        return force
    roll = rng.random()
    cumulative = 0.0
    for name, prob in ERROR_SCENARIOS.items():
        cumulative += prob * multiplier
        if roll < cumulative:
            return name
    return None


def apply_scenario(rows: list[dict], scenario: str | None, rng: random.Random):
    """對乾淨資料套用指定情境。

    回傳 (rows, header, file_mode, delay_days)
    - header: 落檔時實際使用的欄位（schema_drift 會偷改）
    - file_mode: normal | corrupt
    - delay_days: late_arrival 時 > 0
    """
    header = list(EXPECTED_COLUMNS)
    if scenario is None:
        return rows, header, FILE_NORMAL, 0

    if scenario == "row_drop":
        # 上游靜默少送 40%~70% 的資料 —— 檔案本身完全正常，只能靠流量基線抓
        keep = rng.uniform(0.3, 0.6)
        rows = rng.sample(rows, max(1, int(len(rows) * keep)))
        return rows, header, FILE_NORMAL, 0

    if scenario == "null_spike":
        # 20%~50% 的列關鍵欄位變空值
        ratio = rng.uniform(0.2, 0.5)
        target_col = rng.choice(["customer_id_masked", "amount"])
        for row in rng.sample(rows, int(len(rows) * ratio)):
            row[target_col] = ""
        return rows, header, FILE_NORMAL, 0

    if scenario == "schema_drift":
        # 上游改版偷改欄位名：amount -> amt
        header = [c if c != "amount" else "amt" for c in header]
        for row in rows:
            row["amt"] = row.pop("amount")
        return rows, header, FILE_NORMAL, 0

    if scenario == "duplicate_pk":
        # 5%~15% 的列被重複送出（上游重送機制 bug）
        ratio = rng.uniform(0.05, 0.15)
        dups = [dict(r) for r in rng.sample(rows, int(len(rows) * ratio))]
        rows = rows + dups
        rng.shuffle(rows)
        return rows, header, FILE_NORMAL, 0

    if scenario == "encryption_leak":
        # 2%~10% 的列暗碼欄位混入明碼（如真實身分證字號格式）—— 合規最高風險
        ratio = rng.uniform(0.02, 0.10)
        for row in rng.sample(rows, max(1, int(len(rows) * ratio))):
            row["customer_id_masked"] = "A" + "".join(rng.choices("0123456789", k=9))
        return rows, header, FILE_NORMAL, 0

    if scenario == "late_arrival":
        # 檔案晚到 1~2 個營業日：當日 ETL 會發現檔案缺失
        return rows, header, FILE_NORMAL, rng.randint(1, 2)

    if scenario == "corrupt_file":
        return rows, header, FILE_CORRUPT, 0

    raise ValueError(f"unhandled scenario: {scenario}")
