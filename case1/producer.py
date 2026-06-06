"""Data producer — 模擬上游核心系統（保單/理賠/收付…）每日批次落檔。

特性：
- 流量具備真實的週期效應（週末量低 + 隨機噪聲），讓動態基線有意義
- 透過 error_injector 隨機注入七種典型異常，也可 force 指定情境現場演示
- CLI 可單獨執行：python producer.py --pipeline policies --date 2026-02-01 --force-error encryption_leak
"""
import argparse
import csv
import os
import random
from datetime import date, timedelta
from pathlib import Path

import config
import error_injector
from error_injector import FILE_CORRUPT

CATEGORIES = ["auto", "fire", "marine", "health", "liability", "engineering"]
REGIONS = ["north", "central", "south", "east", "offshore"]


def expected_rows(base: int, business_date: date, rng: random.Random) -> int:
    """週末量約為平日 45%，外加 ±5% 噪聲。"""
    factor = 0.45 if business_date.weekday() >= 5 else 1.0
    return max(10, int(base * factor * rng.gauss(1.0, 0.05)))


def generate_rows(pipeline: str, business_date: date, n: int, rng: random.Random) -> list[dict]:
    prefix = pipeline[:3].upper()
    rows = []
    for i in range(n):
        rows.append({
            "record_id": f"{prefix}-{business_date:%Y%m%d}-{i:06d}",
            "customer_id_masked": "ENC$" + "".join(rng.choices("0123456789abcdef", k=16)),
            "amount": f"{rng.uniform(1000, 250000):.2f}",
            "category": rng.choice(CATEGORIES),
            "region": rng.choice(REGIONS),
            "event_date": (business_date - timedelta(days=rng.choices([0, 1], weights=[0.9, 0.1])[0])).isoformat(),
        })
    return rows


def write_file(path: Path, rows: list[dict], header: list[str], file_mode: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if file_mode == FILE_CORRUPT:
        # 寫入含 NUL 與亂碼的損毀檔，模擬傳輸中斷/磁碟壞軌
        with open(path, "wb") as f:
            f.write(",".join(header).encode() + b"\n")
            f.write(b"\x00\xffGARBAGE\x00" * 200)
        return
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        writer.writerows(rows)


def landing_path(pipeline: str, business_date: date) -> Path:
    return Path(config.DATA_DIR) / pipeline / f"{business_date.isoformat()}.csv"


def produce(pipeline: str, business_date: date, rng: random.Random,
            force_error: str | None = None, error_multiplier: float | None = None) -> dict:
    """產生一個營業日的批次檔。

    回傳 {"scenario": str|None, "delay_days": int, "path": Path, "rows": int}
    delay_days > 0 時檔案尚未落地，由呼叫端（simulator）排程延後寫入。
    """
    multiplier = config.ERROR_MULTIPLIER if error_multiplier is None else error_multiplier
    base = config.PIPELINES[pipeline]["base_rows"]
    n = expected_rows(base, business_date, rng)
    rows = generate_rows(pipeline, business_date, n, rng)

    scenario = error_injector.choose_scenario(rng, multiplier, force_error)
    rows, header, file_mode, delay_days = error_injector.apply_scenario(rows, scenario, rng)

    path = landing_path(pipeline, business_date)
    if delay_days == 0:
        write_file(path, rows, header, file_mode)
    return {
        "scenario": scenario,
        "delay_days": delay_days,
        "path": path,
        "rows": len(rows),
        # late_arrival 需要延後寫檔，把材料帶回給 simulator
        "_pending": (rows, header, file_mode) if delay_days > 0 else None,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="模擬上游每日批次落檔（可注入錯誤）")
    parser.add_argument("--pipeline", required=True, choices=list(config.PIPELINES))
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--force-error", default=None, choices=list(config.ERROR_SCENARIOS))
    args = parser.parse_args()

    rng = random.Random(os.getenv("SEED") or None)
    result = produce(args.pipeline, date.fromisoformat(args.date), rng, force_error=args.force_error)
    if result["delay_days"]:
        print(f"[producer] {args.pipeline} {args.date}: late_arrival, 檔案將晚 {result['delay_days']} 天")
    else:
        print(f"[producer] {args.pipeline} {args.date}: scenario={result['scenario']}, "
              f"rows={result['rows']}, file={result['path']}")


if __name__ == "__main__":
    main()
