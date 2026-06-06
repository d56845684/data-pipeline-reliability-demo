"""資料品質檢核 — 分 blocking（中斷+告警）與 warning（記錄+告警）兩級。

對應 STAR 文件 Case 1 的設計：
- Blocking：schema 一致性、主鍵重複、暗碼欄位格式（明碼洩漏）
- Warning：筆數動態基線（同期 ±3σ）、關鍵欄位空值率、freshness
"""
import re
import statistics
from dataclasses import dataclass, field

import config
from config import EXPECTED_COLUMNS

ENC_PATTERN = re.compile(r"^ENC\$[0-9a-f]{16}$")
NULL_RATIO_THRESHOLD = 0.10       # 關鍵欄位空值率 > 10% 即告警
ENC_INVALID_THRESHOLD = 0.005     # 暗碼格式錯誤 > 0.5% 即視為明碼洩漏風險
ZSCORE_THRESHOLD = 3.0
MIN_BASELINE_RUNS = 5             # 同期歷史至少 5 筆才啟用動態基線


@dataclass
class CheckResult:
    name: str
    severity: str          # blocking | warning
    passed: bool
    value: float
    threshold: float
    message: str = ""


@dataclass
class QualityReport:
    blocking: list[CheckResult] = field(default_factory=list)
    warning: list[CheckResult] = field(default_factory=list)

    @property
    def blocking_failed(self) -> bool:
        return any(not c.passed for c in self.blocking)

    @property
    def warning_failed(self) -> bool:
        return any(not c.passed for c in self.warning)

    def all_checks(self) -> list[CheckResult]:
        return self.blocking + self.warning


# ---------- Blocking ----------

def check_schema(header: list[str]) -> CheckResult:
    missing = set(EXPECTED_COLUMNS) - set(header)
    extra = set(header) - set(EXPECTED_COLUMNS)
    ok = not missing and not extra
    msg = "" if ok else f"missing={sorted(missing)} extra={sorted(extra)}"
    return CheckResult("schema_consistency", "blocking", ok, float(len(missing) + len(extra)), 0, msg)


def check_duplicate_pk(rows: list[dict]) -> CheckResult:
    total = len(rows)
    distinct = len({r["record_id"] for r in rows})
    dup_ratio = (total - distinct) / total if total else 0.0
    return CheckResult("duplicate_pk", "blocking", dup_ratio == 0, dup_ratio, 0,
                       f"{total - distinct}/{total} duplicated" if dup_ratio else "")


def check_encryption_format(rows: list[dict]) -> CheckResult:
    """暗碼欄位格式檢核：非空值中不符合 ENC$<16hex> 的比例（明碼洩漏風險）。"""
    values = [r["customer_id_masked"] for r in rows if r.get("customer_id_masked")]
    if not values:
        return CheckResult("encryption_format", "blocking", True, 0.0, ENC_INVALID_THRESHOLD)
    invalid = sum(1 for v in values if not ENC_PATTERN.match(v))
    ratio = invalid / len(values)
    return CheckResult("encryption_format", "blocking", ratio <= ENC_INVALID_THRESHOLD,
                       ratio, ENC_INVALID_THRESHOLD,
                       f"{invalid} rows look like PLAINTEXT" if invalid else "")


# ---------- Warning ----------

def check_null_ratio(rows: list[dict], column: str) -> CheckResult:
    total = len(rows)
    nulls = sum(1 for r in rows if not r.get(column)) if total else 0
    ratio = nulls / total if total else 0.0
    return CheckResult(f"null_ratio_{column}", "warning", ratio <= NULL_RATIO_THRESHOLD,
                       ratio, NULL_RATIO_THRESHOLD)


def rowcount_zscore(current: int, history: list[int]) -> tuple[float, bool]:
    """與同期（同 weekday 類型）歷史比較的 z-score。

    回傳 (zscore, baseline_ready)。歷史不足時回 (0, False)。
    """
    if len(history) < MIN_BASELINE_RUNS:
        return 0.0, False
    mean = statistics.fmean(history)
    std = statistics.pstdev(history)
    std = max(std, 0.03 * mean)  # std 下限 3%，避免過度敏感
    return (current - mean) / std, True


def check_rowcount_baseline(current: int, history: list[int]) -> CheckResult:
    z, ready = rowcount_zscore(current, history)
    if not ready:
        return CheckResult("rowcount_baseline", "warning", True, 0.0, ZSCORE_THRESHOLD,
                           "baseline warming up")
    return CheckResult("rowcount_baseline", "warning", abs(z) <= ZSCORE_THRESHOLD,
                       z, ZSCORE_THRESHOLD,
                       f"z={z:+.1f} vs 同期基線" if abs(z) > ZSCORE_THRESHOLD else "")


def check_duration_baseline(duration_s: float, history: list[float]) -> CheckResult:
    """執行時間劣化偵測：超過過往 N 次成功執行平均的 +10% 即告警。

    value 為「本次 / 歷史平均」的比值（1.0 = 與平均相同）。
    低於 DURATION_MIN_SECONDS 的小 job 不告警（避免秒級以下的噪聲誤報）。
    """
    if len(history) < MIN_BASELINE_RUNS:
        return CheckResult("duration_baseline", "warning", True, 1.0,
                           config.DURATION_RATIO_THRESHOLD, "baseline warming up")
    avg = statistics.fmean(history)
    if avg <= 0:
        return CheckResult("duration_baseline", "warning", True, 1.0,
                           config.DURATION_RATIO_THRESHOLD)
    ratio = duration_s / avg
    passed = ratio <= config.DURATION_RATIO_THRESHOLD or duration_s < config.DURATION_MIN_SECONDS
    return CheckResult("duration_baseline", "warning", passed, ratio,
                       config.DURATION_RATIO_THRESHOLD,
                       "" if passed else
                       f"本次 {duration_s:.2f}s 為過往 {len(history)} 次平均（{avg:.2f}s）的 {ratio:.2f} 倍")


def run_checks(header: list[str], rows: list[dict], history: list[int]) -> QualityReport:
    report = QualityReport()
    report.blocking.append(check_schema(header))
    # schema 壞了後續欄位級檢核無意義
    if report.blocking[0].passed:
        report.blocking.append(check_duplicate_pk(rows))
        report.blocking.append(check_encryption_format(rows))
        report.warning.append(check_null_ratio(rows, "customer_id_masked"))
        report.warning.append(check_null_ratio(rows, "amount"))
        report.warning.append(check_rowcount_baseline(len(rows), history))
    return report
