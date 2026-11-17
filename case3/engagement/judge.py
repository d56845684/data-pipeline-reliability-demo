"""Judge interface. In the MVP it runs in shadow: called and logged, never used.

Two implementations of the same method:
- KeywordJudge: offline placeholder so the PoC and the tests run without a network.
- JevJudge: the hosted judge model, asked three questions.
"""
import json
import os
import ssl
import time
import urllib.request

try:  # python.org builds on macOS ship without a CA bundle
    import certifi
    _SSL = ssl.create_default_context(cafile=certifi.where())
except ImportError:
    _SSL = ssl.create_default_context()

REASONS = {
    "too_expensive": "使用者覺得價格太高或在等優惠。",
    "wants_to_gift": "使用者想送禮給主播但餘額不足。",
    "payment_friction": "付款流程卡住、失敗或太麻煩。",
    "just_browsing": "只是看看方案，沒有明確的付費意圖。",
    "mistap": "誤觸儲值按鈕。",
}

_KEYWORDS = {
    "too_expensive": ("貴", "便宜", "優惠"),
    "wants_to_gift": ("送禮", "禮物", "餘額"),
    "payment_friction": ("付款", "刷不過", "失敗"),
}


class KeywordJudge:
    name = "keyword-stub"

    def cancel_reason(self, ctx: dict) -> dict:
        text = " ".join(ctx["recent_chat"])
        for reason, words in _KEYWORDS.items():
            if any(w in text for w in words):
                return {"model": self.name, "choice": reason, "confidence": 0.8, "used": False}
        # No chat means no semantic signal: low confidence, falls back to rules.
        return {"model": self.name, "choice": "just_browsing", "confidence": 0.3, "used": False}


def load_api_key(env_file: str | None = None) -> str | None:
    """OPENROUTER_API_KEY from the environment, else from a .env file (any letter case)."""
    key = os.environ.get("OPENROUTER_API_KEY") or os.environ.get("openrouter_api_key")
    if key:
        return key
    path = env_file or os.path.join(os.path.dirname(__file__), "..", "..", ".env")
    try:
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                name, _, value = line.strip().partition("=")
                if name.strip().upper() == "OPENROUTER_API_KEY":
                    return value.strip().strip("'\"")
    except OSError:
        pass
    return None


class JevJudge:
    """Typed questions in, typed answers out. No text to parse."""
    url = "https://openrouter.ai/api/alpha/decisions"

    def __init__(self, api_key: str, model: str = "typesafe/jev-1.14", timeout: float = 20.0):
        self.api_key, self.name, self.timeout = api_key, model, timeout

    def cancel_reason(self, ctx: dict) -> dict:
        body = {
            "model": self.name,
            "state": {
                "情境": "直播平台的使用者點了儲值，之後沒有完成就離開儲值流程。",
                "停在哪一步": ctx.get("cancel_step"),
                "付款錯誤碼": ctx.get("error_code"),
                "是否曾經儲值": ctx.get("segment") == "repeat",
                "取消前一小時在聊天室說的話": ctx.get("recent_chat") or "（沒有發言）",
            },
            "questions": {
                "reason": {"type": "choice", "instructions": "這次取消儲值最可能的原因是哪一個？", "criteria": REASONS},
                "flow_error": {"type": "noul", "instructions": "這次取消是付款流程出錯造成的，而不是使用者主動放棄？"},
                "wants_to_gift": {"type": "noul", "instructions": "使用者當下正想送禮給目前的主播？"},
            },
        }
        request = urllib.request.Request(
            self.url, data=json.dumps(body).encode(),
            headers={"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"},
        )
        started = time.time()
        try:
            with urllib.request.urlopen(request, timeout=self.timeout, context=_SSL) as response:
                data = json.load(response)
            answers = data["answers"]
            return {
                "model": self.name,
                "choice": answers["reason"]["choice"],
                "confidence": answers["reason"].get("confidence"),
                "flow_error": answers["flow_error"]["noul"],
                "wants_to_gift": answers["wants_to_gift"]["noul"],
                "latency_ms": round((time.time() - started) * 1000),
                "cost_usd": (data.get("usage") or {}).get("cost"),
                "used": False,
            }
        except Exception as exc:  # shadow mode: a judge failure must never block a decision
            return {"model": self.name, "error": f"{type(exc).__name__}: {exc}"[:200], "used": False}
