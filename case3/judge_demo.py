"""Ask the hosted judge model about a few cancel-recharge cases and print what it returns.

    python3 judge_demo.py

Reads OPENROUTER_API_KEY from the environment or from ../.env. The cases are made up.
"""
import sys

from engagement.judge import JevJudge, KeywordJudge, load_api_key

CASES = [
    ("沒有聊天的使用者", {"segment": "first_time", "cancel_step": "choose_plan", "recent_chat": []}),
    ("嫌貴", {"segment": "first_time", "cancel_step": "choose_plan", "recent_chat": ["這個也太貴了吧", "有沒有首儲優惠"]}),
    ("想送禮但餘額不足", {"segment": "repeat", "cancel_step": "choose_plan", "recent_chat": ["主播等我一下", "餘額不夠想送個火箭"]}),
    ("付款失敗", {"segment": "repeat", "cancel_step": "payment", "error_code": "CARD_DECLINED", "recent_chat": ["怎麼一直刷不過"]}),
    ("與儲值無關的聊天", {"segment": "first_time", "cancel_step": "choose_plan", "recent_chat": ["主播晚安", "這首歌好聽"]}),
    ("反話", {"segment": "repeat", "cancel_step": "choose_plan", "recent_chat": ["哇這價格真是佛心呢，我還是去吃土好了"]}),
]


def main() -> None:
    key = load_api_key()
    if not key:
        sys.exit("OPENROUTER_API_KEY not found in the environment or ../.env")
    jev, stub = JevJudge(key), KeywordJudge()
    total = 0.0
    for title, ctx in CASES:
        out = jev.cancel_reason(ctx)
        print(f"\n{title}　聊天：{ctx['recent_chat'] or '（無）'}")
        if "error" in out:
            print(f"  呼叫失敗：{out['error']}")
            continue
        total += out["cost_usd"] or 0
        confidence = "n/a" if out["confidence"] is None else f"{out['confidence']:.2f}"
        print(f"  Jev   reason={out['choice']:<17} confidence={confidence}  "
              f"flow_error={out['flow_error']:.2f}  wants_to_gift={out['wants_to_gift']:.2f}  "
              f"{out['latency_ms']} ms")
        print(f"  關鍵字佔位實作 reason={stub.cancel_reason(ctx)['choice']}")
    print(f"\n{len(CASES)} 筆合計成本：${total:.5f}")


if __name__ == "__main__":
    main()
