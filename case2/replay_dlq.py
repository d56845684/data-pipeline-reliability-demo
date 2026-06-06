"""DLQ 重放工具 — 修復根因後，把 dead-letter 的訊息送回原佇列重新處理。

模擬「上游已修復損毀內容」：重放時移除 poison 標記。
用法：docker compose exec case2-vector python replay_dlq.py
"""
import json

import common
import config


def main() -> None:
    conn, ch = common.connect_rabbit(retries=3, delay=1)
    common.declare_topology(ch)

    replayed = 0
    while True:
        method, _props, body = ch.basic_get(queue=config.DLQ_QUEUE)
        if method is None:
            break
        msg = json.loads(body)
        msg["poison"] = False   # 模擬根因已修復
        common.publish(ch, config.vector_queue_for(msg["tenant"]), msg)
        ch.basic_ack(method.delivery_tag)
        replayed += 1

    print(f"♻️  已重放 {replayed} 則 DLQ 訊息回處理佇列（冪等鍵保證不會重複入庫）")
    conn.close()


if __name__ == "__main__":
    main()
