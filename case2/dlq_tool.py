"""DLQ 處置工具 — 對應 RB-04 的三段式流程：inspect → 分類 → replay / quarantine。

核心原則：DLQ 是分流區（triage area），不是儲存區。
- 暫時性故障         → replay 原樣重放（冪等鍵保證不重複入庫）
- 已修復的內容問題    → replay 會移除 poison 標記（模擬根因已修復）
- schema 不相容訊息  → 自動改走 quarantine（盲目 replay 會無限循環回 DLQ）
- quarantine        → 取出寫入 PG 隔離表（可審計、可通知租戶），佇列清空

用法：
    docker compose exec case2-vector python dlq_tool.py inspect      # 只看不動
    docker compose exec case2-vector python dlq_tool.py replay      # 可修復的重放、其餘隔離
    docker compose exec case2-vector python dlq_tool.py quarantine  # 全部隔離入庫存證
"""
import json
import sys
from collections import Counter

import common
import config

QUARANTINE_DDL = """
CREATE TABLE IF NOT EXISTS c2_dlq_quarantine (
    id             bigserial PRIMARY KEY,
    tenant         text,
    file_id        text,
    chunk_idx      int,
    reason         text NOT NULL,
    payload        jsonb,
    quarantined_at timestamptz NOT NULL DEFAULT now()
);
"""

REQUIRED_FIELDS = ("file_id", "chunk_idx", "tenant", "upload_ts", "text")


def classify(msg: dict) -> str:
    missing = [f for f in REQUIRED_FIELDS if f not in msg]
    if msg.get("poison"):
        return "poison_content"            # 客戶內容損毀（可於根因修復後 replay）
    if missing:
        return "schema_incompatible"       # 舊版/異常 schema（只能隔離，replay 必再失敗）
    return "transient_or_unknown"          # 暫時性故障（可原樣 replay）


def drain(ch):
    """逐筆取出 DLQ 訊息。"""
    while True:
        method, _props, body = ch.basic_get(queue=config.DLQ_QUEUE)
        if method is None:
            return
        try:
            msg = json.loads(body)
        except json.JSONDecodeError:
            msg = {}
        yield method, msg


def quarantine_message(pg, msg: dict, reason: str) -> None:
    with pg.cursor() as cur:
        cur.execute(
            "INSERT INTO c2_dlq_quarantine (tenant, file_id, chunk_idx, reason, payload) "
            "VALUES (%s, %s, %s, %s, %s)",
            (msg.get("tenant"), msg.get("file_id"), msg.get("chunk_idx"),
             reason, json.dumps(msg)),
        )


def main() -> None:
    action = sys.argv[1] if len(sys.argv) > 1 else ""
    if action not in ("inspect", "replay", "quarantine"):
        print(__doc__)
        sys.exit(1)

    conn, ch = common.connect_rabbit(retries=3, delay=1)
    common.declare_topology(ch)
    summary: Counter = Counter()

    if action == "inspect":
        # 只分類統計，全部放回佇列
        items = list(drain(ch))
        for _method, msg in items:
            summary[(msg.get("tenant", "?"), classify(msg))] += 1
        for method, _msg in items:
            ch.basic_nack(method.delivery_tag, requeue=True)
        if not items:
            print("✅ DLQ 是空的")
        else:
            print(f"DLQ 共 {len(items)} 則：")
            for (tenant, reason), n in sorted(summary.items()):
                print(f"  {tenant:<10} {reason:<22} ×{n}")
            print("\n處置建議：poison_content → 根因修復後 `replay`；"
                  "schema_incompatible → `quarantine`")
        conn.close()
        return

    pg = common.connect_pg(retries=3, delay=1)
    with pg.cursor() as cur:
        cur.execute(QUARANTINE_DDL)

    for method, msg in drain(ch):
        reason = classify(msg)
        if action == "quarantine" or reason == "schema_incompatible":
            quarantine_message(pg, msg, reason)
            summary[f"quarantined:{reason}"] += 1
        else:  # replay：poison 移除標記（模擬根因已修復），其餘原樣重放
            msg["poison"] = False
            common.publish(ch, config.vector_queue_for(msg["tenant"]), msg)
            summary[f"replayed:{reason}"] += 1
        ch.basic_ack(method.delivery_tag)

    if not summary:
        print("✅ DLQ 是空的")
    for key, n in sorted(summary.items()):
        print(f"  {key:<40} ×{n}")
    print("（replay 冪等：已入庫的 chunk 不會重複；quarantine 記錄在 c2_dlq_quarantine 表）")
    conn.close()


if __name__ == "__main__":
    main()
