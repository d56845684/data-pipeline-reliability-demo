"""DLQ 哨兵 — 週期性自動分流 DLQ，只把需要人工決策的訊息留在佇列。

自動處置策略（與 RB-04 的決策表一致）：
- transient_or_unknown：自動重放回租戶佇列（每則最多 DLQ_MAX_AUTO_REPLAYS 次，
  以訊息內的 dlq_replays 計數防止無限循環）
- schema_incompatible：自動隔離入 c2_dlq_quarantine（重放必定再失敗）
- poison_content：自動隔離入 c2_dlq_quarantine —— 處置是機械性的
  （內容損毀 → 存證 → 通知租戶重新上傳），後續人工作業走隔離表彙整，不佔 DLQ
- 重放額度用盡的未知故障：留在 DLQ —— 真正需要人工 debug 的殘餘

C2DLQNotEmpty 告警因此升級語意：佇列裡剩下的都是「哨兵處理不了、真正需要人」的訊息。
"""
import json
import os
import time

from prometheus_client import Counter, Gauge, start_http_server

import common
import config
from dlq_tool import QUARANTINE_DDL, classify, quarantine_message

MAX_AUTO_REPLAYS = int(os.getenv("DLQ_MAX_AUTO_REPLAYS", "2"))
INTERVAL = float(os.getenv("SENTINEL_INTERVAL_SECONDS", "30"))

AUTO_REPLAYED = Counter("c2_dlq_auto_replayed_total", "sentinel auto-replays", ["tenant"])
AUTO_QUARANTINED = Counter("c2_dlq_auto_quarantined_total",
                           "sentinel auto-quarantines", ["tenant", "reason"])
PENDING_HUMAN = Gauge("c2_dlq_pending_human",
                      "DLQ messages awaiting human triage (sentinel cannot handle)")


def cycle(ch, pg) -> None:
    """單次巡檢：先全部取出，分流處置，需要人工的整批放回。"""
    stay = []
    while True:
        method, _props, body = ch.basic_get(queue=config.DLQ_QUEUE)
        if method is None:
            break
        try:
            msg = json.loads(body)
        except json.JSONDecodeError:
            msg = {}
        reason = classify(msg)

        if reason in ("schema_incompatible", "poison_content"):
            quarantine_message(pg, msg, reason)
            AUTO_QUARANTINED.labels(msg.get("tenant", "unknown"), reason).inc()
            ch.basic_ack(method.delivery_tag)
            print(f"[sentinel] 自動隔離 {reason}: "
                  f"{msg.get('file_id', '?')}#{msg.get('chunk_idx', '?')}", flush=True)

        elif reason == "transient_or_unknown" and msg.get("dlq_replays", 0) < MAX_AUTO_REPLAYS:
            msg["dlq_replays"] = msg.get("dlq_replays", 0) + 1
            common.publish(ch, config.vector_queue_for(msg["tenant"]), msg)
            AUTO_REPLAYED.labels(msg["tenant"]).inc()
            ch.basic_ack(method.delivery_tag)
            print(f"[sentinel] 自動重放（第 {msg['dlq_replays']} 次）: "
                  f"{msg['file_id']}#{msg['chunk_idx']}", flush=True)

        else:
            # 重放額度用盡的未知故障 → 真正需要人工 debug，留在 DLQ
            stay.append(method)

    for method in stay:
        ch.basic_nack(method.delivery_tag, requeue=True)
    PENDING_HUMAN.set(len(stay))
    if stay:
        print(f"[sentinel] {len(stay)} 則需人工處置（依 RB-04），留在 DLQ", flush=True)


def main() -> None:
    start_http_server(config.METRICS_PORT)
    pg = common.connect_pg()
    with pg.cursor() as cur:
        cur.execute(QUARANTINE_DDL)
    conn, ch = common.connect_rabbit()
    common.declare_topology(ch)
    print(f"[sentinel] 啟動：每 {INTERVAL}s 巡檢 DLQ，"
          f"自動重放上限 {MAX_AUTO_REPLAYS} 次/則", flush=True)

    while True:
        try:
            cycle(ch, pg)
        except Exception as e:   # 單次巡檢失敗不中斷服務
            print(f"[sentinel] cycle error: {e}", flush=True)
        conn.process_data_events(time_limit=INTERVAL)


if __name__ == "__main__":
    main()
