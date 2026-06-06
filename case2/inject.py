"""Case 2 事故注入工具。

用法（任一 case2 容器內執行）：
    docker compose exec case2-uploader python inject.py burst             # megacorp 100 份大檔
    docker compose exec case2-uploader python inject.py burst 200         # 自訂份數
    docker compose exec case2-uploader python inject.py poison            # 毒訊息 -> DLQ
    docker compose exec case2-uploader python inject.py duplicate         # 重複投遞 -> 冪等跳過
"""
import random
import sys
import time
import uuid

import common
import config


def main() -> None:
    if len(sys.argv) < 2 or sys.argv[1] not in ("burst", "poison", "duplicate"):
        print(__doc__)
        sys.exit(1)
    scenario = sys.argv[1]
    rng = random.Random()
    conn, ch = common.connect_rabbit(retries=3, delay=1)
    common.declare_topology(ch)

    if scenario == "burst":
        n_files = int(sys.argv[2]) if len(sys.argv) > 2 else 100
        tenant = sys.argv[3] if len(sys.argv) > 3 else config.BIG_TENANT
        total_chunks = 0
        for _ in range(n_files):
            n_chunks = rng.randint(40, 80)   # 大檔：切割後 40-80 chunks
            total_chunks += n_chunks
            common.publish(ch, config.UPLOAD_QUEUE, {
                "file_id": f"{tenant}-{uuid.uuid4().hex[:12]}",
                "tenant": tenant, "n_chunks": n_chunks,
                "upload_ts": time.time(), "poison": False,
            })
        print(f"💥 burst：{tenant} 一次上傳 {n_files} 份大檔（將展開為 ~{total_chunks} 個 chunk 任務）")

    elif scenario == "poison":
        tenant = sys.argv[2] if len(sys.argv) > 2 else rng.choice(config.TENANTS)
        common.publish(ch, config.UPLOAD_QUEUE, {
            "file_id": f"{tenant}-poison-{uuid.uuid4().hex[:8]}",
            "tenant": tenant, "n_chunks": 5,
            "upload_ts": time.time(), "poison": True,
        })
        print(f"☠️  poison：{tenant} 上傳了一份會讓向量化失敗的損毀檔 → 將進 DLQ")

    elif scenario == "duplicate":
        tenant = sys.argv[2] if len(sys.argv) > 2 else rng.choice(config.TENANTS)
        msg = {
            "file_id": f"{tenant}-dup-{uuid.uuid4().hex[:8]}",
            "tenant": tenant, "n_chunks": 6,
            "upload_ts": time.time(), "poison": False,
        }
        common.publish(ch, config.UPLOAD_QUEUE, msg)
        common.publish(ch, config.UPLOAD_QUEUE, msg)   # 模擬上游重送
        print(f"♊ duplicate：{tenant} 同一檔案被投遞兩次 → 冪等鍵將跳過第二次")

    conn.close()


if __name__ == "__main__":
    main()
