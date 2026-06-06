"""現場演示工具 — 對指定管道強制注入錯誤情境（下一個模擬日生效）。

用法（在 simulator 容器內執行）：
    docker compose exec simulator python inject.py encryption_leak policies
    docker compose exec simulator python inject.py row_drop "*"        # 所有管道
"""
import json
import sys
from pathlib import Path

import config


def main() -> None:
    if len(sys.argv) < 2 or sys.argv[1] not in config.ERROR_SCENARIOS:
        print(f"用法: python inject.py <scenario> [pipeline|*]")
        print(f"可用情境: {', '.join(config.ERROR_SCENARIOS)}")
        print(f"可用管道: {', '.join(config.PIPELINES)}")
        sys.exit(1)

    scenario = sys.argv[1]
    pipeline = sys.argv[2] if len(sys.argv) > 2 else "*"
    if pipeline != "*" and pipeline not in config.PIPELINES:
        print(f"未知管道: {pipeline}")
        sys.exit(1)

    Path(config.FORCE_FILE).write_text(json.dumps({"scenario": scenario, "pipeline": pipeline}))
    print(f"已排程：下一個模擬日對 [{pipeline}] 注入 [{scenario}]")


if __name__ == "__main__":
    main()
