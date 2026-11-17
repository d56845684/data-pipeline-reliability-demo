"""The asynchronous side: consume the stream, take wake-ups from the scheduler, decide, send."""
import json
import time

from .aws import build_platform


def main() -> None:
    platform, scheduler = build_platform()
    print("worker started", flush=True)
    while True:
        try:
            now = time.time()
            for message in scheduler.poll():
                if message.get("type") == "release":
                    platform.release(message["trigger_id"])
                    print(json.dumps({"scheduler": "release", "trigger_id": message["trigger_id"]}), flush=True)
                elif message.get("type") == "churn_scan":
                    users = platform.run_batch(now)
                    print(json.dumps({"scheduler": "churn_scan", "users_in_recent_layer": users,
                                      "high_risk": platform.schedule_churn_scan(now)}), flush=True)
            result = platform.tick(now)
            for d in result["decided"]:
                print(json.dumps({"decided": {k: d[k] for k in ("user_id", "scenario", "reason", "arm", "action")},
                                  "ev": d["scores"].get("ev"), "judge": d["judge"]}, ensure_ascii=False), flush=True)
            for s in result["sent"]:
                print(json.dumps({"sent": s}), flush=True)
            if platform.context_builder.last_error:
                print(json.dumps({"context_builder": platform.context_builder.last_error}), flush=True)
        except Exception as exc:        # keep the loop alive; everything it does is safe to repeat
            print(json.dumps({"error": f"{type(exc).__name__}: {exc}"[:300]}), flush=True)
        time.sleep(2)


if __name__ == "__main__":
    main()
