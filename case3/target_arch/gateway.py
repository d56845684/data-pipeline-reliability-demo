"""Event Gateway: three deterministic steps, then the stream. No model anywhere."""

# event type -> payload fields that must be present
SCHEMAS = {
    "login": (),
    "app_open": (),
    "enter_room": ("streamer_id",),
    "leave_room": ("streamer_id", "watch_seconds"),
    "follow": ("streamer_id",),
    "chat": ("streamer_id", "text"),
    "click_recharge": (),
    "cancel_recharge": ("step",),
    "recharge_success": ("amount",),
    "send_gift": ("streamer_id", "amount"),
    "stream_start": ("streamer_id",),
    "stream_end": ("streamer_id",),
    "push_opt_out": (),
    "push_sent": ("decision_id",),      # feedback from the executor, closes the loop
}


class Gateway:
    def __init__(self, stream, first_time=None):
        self.stream = stream
        self.seen = set()
        # first_time(event_id) -> bool. Default is in memory; with Redis it is SET NX with a TTL.
        self.first_time = first_time or self._first_time_in_memory
        self.quarantine = []

    def _first_time_in_memory(self, event_id: str) -> bool:
        if event_id in self.seen:
            return False
        self.seen.add(event_id)
        return True

    def ingest(self, event: dict, now: float) -> str:
        problem = self._validate(event)
        if problem:
            self.quarantine.append({"event": event, "problem": problem})
            return "quarantined"
        if not self.first_time(event["event_id"]):
            return "duplicate"
        record = {
            "event_id": event["event_id"], "user_id": event["user_id"], "type": event["type"],
            "ts": now,                  # server time, the device clock is not trusted
            "payload": event.get("payload") or {},
        }
        self.stream.put(event["user_id"], record)
        return "accepted"

    @staticmethod
    def _validate(event) -> str | None:
        for key in ("event_id", "user_id", "type"):
            if not isinstance(event.get(key), str) or not event[key]:
                return f"missing {key}"
        if event["type"] not in SCHEMAS:
            return f"unknown type {event['type']}"
        payload = event.get("payload") or {}
        if not isinstance(payload, dict):
            return "payload is not an object"
        missing = [f for f in SCHEMAS[event["type"]] if f not in payload]
        return f"missing payload field {missing[0]}" if missing else None
