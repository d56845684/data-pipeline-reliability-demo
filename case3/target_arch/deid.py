"""De-identification, shared by the local lake writer and the Firehose transform Lambda.

Standard library only: this one file is what gets deployed as the Lambda.
"""
import base64
import hashlib
import hmac
import json
import os

PII_FIELDS = {"ip", "device_id", "phone", "email"}


def pseudonym(user_id: str, secret: bytes) -> str:
    """The data lake only ever sees this. The secret stays on the operational side."""
    return hmac.new(secret, user_id.encode(), hashlib.sha256).hexdigest()[:16]


def deidentify(record: dict, secret: bytes) -> dict:
    """Runs before anything is written, so no copy of the raw data exists downstream."""
    payload = {k: v for k, v in record["payload"].items() if k not in PII_FIELDS}
    if "text" in payload:                       # chat stays out of the lake; keep only its size
        payload["text_length"] = len(payload.pop("text"))
    return {"event_id": record["event_id"], "user_key": pseudonym(record["user_id"], secret),
            "type": record["type"], "ts": record["ts"],
            "payload": json.dumps(payload, ensure_ascii=False, sort_keys=True)}


def handler(event, context):
    """Firehose data transformation: one de-identified JSON line per record."""
    secret = os.environ["DEID_SECRET"].encode()
    out = []
    for item in event["records"]:
        try:
            row = deidentify(json.loads(base64.b64decode(item["data"])), secret)
            data = base64.b64encode((json.dumps(row, ensure_ascii=False) + "\n").encode()).decode()
            out.append({"recordId": item["recordId"], "result": "Ok", "data": data})
        except Exception:
            out.append({"recordId": item["recordId"], "result": "ProcessingFailed", "data": item["data"]})
    return {"records": out}
