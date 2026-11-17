"""The same Platform with real backends.

    Kinesis Data Streams      instead of stream.Stream
    Redis                     instead of the in-memory realtime layer
    Postgres                  instead of SQLite
    EventBridge Scheduler+SQS instead of polling the due table for delayed work
    Firehose + Lambda + S3    instead of lake.Firehose (AWS runs it, nothing here)
    Athena                    instead of reading local JSONL files
"""
import hashlib
import json
import os
import time
from datetime import datetime, timedelta, timezone

import boto3
import psycopg
import redis

from engagement.config import Config
from engagement.judge import JevJudge, load_api_key
from engagement.store import SCHEMA as BASE_SCHEMA

from .layers import RealtimeStore, RealtimeUnavailable
from .platform import Platform


# ------------------------------------------------------------------ Postgres
class Row:
    """Lets the SQLite-style code read rows by name or by position."""
    __slots__ = ("_names", "_values")

    def __init__(self, names, values):
        self._names, self._values = names, values

    def __getitem__(self, key):
        return self._values[key if isinstance(key, int) else self._names.index(key)]

    def keys(self):
        return list(self._names)


def _row_factory(cursor):
    names = [c.name for c in cursor.description] if cursor.description else []
    return lambda values: Row(names, values)


class PgDb:
    """The three methods the platform uses. The SQL is written to run on both databases."""

    def __init__(self, dsn: str, attempts: int = 30):
        for i in range(attempts):
            try:
                self.conn = psycopg.connect(dsn, autocommit=True, row_factory=_row_factory)
                break
            except psycopg.OperationalError:
                if i == attempts - 1:
                    raise
                time.sleep(1)

    def execute(self, sql: str, params=()):
        return self.conn.execute(sql.replace("?", "%s"), params)

    def executescript(self, script: str) -> None:
        lines = [line.split("--")[0] for line in script.splitlines()]
        for statement in "\n".join(lines).split(";"):
            if statement.strip():
                self.conn.execute(statement)

    def commit(self) -> None:
        pass                    # autocommit; every write is idempotent on its key


# ------------------------------------------------------------------ Kinesis
class KinesisStream:
    def __init__(self, name: str, db: PgDb):
        self.name, self.db = name, db
        self.client = boto3.client("kinesis")
        db.executescript("CREATE TABLE IF NOT EXISTS checkpoints "
                         "(consumer TEXT, shard_id TEXT, sequence TEXT NOT NULL, PRIMARY KEY (consumer, shard_id))")

    def put(self, partition_key: str, record: dict) -> str:
        """Kinesis hashes the partition key to a shard: one user's events stay in order."""
        out = self.client.put_record(StreamName=self.name, PartitionKey=partition_key,
                                     Data=json.dumps(record, ensure_ascii=False).encode())
        return out["ShardId"]

    def consumer(self, name: str) -> "KinesisConsumer":
        return KinesisConsumer(self, name)


class KinesisConsumer:
    """Pulls records and stores its own position, the job KCL does with a DynamoDB table."""

    def __init__(self, stream: KinesisStream, name: str):
        self.stream, self.name = stream, name
        self.last_error, self.behind_ms = None, None

    def run(self, handler) -> int:
        client, db, handled = self.stream.client, self.stream.db, 0
        self.last_error = None
        for shard in client.list_shards(StreamName=self.stream.name)["Shards"]:
            shard_id = shard["ShardId"]
            row = db.execute("SELECT sequence FROM checkpoints WHERE consumer = ? AND shard_id = ?",
                             (self.name, shard_id)).fetchone()
            where = ({"ShardIteratorType": "AFTER_SEQUENCE_NUMBER", "StartingSequenceNumber": row["sequence"]}
                     if row else {"ShardIteratorType": "TRIM_HORIZON"})
            iterator = client.get_shard_iterator(StreamName=self.stream.name, ShardId=shard_id, **where)["ShardIterator"]
            for _ in range(5):
                page = client.get_records(ShardIterator=iterator, Limit=500)
                self.behind_ms = page.get("MillisBehindLatest")
                for item in page["Records"]:
                    try:
                        handler(json.loads(item["Data"]))
                    except Exception as exc:        # stop here; this record comes again next time
                        self.last_error = f"{type(exc).__name__}: {exc}"
                        return handled
                    db.execute(
                        "INSERT INTO checkpoints (consumer, shard_id, sequence) VALUES (?, ?, ?) "
                        "ON CONFLICT (consumer, shard_id) DO UPDATE SET sequence = excluded.sequence",
                        (self.name, shard_id, item["SequenceNumber"]))
                    handled += 1
                iterator = page.get("NextShardIterator")
                if not iterator or (not page["Records"] and not self.behind_ms):
                    break
        return handled

    def lag(self):
        return self.behind_ms


# ------------------------------------------------------------------ Redis
class RedisRealtime(RealtimeStore):
    def __init__(self, url: str):
        super().__init__()
        self.r = redis.Redis.from_url(url, decode_responses=True, socket_timeout=2, socket_connect_timeout=2)

    def _call(self, fn, *args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except redis.RedisError as exc:
            raise RealtimeUnavailable(str(exc)) from exc

    def _check(self) -> None:
        self._call(self.r.ping)

    def _load(self, user_id):
        raw = self._call(self.r.get, f"rt:{user_id}")
        return json.loads(raw) if raw else None

    def _save(self, user_id, state):
        self._call(self.r.set, f"rt:{user_id}", json.dumps(state, ensure_ascii=False), ex=self.ttl)

    def _set_live(self, streamer_id, live):
        self._call(self.r.sadd if live else self.r.srem, "live", streamer_id)

    def live_streamers(self):
        return set(self._call(self.r.smembers, "live"))

    def get_recommendations(self, user_id):
        raw = self._call(self.r.get, f"rec:{user_id}")
        return json.loads(raw) if raw else None

    def set_recommendations(self, user_id, streamers):
        self._call(self.r.set, f"rec:{user_id}", json.dumps(streamers), ex=86400)

    def first_time(self, event_id: str) -> bool:
        """Gateway dedupe. If Redis is down the event is let through: every consumer is idempotent."""
        try:
            return bool(self.r.set(f"seen:{event_id}", 1, nx=True, ex=86400))
        except redis.RedisError:
            return True


# ------------------------------------------------------------------ EventBridge Scheduler
class EventBridgeScheduler:
    """Delayed work lives in AWS, not in a sleeping process. The wake-up arrives on SQS."""

    def __init__(self, group: str, role_arn: str, queue_arn: str, queue_url: str):
        self.group, self.role_arn, self.queue_arn, self.queue_url = group, role_arn, queue_arn, queue_url
        self.scheduler, self.sqs = boto3.client("scheduler"), boto3.client("sqs")

    def schedule(self, trigger_id: str, due_at: float) -> None:
        when = datetime.fromtimestamp(max(due_at, time.time() + 5), timezone.utc)
        try:
            self.scheduler.create_schedule(
                Name="t-" + hashlib.sha1(trigger_id.encode()).hexdigest()[:40], GroupName=self.group,
                ScheduleExpression=f"at({when:%Y-%m-%dT%H:%M:%S})", ScheduleExpressionTimezone="UTC",
                FlexibleTimeWindow={"Mode": "OFF"}, ActionAfterCompletion="DELETE",
                Target={"Arn": self.queue_arn, "RoleArn": self.role_arn,
                        "Input": json.dumps({"type": "release", "trigger_id": trigger_id})})
        except self.scheduler.exceptions.ConflictException:
            pass                # the same trigger was scheduled before: nothing to do

    def poll(self) -> list:
        out = self.sqs.receive_message(QueueUrl=self.queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=0)
        messages = []
        for m in out.get("Messages", []):
            messages.append(json.loads(m["Body"]))
            self.sqs.delete_message(QueueUrl=self.queue_url, ReceiptHandle=m["ReceiptHandle"])
        return messages


# ------------------------------------------------------------------ Athena
def athena_events(workgroup: str, database: str, days: int = 31):
    """Raw de-identified events from the lake. The aggregation is the same Python as the local run."""
    client = boto3.client("athena")
    since = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
    qid = client.start_query_execution(
        QueryString=f"SELECT event_id, user_key, type, ts, payload FROM events WHERE dt >= '{since}'",
        WorkGroup=workgroup, QueryExecutionContext={"Database": database})["QueryExecutionId"]
    while True:
        status = client.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
        if status["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(0.5)
    if status["State"] != "SUCCEEDED":
        raise RuntimeError(f"Athena query {status['State']}: {status.get('StateChangeReason')}")
    first = True
    for page in client.get_paginator("get_query_results").paginate(QueryExecutionId=qid):
        for row in page["ResultSet"]["Rows"]:
            if first:           # header row
                first = False
                continue
            event_id, user_key, type_, ts, payload = (c.get("VarCharValue") for c in row["Data"])
            yield {"event_id": event_id, "user_key": user_key, "type": type_, "ts": float(ts), "payload": payload}


# ------------------------------------------------------------------ wiring
def build_platform() -> tuple[Platform, EventBridgeScheduler]:
    env = os.environ
    db = PgDb(env["DATABASE_URL"])
    db.executescript(BASE_SCHEMA)
    realtime = RedisRealtime(env["REDIS_URL"])
    scheduler = EventBridgeScheduler(env["SCHEDULER_GROUP"], env["SCHEDULER_ROLE_ARN"],
                                     env["TRIGGER_QUEUE_ARN"], env["TRIGGER_QUEUE_URL"])
    key = load_api_key() if env.get("JUDGE", "jev") == "jev" else None
    platform = Platform(
        cfg=Config(buffer_seconds=int(env.get("BUFFER_SECONDS", "600")),
                   quiet_start_hour=int(env.get("QUIET_START_HOUR", "23")),
                   quiet_end_hour=int(env.get("QUIET_END_HOUR", "8"))),
        judge=JevJudge(key) if key else None,
        secret=env["DEID_SECRET"].encode(),
        db=db, stream=KinesisStream(env["KINESIS_STREAM"], db), realtime=realtime, scheduler=scheduler,
        lake_reader=lambda: athena_events(env["ATHENA_WORKGROUP"], env["ATHENA_DATABASE"]),
        first_time=realtime.first_time,
    )
    return platform, scheduler
