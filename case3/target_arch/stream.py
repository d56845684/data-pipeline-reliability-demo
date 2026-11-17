"""Stand-in for Kinesis Data Streams: shards, a partition key, and per-consumer checkpoints."""
import zlib


class Stream:
    def __init__(self, shards: int = 4):
        self.shards = [[] for _ in range(shards)]

    def put(self, partition_key: str, record: dict) -> int:
        """Same key -> same shard, so one user's events stay in order."""
        shard = zlib.crc32(partition_key.encode()) % len(self.shards)
        self.shards[shard].append(record)
        return shard

    def consumer(self, name: str) -> "Consumer":
        return Consumer(self, name)


class Consumer:
    """Each consumer keeps its own position, so a slow or broken one does not hold back the others.

    The checkpoint moves only after the handler returns. A failure stops that shard and the
    same record is delivered again next time: at-least-once, so handlers must be idempotent.
    """

    def __init__(self, stream: Stream, name: str):
        self.stream, self.name = stream, name
        self.checkpoint = [0] * len(stream.shards)
        self.last_error = None

    def run(self, handler) -> int:
        handled = 0
        self.last_error = None
        for i, shard in enumerate(self.stream.shards):
            while self.checkpoint[i] < len(shard):
                try:
                    handler(shard[self.checkpoint[i]])
                except Exception as exc:
                    self.last_error = f"{type(exc).__name__}: {exc}"
                    break
                self.checkpoint[i] += 1
                handled += 1
        return handled

    def lag(self) -> int:
        return sum(len(s) - c for s, c in zip(self.stream.shards, self.checkpoint))
