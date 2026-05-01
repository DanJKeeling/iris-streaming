"""Spark Structured Streaming source for Elexon's IRIS AMQP service.

Implements the Python DataSource API (DBR 15.x+ / Spark 4.0+) using
`SimpleDataSourceStreamReader`. The reader runs on the driver, opens a
single Azure Service Bus queue receiver in peek-lock mode, and acks
messages only when Spark calls `commit()` after the batch is durably
written. This gives at-least-once semantics.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Iterator

from pyspark.sql.datasource import DataSource, SimpleDataSourceStreamReader
from pyspark.sql.types import (
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from iris_connector.config import IRISConfig

logger = logging.getLogger(__name__)
logging.getLogger("azure").setLevel(logging.WARNING)


IRIS_SCHEMA = StructType([
    StructField("dataset", StringType(), nullable=True),
    StructField("body", StringType(), nullable=False),
    StructField("message_id", StringType(), nullable=True),
    StructField("correlation_id", StringType(), nullable=True),
    StructField("enqueued_time", TimestampType(), nullable=True),
    StructField("properties", MapType(StringType(), StringType()), nullable=True),
    StructField("received_at", TimestampType(), nullable=False),
])


class IRISDataSource(DataSource):
    @classmethod
    def name(cls) -> str:
        return "iris"

    def schema(self) -> StructType:
        return IRIS_SCHEMA

    def simpleStreamReader(self, schema: StructType) -> "IRISStreamReader":
        return IRISStreamReader(IRISConfig.from_options(self.options))


class IRISStreamReader(SimpleDataSourceStreamReader):
    """Driver-side micro-batch reader for an IRIS queue.

    Offsets are `{"seq": <int>}` — a monotonic counter assigned as messages
    arrive. Pending message+row pairs are evicted on commit, so replay via
    `readBetweenOffsets` only works for un-committed offsets (in-process
    re-execution). Across driver restarts, un-acked messages are
    redelivered by Service Bus after the lock expires.
    """

    def __init__(self, config: IRISConfig):
        self._config = config
        self._credential = None
        self._client = None
        self._receiver = None
        self._renewer = None
        self._next_seq = 0
        self._pending: dict[int, tuple[Any, tuple]] = {}

    def initialOffset(self) -> dict:
        return {"seq": 0}

    def read(self, start: dict) -> tuple[Iterator[tuple], dict]:
        start_seq = int(start.get("seq", 0))
        if start_seq < self._next_seq:
            return self._cached_iter(start_seq, self._next_seq), {"seq": self._next_seq}

        from azure.servicebus.exceptions import ServiceBusError

        self._ensure_open()
        rows: list[tuple] = []
        try:
            batch = self._receiver.receive_messages(
                max_message_count=self._config.max_messages_per_trigger,
                max_wait_time=self._config.max_wait_time_seconds,
            )
        except ServiceBusError:
            logger.exception("Service Bus receive failed; resetting receiver")
            self._reset_receiver()
            raise

        for msg in batch:
            self._renewer.register(self._receiver, msg)
            seq = self._next_seq
            self._next_seq += 1
            row = _to_row(msg)
            self._pending[seq] = (msg, row)
            rows.append(row)

        return iter(rows), {"seq": self._next_seq}

    def readBetweenOffsets(self, start: dict, end: dict) -> Iterator[tuple]:
        return self._cached_iter(int(start.get("seq", 0)), int(end.get("seq", 0)))

    def commit(self, end: dict) -> None:
        from azure.servicebus.exceptions import ServiceBusError

        end_seq = int(end.get("seq", 0))
        if not self._receiver:
            return
        for seq in [s for s in list(self._pending.keys()) if s < end_seq]:
            msg, _ = self._pending.pop(seq)
            try:
                self._receiver.complete_message(msg)
            except ServiceBusError:
                logger.exception("complete_message failed for seq %d (lock may have expired)", seq)

    def _cached_iter(self, start_seq: int, end_seq: int) -> Iterator[tuple]:
        for seq in range(start_seq, end_seq):
            entry = self._pending.get(seq)
            if entry is not None:
                yield entry[1]
            else:
                logger.warning(
                    "Replay requested for seq %d after commit/eviction; row dropped.",
                    seq,
                )

    def _ensure_open(self) -> None:
        if self._receiver is not None:
            return
        from azure.identity import ClientSecretCredential
        from azure.servicebus import AutoLockRenewer, ServiceBusClient

        if self._credential is None:
            self._credential = ClientSecretCredential(
                tenant_id=self._config.tenant_id,
                client_id=self._config.client_id,
                client_secret=self._config.client_secret,
            )
        self._client = ServiceBusClient(
            fully_qualified_namespace=self._config.fully_qualified_namespace,
            credential=self._credential,
        )
        self._receiver = self._client.get_queue_receiver(
            queue_name=self._config.entity_path,
            prefetch_count=self._config.prefetch_count,
        )
        self._renewer = AutoLockRenewer(
            max_lock_renewal_duration=self._config.lock_renewal_seconds,
        )

    def _reset_receiver(self) -> None:
        for closer in (self._renewer, self._receiver, self._client):
            if closer is not None:
                try:
                    closer.close()
                except Exception:
                    logger.debug("Error closing %r", closer, exc_info=True)
        self._renewer = self._receiver = self._client = None


def _to_row(msg) -> tuple:
    return (
        msg.subject,
        b"".join(msg.body).decode("utf-8", errors="replace"),
        msg.message_id,
        msg.correlation_id,
        msg.enqueued_time_utc,
        _str_props(msg.application_properties),
        datetime.now(timezone.utc),
    )


def _str_props(props) -> dict[str, str] | None:
    if not props:
        return None
    return {_to_str(k): _to_str(v) for k, v in props.items()}


def _to_str(value) -> str:
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).decode("utf-8", errors="replace")
    return str(value)
