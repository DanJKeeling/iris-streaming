"""Configuration for the IRIS Spark Structured Streaming source."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Mapping


@dataclass
class IRISConfig:
    """Resolved connection settings for an Elexon IRIS queue receiver.

    Constructed inside the data source from the options passed to
    `spark.readStream.format("iris").option(...)`. Credentials must be
    supplied as plain strings — the data source runs in an isolated
    subprocess where `dbutils` is unavailable, so resolve Databricks
    secrets in the calling notebook and pass them as `option(...)` values.
    """

    fully_qualified_namespace: str
    entity_path: str
    client_id: str
    client_secret: str
    tenant_id: str
    prefetch_count: int = 100
    max_messages_per_trigger: int = 1000
    max_wait_time_seconds: float = 0.1
    lock_renewal_seconds: int = 300

    @classmethod
    def from_options(cls, options: Mapping[str, str]) -> "IRISConfig":
        opts = {k.lower(): v for k, v in options.items()}

        namespace = (
            opts.get("fully_qualified_namespace")
            or opts.get("namespace")
            or os.getenv("IRIS_NAMESPACE")
        )
        if not namespace:
            raise ValueError(
                "IRIS source requires a 'fully_qualified_namespace' option "
                "(e.g. '<your-iris-namespace>.servicebus.windows.net')."
            )
        entity_path = opts.get("entity_path") or os.getenv("IRIS_ENTITY_PATH")
        if not entity_path:
            raise ValueError(
                "IRIS source requires an 'entity_path' (queue name) option."
            )

        client_id = opts.get("client_id") or os.getenv("IRIS_CLIENT_ID", "")
        client_secret = opts.get("client_secret") or os.getenv("IRIS_CLIENT_SECRET", "")
        tenant_id = opts.get("tenant_id") or os.getenv("IRIS_TENANT_ID", "")

        if not (client_id and client_secret and tenant_id):
            raise ValueError(
                "IRIS source requires client_id, client_secret, and tenant_id options. "
                "On Databricks, resolve via dbutils.secrets.get() in the calling "
                "notebook and pass the values as .option(...) — the data source "
                "subprocess cannot access dbutils directly."
            )

        return cls(
            fully_qualified_namespace=namespace,
            entity_path=entity_path,
            client_id=client_id,
            client_secret=client_secret,
            tenant_id=tenant_id,
            prefetch_count=int(opts.get("prefetch_count", 100)),
            max_messages_per_trigger=int(opts.get("max_messages_per_trigger", 1000)),
            max_wait_time_seconds=float(opts.get("max_wait_time_seconds", 0.1)),
            lock_renewal_seconds=int(opts.get("lock_renewal_seconds", 300)),
        )
