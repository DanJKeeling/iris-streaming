"""Configuration for the IRIS Spark Structured Streaming source."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Mapping


_DEFAULT_NAMESPACE = "elexon-insights-iris.servicebus.windows.net"


@dataclass
class IRISConfig:
    """Resolved connection settings for an Elexon IRIS queue receiver.

    Constructed inside the data source from the options passed to
    `spark.readStream.format("iris").option(...)`. Credentials may be
    supplied directly, via Databricks secrets, or via environment variables.
    """

    fully_qualified_namespace: str
    entity_path: str
    client_id: str
    client_secret: str
    tenant_id: str
    prefetch_count: int = 100
    max_messages_per_trigger: int = 1000
    max_wait_time_seconds: int = 1
    lock_renewal_seconds: int = 300

    @classmethod
    def from_options(cls, options: Mapping[str, str]) -> "IRISConfig":
        opts = {k.lower(): v for k, v in options.items()}

        namespace = (
            opts.get("fully_qualified_namespace")
            or opts.get("namespace")
            or os.getenv("IRIS_NAMESPACE")
            or _DEFAULT_NAMESPACE
        )
        entity_path = opts.get("entity_path") or os.getenv("IRIS_ENTITY_PATH")
        if not entity_path:
            raise ValueError(
                "IRIS source requires an 'entity_path' (queue name) option."
            )

        client_id, client_secret, tenant_id = _resolve_credentials(opts)

        return cls(
            fully_qualified_namespace=namespace,
            entity_path=entity_path,
            client_id=client_id,
            client_secret=client_secret,
            tenant_id=tenant_id,
            prefetch_count=int(opts.get("prefetch_count", 100)),
            max_messages_per_trigger=int(opts.get("max_messages_per_trigger", 1000)),
            max_wait_time_seconds=int(opts.get("max_wait_time_seconds", 1)),
            lock_renewal_seconds=int(opts.get("lock_renewal_seconds", 300)),
        )


def _resolve_credentials(opts: Mapping[str, str]) -> tuple[str, str, str]:
    scope = opts.get("secret_scope")
    if scope:
        get = _dbutils_secret_getter()
        return (
            get(scope, opts.get("client_id_key", "iris-client-id")),
            get(scope, opts.get("client_secret_key", "iris-client-secret")),
            get(scope, opts.get("tenant_id_key", "iris-tenant-id")),
        )

    client_id = opts.get("client_id") or os.getenv("IRIS_CLIENT_ID", "")
    client_secret = opts.get("client_secret") or os.getenv("IRIS_CLIENT_SECRET", "")
    tenant_id = opts.get("tenant_id") or os.getenv("IRIS_TENANT_ID", "")

    if not (client_id and client_secret and tenant_id):
        raise ValueError(
            "IRIS source requires client_id, client_secret, and tenant_id "
            "(supply directly, via 'secret_scope' option, or via "
            "IRIS_CLIENT_ID/IRIS_CLIENT_SECRET/IRIS_TENANT_ID env vars)."
        )
    return client_id, client_secret, tenant_id


def _dbutils_secret_getter():
    from pyspark.dbutils import DBUtils
    from pyspark.sql import SparkSession

    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError(
            "secret_scope option requires an active SparkSession on a Databricks runtime."
        )
    dbutils = DBUtils(spark)

    def _get(scope: str, key: str) -> str:
        return dbutils.secrets.get(scope=scope, key=key)

    return _get
