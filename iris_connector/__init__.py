"""Spark Structured Streaming source for Elexon's IRIS AMQP service."""

from iris_connector.config import IRISConfig
from iris_connector.source import IRIS_SCHEMA, IRISDataSource, IRISStreamReader

__version__ = "0.2.0"
__all__ = [
    "IRISConfig",
    "IRISDataSource",
    "IRISStreamReader",
    "IRIS_SCHEMA",
    "register",
]


def register(spark) -> None:
    """Register the 'iris' streaming format with the given SparkSession."""
    spark.dataSource.register(IRISDataSource)
