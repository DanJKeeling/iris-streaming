"""
IRIS PySpark Streaming Connector

A custom PySpark streaming source connector for Elexon's IRIS
(Insights Real-time Information Service) AMQP 1.0 message server.
"""

from iris_connector.config import IRISConfig
from iris_connector.amqp_client import IRISAMQPClient
from iris_connector.spark_source import (
    IRISStreamingSource,
    create_iris_stream,
    IRISMicroBatchReader,
)

__version__ = "0.1.0"
__all__ = [
    "IRISConfig",
    "IRISAMQPClient", 
    "IRISStreamingSource",
    "create_iris_stream",
    "IRISMicroBatchReader",
]

