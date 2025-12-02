"""
PySpark Structured Streaming source for Elexon's IRIS service.

Provides both a custom streaming source implementation and utility functions
for easier integration with PySpark applications.
"""

import json
import logging
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterator, List, Optional, Tuple
from queue import Queue, Empty

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    MapType,
)

from iris_connector.config import IRISConfig
from iris_connector.amqp_client import IRISAMQPClient, IRISMessage


logger = logging.getLogger(__name__)


# Schema for IRIS messages in Spark DataFrames
IRIS_MESSAGE_SCHEMA = StructType([
    StructField("topic", StringType(), nullable=False),
    StructField("body", StringType(), nullable=False),
    StructField("message_id", StringType(), nullable=True),
    StructField("correlation_id", StringType(), nullable=True),
    StructField("timestamp", StringType(), nullable=True),
    StructField("properties", StringType(), nullable=True),
    StructField("received_at", StringType(), nullable=False),
])


@dataclass
class IRISOffset:
    """
    Represents an offset in the IRIS message stream.
    
    For AMQP, we use a monotonically increasing sequence number
    combined with timestamp for tracking progress.
    """
    sequence: int
    timestamp: str
    
    def to_json(self) -> str:
        """Serialize offset to JSON."""
        return json.dumps({
            "sequence": self.sequence,
            "timestamp": self.timestamp,
        })
    
    @classmethod
    def from_json(cls, json_str: str) -> "IRISOffset":
        """Deserialize offset from JSON."""
        data = json.loads(json_str)
        return cls(
            sequence=data["sequence"],
            timestamp=data["timestamp"],
        )
    
    @classmethod
    def initial(cls) -> "IRISOffset":
        """Create an initial offset."""
        return cls(sequence=0, timestamp=datetime.utcnow().isoformat())


class IRISMicroBatchReader:
    """
    Micro-batch reader for IRIS streaming data.
    
    This class manages the connection to IRIS and provides batches
    of messages for Spark Structured Streaming processing.
    """
    
    def __init__(self, config: IRISConfig):
        """
        Initialize the micro-batch reader.
        
        Args:
            config: IRIS connection configuration
        """
        self.config = config
        self._client: Optional[IRISAMQPClient] = None
        self._current_offset = IRISOffset.initial()
        self._message_buffer: List[IRISMessage] = []
        self._lock = threading.Lock()
        self._started = False
    
    def start(self) -> bool:
        """
        Start the reader and connect to IRIS.
        
        Returns:
            True if started successfully
        """
        if self._started:
            return True
        
        logger.info("Starting IRIS micro-batch reader")
        self._client = IRISAMQPClient(self.config)
        
        if self._client.start(timeout=self.config.connection_timeout):
            self._started = True
            return True
        else:
            logger.error("Failed to start IRIS client")
            return False
    
    def stop(self):
        """Stop the reader and disconnect from IRIS."""
        if self._client:
            self._client.stop()
            self._client = None
        self._started = False
        logger.info("IRIS micro-batch reader stopped")
    
    def get_latest_offset(self) -> IRISOffset:
        """
        Get the latest available offset.
        
        This fetches any pending messages and updates the offset.
        """
        if not self._started or not self._client:
            return self._current_offset
        
        # Fetch pending messages into buffer
        with self._lock:
            messages = self._client.get_batch(
                max_size=self.config.max_batch_size,
                timeout=0.1,
            )
            self._message_buffer.extend(messages)
            
            if messages:
                self._current_offset = IRISOffset(
                    sequence=self._current_offset.sequence + len(messages),
                    timestamp=datetime.utcnow().isoformat(),
                )
        
        return self._current_offset
    
    def get_batch(
        self,
        start_offset: Optional[IRISOffset],
        end_offset: IRISOffset,
    ) -> List[Dict[str, Any]]:
        """
        Get a batch of messages between offsets.
        
        Args:
            start_offset: Starting offset (exclusive)
            end_offset: Ending offset (inclusive)
            
        Returns:
            List of message dictionaries
        """
        with self._lock:
            # Calculate how many messages to return
            start_seq = start_offset.sequence if start_offset else 0
            end_seq = end_offset.sequence
            count = end_seq - start_seq
            
            if count <= 0:
                return []
            
            # Get messages from buffer
            messages = self._message_buffer[:count]
            self._message_buffer = self._message_buffer[count:]
            
            # Convert to dictionaries
            return [msg.to_dict() for msg in messages]
    
    def commit(self, offset: IRISOffset):
        """
        Commit an offset (acknowledge messages have been processed).
        
        Args:
            offset: The offset to commit
        """
        # For AMQP with auto-ack, messages are acknowledged on receive
        # This is mainly for bookkeeping
        logger.debug(f"Committed offset: {offset.sequence}")


class IRISStreamingSource:
    """
    High-level streaming source for IRIS that integrates with PySpark.
    
    This provides a simple interface for creating streaming DataFrames
    from IRIS message data using a foreachBatch pattern.
    
    Example:
        ```python
        source = IRISStreamingSource(config)
        source.start()
        
        # Process batches with foreachBatch
        def process_batch(df, batch_id):
            df.show()
        
        query = source.create_streaming_query(
            spark,
            process_batch,
            checkpoint_location="/tmp/checkpoint",
        )
        query.awaitTermination()
        ```
    """
    
    def __init__(self, config: IRISConfig):
        """
        Initialize the streaming source.
        
        Args:
            config: IRIS connection configuration
        """
        self.config = config
        self._reader = IRISMicroBatchReader(config)
        self._running = False
        self._batch_queue: Queue = Queue()
        self._poll_thread: Optional[threading.Thread] = None
    
    def start(self) -> bool:
        """
        Start the streaming source.
        
        Returns:
            True if started successfully
        """
        if self._running:
            return True
        
        if not self._reader.start():
            return False
        
        self._running = True
        
        # Start background polling thread
        self._poll_thread = threading.Thread(
            target=self._poll_messages,
            daemon=True,
        )
        self._poll_thread.start()
        
        logger.info("IRIS streaming source started")
        return True
    
    def stop(self):
        """Stop the streaming source."""
        self._running = False
        
        if self._poll_thread and self._poll_thread.is_alive():
            self._poll_thread.join(timeout=5.0)
        
        self._reader.stop()
        logger.info("IRIS streaming source stopped")
    
    def _poll_messages(self):
        """Background thread that polls for messages."""
        while self._running:
            try:
                # Get latest offset (this fetches messages)
                offset = self._reader.get_latest_offset()
                time.sleep(0.1)  # Small delay to prevent tight loop
            except Exception as e:
                logger.error(f"Error polling messages: {e}")
                time.sleep(1.0)
    
    def get_batch_df(self, spark: SparkSession) -> Optional[DataFrame]:
        """
        Get a batch of messages as a DataFrame.
        
        Args:
            spark: SparkSession
            
        Returns:
            DataFrame with IRIS messages, or None if no messages available
        """
        # Get current offset and fetch batch
        end_offset = self._reader.get_latest_offset()
        batch = self._reader.get_batch(None, end_offset)
        
        if not batch:
            return None
        
        # Create DataFrame
        return spark.createDataFrame(batch, schema=IRIS_MESSAGE_SCHEMA)
    
    def create_rate_source_stream(
        self,
        spark: SparkSession,
        rows_per_second: int = 1,
    ) -> DataFrame:
        """
        Create a rate source that triggers IRIS message fetching.
        
        This uses Spark's built-in rate source as a trigger mechanism
        to periodically fetch IRIS messages.
        
        Args:
            spark: SparkSession
            rows_per_second: Trigger rate
            
        Returns:
            Streaming DataFrame
        """
        return (
            spark.readStream
            .format("rate")
            .option("rowsPerSecond", rows_per_second)
            .load()
        )


def create_iris_stream(
    spark: SparkSession,
    config: Optional[IRISConfig] = None,
    **kwargs,
) -> Tuple[IRISStreamingSource, DataFrame]:
    """
    Create an IRIS streaming source and trigger DataFrame.
    
    This is a convenience function that sets up the IRIS streaming
    infrastructure and returns both the source (for lifecycle management)
    and a rate-based trigger DataFrame.
    
    Args:
        spark: SparkSession
        config: IRIS configuration (if None, loads from environment)
        **kwargs: Additional configuration overrides
        
    Returns:
        Tuple of (IRISStreamingSource, trigger DataFrame)
        
    Example:
        ```python
        config = IRISConfig.from_databricks_secrets(
            scope="iris",
            topics=["bmrs/FREQ", "bmrs/INDDEM"],
        )
        source, trigger_df = create_iris_stream(spark, config)
        
        # Process with foreachBatch
        def process(batch_df, batch_id):
            messages_df = source.get_batch_df(spark)
            if messages_df:
                messages_df.show()
        
        query = (
            trigger_df.writeStream
            .foreachBatch(process)
            .option("checkpointLocation", "/dbfs/checkpoints/iris")
            .start()
        )
        
        try:
            query.awaitTermination()
        finally:
            source.stop()
        ```
    """
    # Create configuration
    if config is None:
        config = IRISConfig.from_env()
    
    # Apply any overrides
    for key, value in kwargs.items():
        if hasattr(config, key):
            setattr(config, key, value)
    
    config.validate()
    
    # Create and start source
    source = IRISStreamingSource(config)
    if not source.start():
        raise RuntimeError("Failed to start IRIS streaming source")
    
    # Create trigger DataFrame
    trigger_df = source.create_rate_source_stream(spark)
    
    return source, trigger_df


class IRISStreamProcessor:
    """
    Complete streaming processor for IRIS data with PySpark.
    
    This class provides a higher-level abstraction that handles the
    complete streaming pipeline including connection management,
    message parsing, and DataFrame creation.
    
    Example:
        ```python
        config = IRISConfig.from_databricks_secrets(
            scope="iris",
            topics=["bmrs/FREQ"],
        )
        
        processor = IRISStreamProcessor(spark, config=config)
        
        # Define processing function
        def handle_messages(df: DataFrame, batch_id: int):
            # Parse JSON body and process
            parsed = df.selectExpr(
                "topic",
                "from_json(body, 'struct<...>') as data",
                "received_at",
            )
            parsed.show()
        
        # Start processing
        processor.start(handle_messages)
        processor.await_termination()
        ```
    """
    
    def __init__(
        self,
        spark: SparkSession,
        config: Optional[IRISConfig] = None,
        checkpoint_location: Optional[str] = None,
    ):
        """
        Initialize the stream processor.
        
        Args:
            spark: SparkSession
            config: IRIS configuration
            checkpoint_location: Spark checkpoint location
        """
        self.spark = spark
        self.config = config or IRISConfig.from_env()
        self.checkpoint_location = (
            checkpoint_location or self.config.checkpoint_location
        )
        
        self._source: Optional[IRISStreamingSource] = None
        self._query = None
    
    def start(
        self,
        process_fn,
        trigger_interval: str = "1 second",
        output_mode: str = "append",
    ):
        """
        Start the streaming processor.
        
        Args:
            process_fn: Function to process each batch (df, batch_id) -> None
            trigger_interval: Spark trigger interval (e.g., "1 second", "5 seconds")
            output_mode: Spark output mode
            
        Returns:
            The streaming query
        """
        # Create source
        self._source, trigger_df = create_iris_stream(
            self.spark,
            self.config,
        )
        
        # Wrapper to fetch and process IRIS messages
        def process_batch(trigger_batch_df, batch_id):
            messages_df = self._source.get_batch_df(self.spark)
            if messages_df is not None and messages_df.count() > 0:
                process_fn(messages_df, batch_id)
        
        # Start streaming query
        self._query = (
            trigger_df.writeStream
            .foreachBatch(process_batch)
            .trigger(processingTime=trigger_interval)
            .option("checkpointLocation", self.checkpoint_location)
            .start()
        )
        
        logger.info(f"Started IRIS stream processor with trigger: {trigger_interval}")
        return self._query
    
    def stop(self):
        """Stop the streaming processor."""
        if self._query:
            self._query.stop()
            self._query = None
        
        if self._source:
            self._source.stop()
            self._source = None
        
        logger.info("IRIS stream processor stopped")
    
    def await_termination(self, timeout: Optional[float] = None):
        """
        Wait for the streaming query to terminate.
        
        Args:
            timeout: Optional timeout in seconds
        """
        if self._query:
            self._query.awaitTermination(timeout)
    
    def is_active(self) -> bool:
        """Check if the processor is active."""
        return self._query is not None and self._query.isActive
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.stop()
        return False

