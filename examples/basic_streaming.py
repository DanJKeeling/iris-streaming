"""
Basic IRIS Streaming Example for Databricks

This example demonstrates how to connect to Elexon's IRIS AMQP service
and stream messages into PySpark for processing.

Databricks Setup:
    1. Install cluster library: python-qpid-proton (via PyPI)
    2. Set up secrets (run once via Databricks CLI):
        databricks secrets create-scope --scope iris
        databricks secrets put --scope iris --key iris-client-id
        databricks secrets put --scope iris --key iris-client-secret

Usage:
    Copy this code into a Databricks notebook and run cell by cell,
    or run directly as a Python script via Databricks job.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

from iris_connector import IRISConfig, IRISStreamProcessor, IRISTopics


# Get existing Spark session (provided by Databricks) or create one for local testing
spark = SparkSession.builder.getOrCreate()

# Configure IRIS connection with credentials from Databricks secrets
# Secrets are retrieved from scope "iris" with keys "iris-client-id" and "iris-client-secret"
config = IRISConfig.from_databricks_secrets(
    scope="iris",
    client_id_key="iris-client-id",
    client_secret_key="iris-client-secret",
    topics=[
        IRISTopics.FREQ,           # System Frequency
        IRISTopics.INDDEM,         # Indicated Demand
        IRISTopics.INDGEN,         # Indicated Generation
    ],
    use_tls=True,
    max_batch_size=500,
    # Use DBFS for checkpoint persistence across cluster restarts
    checkpoint_location="/dbfs/checkpoints/iris/basic",
)


# Define how to process each batch of messages
def process_messages(df, batch_id):
    """Process each micro-batch of IRIS messages."""
    print(f"\n=== Batch {batch_id} ===")
    print(f"Received {df.count()} messages")
    
    # Show raw messages
    df.select("topic", "body", "received_at").show(truncate=50)
    
    # Parse FREQ (System Frequency) messages as an example
    freq_schema = StructType([
        StructField("settlementDate", StringType()),
        StructField("settlementPeriod", StringType()),
        StructField("frequency", DoubleType()),
        StructField("measurementTimestamp", StringType()),
    ])
    
    freq_messages = (
        df.filter(col("topic") == IRISTopics.FREQ)
        .withColumn("parsed", from_json(col("body"), freq_schema))
        .select("topic", "parsed.*", "received_at")
    )
    
    if freq_messages.count() > 0:
        print("\nParsed Frequency Data:")
        freq_messages.show()


# Create the processor
processor = IRISStreamProcessor(
    spark,
    config=config,
    checkpoint_location="/dbfs/checkpoints/iris/basic",
)

print("Starting IRIS streaming...")
print(f"Subscribing to topics: {config.topics}")

# Start processing with a 2-second trigger interval
query = processor.start(
    process_fn=process_messages,
    trigger_interval="2 seconds",
)

# In Databricks notebook, you can stop the stream with:
#   processor.stop()
# 
# Or let it run and monitor via Spark UI

