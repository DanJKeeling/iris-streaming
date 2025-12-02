"""
Basic IRIS Streaming Example

This example demonstrates how to connect to Elexon's IRIS AMQP service
and stream messages into PySpark for processing.

Databricks Secret Setup (run once via Databricks CLI):
    databricks secrets create-scope --scope iris
    databricks secrets put --scope iris --key iris-username
    databricks secrets put --scope iris --key iris-password
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType

from iris_connector import IRISConfig, IRISStreamProcessor, IRISTopics


def main():
    # Create Spark session
    spark = (
        SparkSession.builder
        .appName("IRIS Basic Streaming")
        .config("spark.sql.streaming.checkpointLocation", "/tmp/iris_checkpoint")
        .getOrCreate()
    )
    
    spark.sparkContext.setLogLevel("WARN")
    
    # Configure IRIS connection with credentials from Databricks secrets
    # Secrets are retrieved from scope "iris" with keys "iris-username" and "iris-password"
    config = IRISConfig.from_databricks_secrets(
        scope="iris",
        username_key="iris-username",
        password_key="iris-password",
        topics=[
            IRISTopics.FREQ,           # System Frequency
            IRISTopics.INDDEM,         # Indicated Demand
            IRISTopics.INDGEN,         # Indicated Generation
        ],
        use_tls=True,
        max_batch_size=500,
    )
    
    # Define how to process each batch of messages
    def process_messages(df, batch_id):
        print(f"\n=== Batch {batch_id} ===")
        print(f"Received {df.count()} messages")
        
        # Show raw messages
        df.select("topic", "body", "received_at").show(truncate=50)
        
        # You can parse the JSON body based on the topic
        # For example, for FREQ (System Frequency) messages:
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
    
    # Create and start the processor
    processor = IRISStreamProcessor(
        spark,
        config=config,
        checkpoint_location="/tmp/iris_basic_checkpoint",
    )
    
    print("Starting IRIS streaming...")
    print(f"Subscribing to topics: {config.topics}")
    
    try:
        # Start processing with a 2-second trigger interval
        processor.start(
            process_fn=process_messages,
            trigger_interval="2 seconds",
        )
        
        # Wait for termination (Ctrl+C to stop)
        processor.await_termination()
        
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        processor.stop()
        spark.stop()


if __name__ == "__main__":
    main()

