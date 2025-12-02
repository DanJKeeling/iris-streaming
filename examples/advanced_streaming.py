"""
Advanced IRIS Streaming Example

This example demonstrates more advanced usage patterns:
- Custom message parsing based on message content
- Writing to different sinks (Parquet, Delta, console)
- Error handling and monitoring

Databricks Secret Setup (run once via Databricks CLI):
    databricks secrets create-scope --scope iris
    databricks secrets put --scope iris --key iris-client-id
    databricks secrets put --scope iris --key iris-client-secret
"""

from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    from_json,
    col,
    get_json_object,
    avg,
    max as spark_max,
    min as spark_min,
    count,
    to_timestamp,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
)

from iris_connector import IRISConfig, create_iris_stream


class IRISDataProcessor:
    """
    Advanced processor for IRIS streaming data with multiple sinks.
    """
    
    def __init__(self, spark: SparkSession, output_path: str):
        self.spark = spark
        self.output_path = output_path
        self.message_counts = 0
        self.error_counts = {}
    
    def process_batch(self, df: DataFrame, batch_id: int):
        """
        Main batch processing function.
        
        Processes all messages from the IRIS queue.
        """
        if df is None or df.count() == 0:
            return
        
        total_count = df.count()
        self.message_counts += total_count
        
        print(f"\n{'='*50}")
        print(f"📨 Received batch {batch_id}: {total_count} messages")
        print(f"   Timestamp: {datetime.now().isoformat()}")
        print(f"   Total processed: {self.message_counts}")
        
        try:
            # Show sample of messages
            print("\n📄 Sample messages:")
            df.select("body", "received_at").show(5, truncate=80)
            
            # Write all messages to Parquet for later analysis
            output_dir = f"{self.output_path}/messages"
            df.write.mode("append").parquet(output_dir)
            
        except Exception as e:
            print(f"❌ Error processing batch {batch_id}: {e}")
            self.error_counts[batch_id] = str(e)
    
    def print_summary(self):
        """Print processing summary."""
        print("\n" + "="*50)
        print("📊 Processing Summary")
        print("="*50)
        print(f"Total messages processed: {self.message_counts}")
        
        if self.error_counts:
            print(f"\nErrors: {len(self.error_counts)}")


def run_streaming():
    """
    Run the advanced IRIS streaming processor.
    
    Call this function from a Databricks notebook or let it run automatically
    when imported as a script.
    """
    # Configure output path - use DBFS for Databricks persistence
    output_path = "/dbfs/iris_data"
    checkpoint_path = "/dbfs/checkpoints/iris/advanced"
    
    # Get existing Spark session (provided by Databricks)
    spark = SparkSession.builder.getOrCreate()
    
    # Configure IRIS connection with credentials from Databricks secrets
    config = IRISConfig.from_databricks_secrets(
        scope="iris",
        client_id_key="iris-client-id",
        client_secret_key="iris-client-secret",
        # Default queue: iris.5c9f752d-795a-4986-97cc-8a49f5380c02
        max_batch_size=1000,
        prefetch_count=200,
    )
    
    # Create processor
    processor = IRISDataProcessor(spark, output_path)
    
    print("🚀 Starting IRIS Advanced Streaming")
    print(f"📁 Output path: {output_path}")
    print(f"📡 Queue: {config.queue}")
    
    # Create streaming source
    source, trigger_df = create_iris_stream(spark, config)
    
    # Define batch processing
    def process(trigger_batch_df, batch_id):
        messages_df = source.get_batch_df(spark)
        processor.process_batch(messages_df, batch_id)
    
    # Start streaming query
    query = (
        trigger_df.writeStream
        .foreachBatch(process)
        .trigger(processingTime="5 seconds")
        .option("checkpointLocation", checkpoint_path)
        .start()
    )
    
    print("✅ Streaming started.")
    print("   To stop: source.stop() or query.stop()")
    
    return query, source, processor


# Auto-run when executed as script or in notebook
query, source, processor = run_streaming()
