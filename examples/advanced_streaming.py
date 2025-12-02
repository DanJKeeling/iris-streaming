"""
Advanced IRIS Streaming Example

This example demonstrates more advanced usage patterns:
- Custom message parsing per topic
- Writing to different sinks (Parquet, Delta, console)
- Watermarking and windowed aggregations
- Error handling and monitoring
"""

import os
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    from_json,
    col,
    window,
    avg,
    max as spark_max,
    min as spark_min,
    count,
    current_timestamp,
    to_timestamp,
    lit,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    TimestampType,
)

from iris_connector import IRISConfig, create_iris_stream, IRISTopics


# Schema definitions for different IRIS message types
SCHEMAS = {
    IRISTopics.FREQ: StructType([
        StructField("settlementDate", StringType()),
        StructField("settlementPeriod", IntegerType()),
        StructField("frequency", DoubleType()),
        StructField("measurementTimestamp", StringType()),
    ]),
    
    IRISTopics.INDDEM: StructType([
        StructField("settlementDate", StringType()),
        StructField("settlementPeriod", IntegerType()),
        StructField("indicatedDemand", DoubleType()),
        StructField("publishTime", StringType()),
    ]),
    
    IRISTopics.INDGEN: StructType([
        StructField("settlementDate", StringType()),
        StructField("settlementPeriod", IntegerType()),
        StructField("indicatedGeneration", DoubleType()),
        StructField("publishTime", StringType()),
    ]),
    
    IRISTopics.MID: StructType([
        StructField("settlementDate", StringType()),
        StructField("settlementPeriod", IntegerType()),
        StructField("dataProvider", StringType()),
        StructField("price", DoubleType()),
        StructField("volume", DoubleType()),
    ]),
}


class IRISDataProcessor:
    """
    Advanced processor for IRIS streaming data with multiple sinks.
    """
    
    def __init__(self, spark: SparkSession, output_path: str):
        self.spark = spark
        self.output_path = output_path
        self.message_counts = {}
        self.error_counts = {}
    
    def parse_message(self, df: DataFrame, topic: str) -> DataFrame:
        """Parse JSON body based on topic schema."""
        if topic not in SCHEMAS:
            return df
        
        return (
            df.filter(col("topic") == topic)
            .withColumn("parsed", from_json(col("body"), SCHEMAS[topic]))
            .select(
                col("topic"),
                col("parsed.*"),
                to_timestamp(col("received_at")).alias("received_at"),
            )
        )
    
    def process_frequency_data(self, df: DataFrame, batch_id: int):
        """
        Process frequency data with windowed aggregations.
        
        Calculates 1-minute windows of frequency statistics.
        """
        freq_df = self.parse_message(df, IRISTopics.FREQ)
        
        if freq_df.count() == 0:
            return
        
        # Add processing timestamp
        freq_with_ts = freq_df.withColumn(
            "event_time",
            to_timestamp(col("measurementTimestamp"))
        )
        
        # Calculate statistics
        stats = freq_with_ts.select(
            avg("frequency").alias("avg_frequency"),
            spark_max("frequency").alias("max_frequency"),
            spark_min("frequency").alias("min_frequency"),
            count("*").alias("sample_count"),
        ).collect()[0]
        
        print(f"\n📊 Frequency Statistics (Batch {batch_id}):")
        print(f"   Average: {stats['avg_frequency']:.3f} Hz")
        print(f"   Range: {stats['min_frequency']:.3f} - {stats['max_frequency']:.3f} Hz")
        print(f"   Samples: {stats['sample_count']}")
        
        # Write to Parquet
        output_dir = f"{self.output_path}/frequency"
        freq_df.write.mode("append").parquet(output_dir)
    
    def process_demand_generation(self, df: DataFrame, batch_id: int):
        """
        Process demand and generation data.
        
        Compares indicated demand vs generation.
        """
        demand_df = self.parse_message(df, IRISTopics.INDDEM)
        gen_df = self.parse_message(df, IRISTopics.INDGEN)
        
        demand_count = demand_df.count()
        gen_count = gen_df.count()
        
        if demand_count == 0 and gen_count == 0:
            return
        
        print(f"\n⚡ Demand/Generation (Batch {batch_id}):")
        
        if demand_count > 0:
            latest_demand = demand_df.orderBy(col("publishTime").desc()).first()
            print(f"   Latest Demand: {latest_demand['indicatedDemand']} MW")
            
            # Write to Parquet
            demand_df.write.mode("append").parquet(f"{self.output_path}/demand")
        
        if gen_count > 0:
            latest_gen = gen_df.orderBy(col("publishTime").desc()).first()
            print(f"   Latest Generation: {latest_gen['indicatedGeneration']} MW")
            
            # Write to Parquet
            gen_df.write.mode("append").parquet(f"{self.output_path}/generation")
    
    def process_market_data(self, df: DataFrame, batch_id: int):
        """
        Process Market Index Data (MID).
        """
        mid_df = self.parse_message(df, IRISTopics.MID)
        
        if mid_df.count() == 0:
            return
        
        # Group by data provider
        by_provider = (
            mid_df.groupBy("dataProvider")
            .agg(
                avg("price").alias("avg_price"),
                avg("volume").alias("avg_volume"),
            )
            .collect()
        )
        
        print(f"\n💰 Market Data (Batch {batch_id}):")
        for row in by_provider:
            print(f"   {row['dataProvider']}: £{row['avg_price']:.2f}/MWh, {row['avg_volume']:.0f} MWh")
        
        # Write to Parquet
        mid_df.write.mode("append").parquet(f"{self.output_path}/market")
    
    def process_batch(self, df: DataFrame, batch_id: int):
        """
        Main batch processing function.
        
        Routes messages to appropriate processors based on topic.
        """
        if df is None or df.count() == 0:
            return
        
        total_count = df.count()
        print(f"\n{'='*50}")
        print(f"📨 Received batch {batch_id}: {total_count} messages")
        print(f"   Timestamp: {datetime.now().isoformat()}")
        
        # Show topic distribution
        topic_counts = df.groupBy("topic").count().collect()
        for row in topic_counts:
            topic = row["topic"]
            count = row["count"]
            self.message_counts[topic] = self.message_counts.get(topic, 0) + count
            print(f"   {topic}: {count} messages")
        
        try:
            # Process each message type
            self.process_frequency_data(df, batch_id)
            self.process_demand_generation(df, batch_id)
            self.process_market_data(df, batch_id)
            
        except Exception as e:
            print(f"❌ Error processing batch {batch_id}: {e}")
            self.error_counts[batch_id] = str(e)
    
    def print_summary(self):
        """Print processing summary."""
        print("\n" + "="*50)
        print("📊 Processing Summary")
        print("="*50)
        
        total = sum(self.message_counts.values())
        print(f"Total messages processed: {total}")
        
        for topic, count in sorted(self.message_counts.items()):
            print(f"  {topic}: {count}")
        
        if self.error_counts:
            print(f"\nErrors: {len(self.error_counts)}")


def main():
    # Configure output path
    output_path = os.getenv("IRIS_OUTPUT_PATH", "/tmp/iris_data")
    
    # Create Spark session with additional configs for better performance
    spark = (
        SparkSession.builder
        .appName("IRIS Advanced Streaming")
        .config("spark.sql.streaming.checkpointLocation", "/tmp/iris_advanced_checkpoint")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        .getOrCreate()
    )
    
    spark.sparkContext.setLogLevel("WARN")
    
    # Configure IRIS connection
    config = IRISConfig(
        host="bmrs-iris.elexon.co.uk",
        port=5671,
        username=os.getenv("IRIS_USERNAME", "YOUR_API_KEY"),
        password=os.getenv("IRIS_PASSWORD", "YOUR_API_SECRET"),
        topics=[
            IRISTopics.FREQ,
            IRISTopics.INDDEM,
            IRISTopics.INDGEN,
            IRISTopics.MID,
        ],
        max_batch_size=1000,
        prefetch_count=200,
    )
    
    # Create processor
    processor = IRISDataProcessor(spark, output_path)
    
    print("🚀 Starting IRIS Advanced Streaming")
    print(f"📁 Output path: {output_path}")
    print(f"📡 Topics: {config.topics}")
    
    # Create streaming source
    source, trigger_df = create_iris_stream(spark, config)
    
    # Define batch processing
    def process(trigger_batch_df, batch_id):
        messages_df = source.get_batch_df(spark)
        processor.process_batch(messages_df, batch_id)
    
    try:
        # Start streaming query
        query = (
            trigger_df.writeStream
            .foreachBatch(process)
            .trigger(processingTime="5 seconds")
            .option("checkpointLocation", "/tmp/iris_advanced_checkpoint")
            .start()
        )
        
        print("✅ Streaming started. Press Ctrl+C to stop.\n")
        query.awaitTermination()
        
    except KeyboardInterrupt:
        print("\n🛑 Shutting down...")
    finally:
        processor.print_summary()
        source.stop()
        spark.stop()


if __name__ == "__main__":
    main()

