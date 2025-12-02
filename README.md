# IRIS PySpark Streaming Connector

A custom PySpark streaming source connector for **Elexon's IRIS** (Insights Real-time Information Service) AMQP 1.0 message server.

## Overview

This connector enables real-time streaming of UK electricity market data from Elexon's BMRS (Balancing Mechanism Reporting Service) into PySpark for processing and analysis. IRIS provides near real-time data via AMQP 1.0 protocol with 99.9% uptime SLA.

## Features

- **AMQP 1.0 Protocol Support**: Uses Apache Qpid Proton for full AMQP 1.0 compatibility
- **PySpark Structured Streaming Integration**: Works with Spark's modern streaming API
- **Multiple Topics**: Subscribe to multiple BMRS data feeds simultaneously
- **TLS/SSL Security**: Secure connections to IRIS
- **Fault Tolerance**: Automatic reconnection and error handling
- **Configurable Batching**: Control message batch sizes for optimal throughput

## Installation

```bash
# Install dependencies
pip install -r requirements.txt

# Or install individually
pip install python-qpid-proton pyspark pydantic pydantic-settings structlog
```

### System Dependencies

The `python-qpid-proton` library requires some system dependencies:

**macOS:**
```bash
brew install qpid-proton
```

**Ubuntu/Debian:**
```bash
sudo apt-get install libqpid-proton-dev python3-qpid-proton
```

**RHEL/CentOS:**
```bash
sudo yum install qpid-proton-c-devel python3-qpid-proton
```

## Quick Start

### 1. Basic Streaming Example

```python
from pyspark.sql import SparkSession
from iris_connector import IRISConfig, IRISStreamProcessor, IRISTopics

# Create Spark session
spark = SparkSession.builder.appName("IRIS Stream").getOrCreate()

# Configure IRIS connection
config = IRISConfig(
    username="YOUR_API_KEY",
    password="YOUR_API_SECRET",
    topics=[IRISTopics.FREQ, IRISTopics.INDDEM],
)

# Process messages
def process_messages(df, batch_id):
    df.show()

# Start streaming
processor = IRISStreamProcessor(spark, config)
processor.start(process_messages, trigger_interval="2 seconds")
processor.await_termination()
```

### 2. Using Environment Variables

```bash
export IRIS_USERNAME="your_api_key"
export IRIS_PASSWORD="your_api_secret"
export IRIS_TOPICS="bmrs/FREQ,bmrs/INDDEM,bmrs/INDGEN"
```

```python
from iris_connector import IRISConfig, IRISStreamProcessor

config = IRISConfig.from_env()
processor = IRISStreamProcessor(spark, config)
```

### 3. Direct AMQP Client (without PySpark)

```python
from iris_connector import IRISAMQPClient, IRISConfig, IRISTopics

config = IRISConfig(
    username="YOUR_API_KEY",
    password="YOUR_API_SECRET",
    topics=[IRISTopics.FREQ],
)

with IRISAMQPClient(config) as client:
    while True:
        messages = client.get_batch(max_size=100, timeout=1.0)
        for msg in messages:
            print(f"Topic: {msg.topic}, Body: {msg.body}")
```

## Available Topics

The connector supports all IRIS BMRS topics. Common ones include:

| Topic | Description | Update Frequency |
|-------|-------------|------------------|
| `bmrs/FREQ` | System Frequency | ~2 seconds |
| `bmrs/INDDEM` | Indicated Demand | Per settlement period |
| `bmrs/INDGEN` | Indicated Generation | Per settlement period |
| `bmrs/PN` | Physical Notification | As submitted |
| `bmrs/BOALF` | Bid Offer Acceptance Levels | As issued |
| `bmrs/MID` | Market Index Data | Per settlement period |
| `bmrs/B1610` | Actual Generation per Type | Per settlement period |
| `bmrs/B1630` | Wind and Solar Generation | Per settlement period |

See `IRISTopics` class for the complete list.

## Configuration Reference

### IRISConfig Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `host` | str | `bmrs-iris.elexon.co.uk` | IRIS server hostname |
| `port` | int | `5671` | AMQP port (5671 for TLS) |
| `username` | str | `""` | API key/username |
| `password` | str | `""` | API secret/password |
| `topics` | list | Various | Topics to subscribe to |
| `use_tls` | bool | `True` | Enable TLS encryption |
| `verify_ssl` | bool | `True` | Verify SSL certificates |
| `prefetch_count` | int | `100` | Messages to prefetch |
| `max_batch_size` | int | `1000` | Max messages per batch |
| `connection_timeout` | int | `30` | Connection timeout (seconds) |
| `checkpoint_location` | str | `/tmp/iris_checkpoint` | Spark checkpoint dir |

### Environment Variables

| Variable | Description |
|----------|-------------|
| `IRIS_HOST` | AMQP server hostname |
| `IRIS_PORT` | AMQP port |
| `IRIS_USERNAME` | Authentication username |
| `IRIS_PASSWORD` | Authentication password |
| `IRIS_TOPICS` | Comma-separated list of topics |
| `IRIS_USE_TLS` | Enable TLS (true/false) |
| `IRIS_PREFETCH_COUNT` | Message prefetch count |
| `IRIS_MAX_BATCH_SIZE` | Max messages per batch |
| `IRIS_CHECKPOINT_LOCATION` | Spark checkpoint directory |

## Message Schema

Messages are delivered as DataFrames with the following schema:

| Column | Type | Description |
|--------|------|-------------|
| `topic` | string | The AMQP topic/address |
| `body` | string | JSON-encoded message body |
| `message_id` | string | Unique message identifier |
| `correlation_id` | string | Correlation ID |
| `timestamp` | string | Message timestamp (ISO 8601) |
| `properties` | string | JSON-encoded properties |
| `received_at` | string | Local receive timestamp |

### Parsing Message Bodies

```python
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Define schema for FREQ messages
freq_schema = StructType([
    StructField("settlementDate", StringType()),
    StructField("settlementPeriod", StringType()),
    StructField("frequency", DoubleType()),
    StructField("measurementTimestamp", StringType()),
])

# Parse JSON body
parsed_df = (
    df.filter(col("topic") == "bmrs/FREQ")
    .withColumn("data", from_json(col("body"), freq_schema))
    .select("topic", "data.*", "received_at")
)
```

## Architecture

```
┌─────────────────┐     AMQP 1.0      ┌──────────────────┐
│   Elexon IRIS   │ ◄───────────────► │  IRISAMQPClient  │
│  (AMQP Server)  │    TLS/SSL        │  (Proton-based)  │
└─────────────────┘                   └────────┬─────────┘
                                               │
                                               ▼
                                    ┌──────────────────────┐
                                    │  IRISStreamingSource │
                                    │  (Message Buffer)    │
                                    └────────┬─────────────┘
                                             │
                                             ▼
                                    ┌──────────────────────┐
                                    │ IRISStreamProcessor  │
                                    │ (foreachBatch)       │
                                    └────────┬─────────────┘
                                             │
                                             ▼
                                    ┌──────────────────────┐
                                    │   PySpark DataFrame  │
                                    │   (Your Processing)  │
                                    └──────────────────────┘
```

## Examples

Run the example scripts:

```bash
# Simple client (no Spark)
python examples/simple_client.py

# Basic PySpark streaming
python examples/basic_streaming.py

# Advanced streaming with multiple sinks
python examples/advanced_streaming.py
```

## Error Handling

The connector includes built-in error handling:

```python
client = IRISAMQPClient(config)
client.start()

# Check connection status
if not client.is_connected():
    print("Connection lost!")

# Get recent errors
errors = client.get_errors()
for timestamp, error in errors:
    print(f"[{timestamp}] {error}")
```

## Performance Tuning

### Batch Size
Adjust `max_batch_size` based on your processing capacity:
- Smaller batches (100-500): Lower latency, more frequent processing
- Larger batches (1000-5000): Higher throughput, less overhead

### Prefetch Count
The `prefetch_count` controls how many messages the AMQP client requests in advance:
- Higher values (200-500): Better throughput for high-volume topics
- Lower values (10-50): Better for low-volume or large messages

### Spark Configuration
```python
spark = (
    SparkSession.builder
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.default.parallelism", "4")
    .config("spark.streaming.backpressure.enabled", "true")
    .getOrCreate()
)
```

## Obtaining IRIS Credentials

To access IRIS, you need to register with Elexon:

1. Visit [Elexon's BMRS website](https://www.elexon.co.uk/operations-settlement/bsc-central-services/balancing-mechanism-reporting-agent/)
2. Register for API access
3. Obtain your API key and secret

## License

MIT License - See LICENSE file for details.

## Contributing

Contributions are welcome! Please submit issues and pull requests on GitHub.

## Acknowledgments

- [Elexon](https://www.elexon.co.uk/) for providing the IRIS service
- [Apache Qpid Proton](https://qpid.apache.org/proton/) for AMQP 1.0 support
- [Apache Spark](https://spark.apache.org/) for the streaming framework

