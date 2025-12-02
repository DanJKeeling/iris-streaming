# IRIS PySpark Streaming Connector

A custom PySpark streaming source connector for **Elexon's IRIS** (Insights Real-time Information Service) AMQP 1.0 message server.

## Overview

This connector enables real-time streaming of UK electricity market data from Elexon's BMRS (Balancing Mechanism Reporting Service) into PySpark for processing and analysis. IRIS provides near real-time data via AMQP 1.0 protocol with 99.9% uptime SLA.

## Features

- **AMQP 1.0 Protocol Support**: Uses Apache Qpid Proton for full AMQP 1.0 compatibility
- **PySpark Structured Streaming Integration**: Works with Spark's modern streaming API
- **Databricks Secret Integration**: Secure credential management via Databricks secret scopes
- **Multiple Topics**: Subscribe to multiple BMRS data feeds simultaneously
- **TLS/SSL Security**: Secure connections to IRIS
- **Fault Tolerance**: Automatic reconnection and error handling
- **Configurable Batching**: Control message batch sizes for optimal throughput

## Databricks Setup

### 1. Cluster Configuration

The `python-qpid-proton` library requires system-level dependencies. Create a cluster init script:

**Create init script** (`dbfs:/init-scripts/install-qpid-proton.sh`):
```bash
#!/bin/bash
# Install Qpid Proton dependencies for AMQP 1.0 support

apt-get update
apt-get install -y libqpid-proton-dev

pip install python-qpid-proton
```

**Upload to DBFS:**
```bash
databricks fs cp install-qpid-proton.sh dbfs:/init-scripts/install-qpid-proton.sh
```

**Configure cluster:**
1. Go to Compute → Select your cluster → Edit
2. Under "Advanced options" → "Init Scripts"
3. Add: `dbfs:/init-scripts/install-qpid-proton.sh`
4. Restart the cluster

**Alternative: Install via cluster libraries:**
- Go to Compute → Select cluster → Libraries → Install New
- Select PyPI and enter: `python-qpid-proton`

> **Note**: PyPI installation may fail without the init script on some cluster types.

### 2. Install the Connector

Upload the `iris_connector` package to your Databricks workspace or install from a wheel:

```python
# In a notebook, install from workspace path
%pip install /Workspace/path/to/iris_connector

# Or from a wheel file on DBFS
%pip install /dbfs/libraries/iris_connector-0.1.0-py3-none-any.whl
```

### 3. Secret Setup

Before using the connector, set up your IRIS credentials in a Databricks secret scope:

```bash
# Create the secret scope (run once)
databricks secrets create-scope --scope iris

# Add your IRIS API credentials
databricks secrets put --scope iris --key iris-username
databricks secrets put --scope iris --key iris-password
```

## Quick Start

### 1. Basic Streaming Example (Databricks Notebook)

```python
from iris_connector import IRISConfig, IRISStreamProcessor, IRISTopics

# spark is already available in Databricks notebooks

# Configure IRIS connection with Databricks secrets
config = IRISConfig.from_databricks_secrets(
    scope="iris",
    username_key="iris-username",
    password_key="iris-password",
    topics=[IRISTopics.FREQ, IRISTopics.INDDEM],
    # Use DBFS for checkpoint persistence
    checkpoint_location="/dbfs/checkpoints/iris/stream",
)

# Process messages
def process_messages(df, batch_id):
    df.show()

# Start streaming
processor = IRISStreamProcessor(spark, config)
query = processor.start(process_messages, trigger_interval="2 seconds")

# To stop the stream:
# processor.stop()
```

### 2. Custom Secret Scope and Keys

```python
from iris_connector import IRISConfig, IRISStreamProcessor, IRISTopics

# Use custom secret scope and key names
config = IRISConfig.from_databricks_secrets(
    scope="my-custom-scope",
    username_key="elexon-api-key",
    password_key="elexon-api-secret",
    topics=[IRISTopics.FREQ, IRISTopics.INDDEM],
    checkpoint_location="/dbfs/checkpoints/iris/custom",
)

processor = IRISStreamProcessor(spark, config)
```

### 3. Test Connectivity (Direct AMQP Client)

```python
from iris_connector import IRISAMQPClient, IRISConfig, IRISTopics

# Credentials retrieved from Databricks secrets
config = IRISConfig.from_databricks_secrets(
    scope="iris",
    topics=[IRISTopics.FREQ],
)

# Connect and receive some messages
client = IRISAMQPClient(config)
client.start()

# Get a batch of messages
messages = client.get_batch(max_size=10, timeout=10.0)
for msg in messages:
    print(f"Topic: {msg.topic}, Body: {msg.body}")

# Remember to stop when done
client.stop()
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

### Databricks Secrets (Recommended)

The recommended way to configure credentials in Databricks:

```python
config = IRISConfig.from_databricks_secrets(
    scope="iris",                    # Databricks secret scope name
    username_key="iris-username",    # Key for username secret
    password_key="iris-password",    # Key for password secret
    topics=[...],                    # Topics to subscribe to
    # Additional options can be passed as keyword arguments
    max_batch_size=1000,
    prefetch_count=200,
)
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `scope` | str | `"iris"` | Databricks secret scope name |
| `username_key` | str | `"iris-username"` | Secret key for API username |
| `password_key` | str | `"iris-password"` | Secret key for API password |

### Environment Variables (Alternative)

For non-Databricks environments, you can use environment variables:

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

