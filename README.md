# IRIS PySpark Streaming Connector

A custom PySpark streaming source connector for **Elexon's IRIS** (Insights Real-time Information Service) AMQP 1.0 message server.

## Overview

This connector enables real-time streaming of UK electricity market data from Elexon's BMRS (Balancing Mechanism Reporting Service) into PySpark for processing and analysis. IRIS provides near real-time data via AMQP 1.0 protocol with 99.9% uptime SLA.

## Features

- **AMQP 1.0 Protocol Support**: Uses Azure Service Bus SDK for robust AMQP 1.0 connectivity
- **PySpark Structured Streaming Integration**: Works with Spark's modern streaming API
- **Azure Active Directory Support**: Authenticate securely using Client ID, Secret, and Tenant ID
- **Databricks Secret Integration**: Secure credential management via Databricks secret scopes
- **Multiple Topics**: Subscribe to multiple BMRS data feeds simultaneously
- **Fault Tolerance**: Automatic reconnection and error handling
- **Configurable Batching**: Control message batch sizes for optimal throughput

## Databricks Setup

### 1. Install the Connector

Upload the `iris_connector` package to your Databricks workspace or install from a wheel. 
The connector depends on `azure-servicebus` and `azure-identity`.

```python
# In a notebook, install from workspace path
%pip install /Workspace/path/to/iris_connector

# Or from a wheel file on DBFS
%pip install /dbfs/libraries/iris_connector-0.1.0-py3-none-any.whl
```

### 2. Secret Setup

Before using the connector, set up your IRIS credentials in a Databricks secret scope:

```bash
# Create the secret scope (run once)
databricks secrets create-scope --scope iris

# Add your IRIS API credentials
databricks secrets put --scope iris --key iris-client-id
databricks secrets put --scope iris --key iris-client-secret
databricks secrets put --scope iris --key iris-tenant-id
```

## Quick Start

### 1. Basic Streaming Example (Databricks Notebook)

```python
from iris_connector import IRISConfig, IRISStreamProcessor

# spark is already available in Databricks notebooks

# Configure IRIS connection with Databricks secrets
config = IRISConfig.from_databricks_secrets(
    scope="iris",
    client_id_key="iris-client-id",
    client_secret_key="iris-client-secret",
    tenant_id_key="iris-tenant-id",
    # Uses default queue: iris.5c9f752d-795a-4986-97cc-8a49f5380c02
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
from iris_connector import IRISConfig, IRISStreamProcessor

# Use custom secret scope and key names
config = IRISConfig.from_databricks_secrets(
    scope="my-custom-scope",
    client_id_key="elexon-client-id",
    client_secret_key="elexon-client-secret",
    tenant_id_key="elexon-tenant-id",
    checkpoint_location="/dbfs/checkpoints/iris/custom",
)

processor = IRISStreamProcessor(spark, config)
```

### 3. Test Connectivity (Direct AMQP Client)

```python
from iris_connector import IRISAMQPClient, IRISConfig

# Credentials retrieved from Databricks secrets
config = IRISConfig.from_databricks_secrets(scope="iris")

# Connect and receive some messages
client = IRISAMQPClient(config)
client.start()

# Get a batch of messages
messages = client.get_batch(max_size=10, timeout=10.0)
for msg in messages:
    print(f"Body: {msg.body}")

# Remember to stop when done
client.stop()
```

## Configuration Reference

### IRISConfig Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `host` | str | `elexon-insights-iris.servicebus.windows.net` | Azure Service Bus hostname |
| `port` | int | `5671` | AMQP port (5671 for TLS) |
| `queue` | str | `iris.5c9f752d-795a-4986-97cc-8a49f5380c02` | IRIS queue path |
| `client_id` | str | `""` | OAuth Client ID |
| `client_secret` | str | `""` | OAuth Client Secret |
| `tenant_id` | str | `""` | OAuth Tenant ID |
| `use_tls` | bool | `True` | Enable TLS encryption |
| `verify_ssl` | bool | `True` | Verify SSL certificates |
| `prefetch_count` | int | `100` | Messages to prefetch |
| `max_batch_size` | int | `1000` | Max messages per batch |
| `connection_timeout` | int | `30` | Connection timeout (seconds) |
| `checkpoint_location` | str | `/dbfs/checkpoints/iris` | Spark checkpoint dir |

### Databricks Secrets (Recommended)

The recommended way to configure credentials in Databricks:

```python
config = IRISConfig.from_databricks_secrets(
    scope="iris",                        # Databricks secret scope name
    client_id_key="iris-client-id",      # Key for Client ID secret
    client_secret_key="iris-client-secret",  # Key for Client Secret
    tenant_id_key="iris-tenant-id",      # Key for Tenant ID secret
    # Additional options can be passed as keyword arguments
    max_batch_size=1000,
    prefetch_count=200,
)
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `scope` | str | `"iris"` | Databricks secret scope name |
| `client_id_key` | str | `"iris-client-id"` | Secret key for Client ID |
| `client_secret_key` | str | `"iris-client-secret"` | Secret key for Client Secret |
| `tenant_id_key` | str | `"iris-tenant-id"` | Secret key for Tenant ID |

### Environment Variables (Alternative)

For non-Databricks environments, you can use environment variables:

| Variable | Description |
|----------|-------------|
| `IRIS_HOST` | Azure Service Bus hostname |
| `IRIS_PORT` | AMQP port |
| `IRIS_QUEUE` | IRIS queue path |
| `IRIS_CLIENT_ID` | OAuth Client ID |
| `IRIS_CLIENT_SECRET` | OAuth Client Secret |
| `IRIS_TENANT_ID` | OAuth Tenant ID |
| `IRIS_USE_TLS` | Enable TLS (true/false) |
| `IRIS_PREFETCH_COUNT` | Message prefetch count |
| `IRIS_MAX_BATCH_SIZE` | Max messages per batch |
| `IRIS_CHECKPOINT_LOCATION` | Spark checkpoint directory |

## Message Schema

Messages are delivered as DataFrames with the following schema:

| Column | Type | Description |
|--------|------|-------------|
| `topic` | string | The AMQP queue address |
| `body` | string | JSON-encoded message body |
| `message_id` | string | Unique message identifier |
| `correlation_id` | string | Correlation ID |
| `timestamp` | string | Message timestamp (ISO 8601) |
| `properties` | string | JSON-encoded properties |
| `received_at` | string | Local receive timestamp |

### Parsing Message Bodies

```python
from pyspark.sql.functions import from_json, col, get_json_object
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Extract specific fields from JSON body
df_with_fields = df.select(
    get_json_object(col("body"), "$.messageType").alias("message_type"),
    col("body"),
    col("received_at"),
)

# Or define a schema and parse the full body
message_schema = StructType([
    StructField("messageType", StringType()),
    StructField("data", StringType()),
])

parsed_df = (
    df.withColumn("parsed", from_json(col("body"), message_schema))
    .select("parsed.*", "received_at")
)
```

## Architecture

```
┌─────────────────────────────┐   AMQP 1.0   ┌──────────────────┐
│  Azure Service Bus (IRIS)   │ ◄──────────► │  IRISAMQPClient  │
│  elexon-insights-iris...    │   TLS/SSL    │ (Azure SDK-based)│
└─────────────────────────────┘              └────────┬─────────┘
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
- Higher values (200-500): Better throughput for high message rates
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
- [Azure Service Bus SDK](https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/servicebus/azure-servicebus) for AMQP 1.0 support
- [Apache Spark](https://spark.apache.org/) for the streaming framework

