# IRIS Spark Streaming Connector

A Spark Structured Streaming source for **Elexon's IRIS** (Insights Real-time Information Service), the public AMQP feed of UK electricity market data.

Implemented as a native Python `DataSource` using `pyspark.sql.datasource.SimpleDataSourceStreamReader`. Requires **Databricks Runtime 15.x+** (or open-source Spark 4.0+).

## Semantics

- **At-least-once delivery.** Messages are received in peek-lock mode and only acked (`complete_message`) when Spark calls `commit()` after the micro-batch is durably written. A driver crash mid-batch causes Service Bus to redeliver the un-acked messages once the lock expires.
- **Single driver-side receiver.** IRIS exposes one connection per queue (Elexon: *"It is only possible to have one connection per queue"*), so parallel executor-side receive is not applicable.
- **Offsets** are a monotonic in-process counter `{"seq": <int>}` checkpointed by Spark. They are not durable across query restarts in the source — replay across restarts relies on Service Bus redelivery.

## Install

On a Databricks cluster:

```python
%pip install /Workspace/path/to/iris-pyspark-connector
# or from a wheel on a UC Volume:
%pip install /Volumes/main/libraries/iris/iris_pyspark_connector-0.2.0-py3-none-any.whl
```

Locally (for tests):

```bash
pip install -e .[dev]
```

## Credentials

Create a Databricks secret scope and load three keys:

```bash
databricks secrets create-scope iris
databricks secrets put-secret iris iris-client-id     --string-value <CLIENT_ID>
databricks secrets put-secret iris iris-client-secret --string-value <CLIENT_SECRET>
databricks secrets put-secret iris iris-tenant-id     --string-value <TENANT_ID>
```

Obtain credentials by registering at [bmrs.elexon.co.uk/iris](https://bmrs.elexon.co.uk/iris).

## Usage

```python
from iris_connector import register

register(spark)

df = (
    spark.readStream
    .format("iris")
    .option("secret_scope", "iris")
    .option("entity_path", "iris.5c9f752d-795a-4986-97cc-8a49f5380c02")
    .load()
)

(
    df.writeStream
    .format("delta")
    .option("checkpointLocation", "/Volumes/main/default/iris/_checkpoints/raw")
    .trigger(processingTime="2 seconds")
    .toTable("main.default.iris_raw")
)
```

## Options

| Option | Default | Description |
|---|---|---|
| `entity_path` | *required* | IRIS queue name, e.g. `iris.<uuid>` |
| `fully_qualified_namespace` | `elexon-insights-iris.servicebus.windows.net` | Service Bus namespace |
| `secret_scope` | — | Databricks secret scope holding credentials |
| `client_id_key` | `iris-client-id` | Secret key for client id |
| `client_secret_key` | `iris-client-secret` | Secret key for client secret |
| `tenant_id_key` | `iris-tenant-id` | Secret key for tenant id |
| `client_id` / `client_secret` / `tenant_id` | — | Inline credentials (use `secret_scope` in production) |
| `prefetch_count` | `100` | AMQP prefetch window |
| `max_messages_per_trigger` | `1000` | Upper bound on messages per micro-batch |
| `max_wait_time_seconds` | `1` | Max time to wait for a non-empty batch |
| `lock_renewal_seconds` | `300` | Auto-renew peek-locks for up to this many seconds per message |

Environment-variable fallback (for local testing): `IRIS_NAMESPACE`, `IRIS_ENTITY_PATH`, `IRIS_CLIENT_ID`, `IRIS_CLIENT_SECRET`, `IRIS_TENANT_ID`.

## Schema

| Column | Type | Source |
|---|---|---|
| `dataset` | string | `msg.subject` (e.g. `BOALF`, `FREQ`, `INDDEM`) |
| `body` | string | UTF-8 decoded message body (typically JSON) |
| `message_id` | string | `msg.message_id` |
| `correlation_id` | string | `msg.correlation_id` |
| `enqueued_time` | timestamp | `msg.enqueued_time_utc` |
| `properties` | map<string,string> | `msg.application_properties` |
| `received_at` | timestamp | Wall-clock on the driver |

Filter by dataset:

```python
freq = df.filter("dataset = 'FREQ'")
```

## Cluster compatibility

| Mode | Status |
|---|---|
| Dedicated (single-user) access mode, DBR 15.x+ | Supported |
| Standard (shared) access mode | Not supported (Python data sources require dedicated/serverless) |
| Serverless | Supported on serverless versions that include the Python DataSource API |

## Lock duration

Service Bus messages must be acked before their lock expires (default 30s on the queue). The connector uses `AutoLockRenewer` to extend each message's lock for up to `lock_renewal_seconds` (default 5 min), which covers any realistic Spark micro-batch. If a batch could plausibly take longer, raise `lock_renewal_seconds` further or lower `max_messages_per_trigger`.

## License

MIT — see [LICENSE](LICENSE).
