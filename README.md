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

Resolve credentials in the notebook (the data source runs in an isolated subprocess where `dbutils` is unavailable) and pass them as options:

```python
from iris_connector import register

register(spark)

client_id     = dbutils.secrets.get("iris", "iris-client-id")
client_secret = dbutils.secrets.get("iris", "iris-client-secret")
tenant_id     = dbutils.secrets.get("iris", "iris-tenant-id")

df = (
    spark.readStream
    .format("iris")
    .option("fully_qualified_namespace", "<your-iris-namespace>.servicebus.windows.net")
    .option("entity_path", "<your-iris-queue-uuid>")
    .option("client_id", client_id)
    .option("client_secret", client_secret)
    .option("tenant_id", tenant_id)
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

## Deploy via Asset Bundle

The repo ships a Databricks Asset Bundle (`databricks.yml`) that builds the wheel, provisions a UC Volume for checkpoints, and deploys `examples/quickstart.py` as a serverless Job. The bundle has **no environment-specific values committed** — workspace, catalog, and Elexon-portal-derived values are all supplied at deploy time.

**Before `databricks bundle deploy` will work, you must:**

1. **Configure a Databricks CLI profile for your workspace.** The bundle does not pin `workspace.host`; the host comes from whichever profile you pass via `-p`. Set one up once:

   ```bash
   databricks configure --profile <your-cli-profile>   # interactive
   ```

2. **Create `.databricks/bundle/<target>/variable-overrides.json`** *(gitignored — never committed)*
   The shipped target name is `fevm`; either reuse it or rename it in `databricks.yml`. Then create the matching override file with the five required values:

   ```json
   {
     "catalog": "<your-catalog>",
     "schema": "<your-schema>",
     "volume": "<your-volume-name>",
     "iris_namespace": "<your-iris-namespace>.servicebus.windows.net",
     "iris_entity_path": "iris.<your-queue-uuid>"
   }
   ```

   The catalog and schema must already exist (the bundle creates the volume). The IRIS values come from your Elexon registration.

3. **Populate the `iris` secret scope** (see [Credentials](#credentials) above) — the notebook reads `iris-client-id`, `iris-client-secret`, and `iris-tenant-id` via `dbutils.secrets.get` at runtime. To use a different scope name, override `iris_secret_scope` in the JSON file (default is `iris`).

Then:

```bash
databricks bundle deploy -t fevm -p <your-cli-profile>
databricks bundle run iris_streaming_quickstart -t fevm -p <your-cli-profile>
```

### Known gotchas

- **Terraform GPG-key expiry in CLI v0.294.x.** If `bundle deploy` fails with `error downloading Terraform: openpgp: key expired`, point DAB at a locally-installed Terraform: `export DATABRICKS_TF_EXEC_PATH=$(which terraform) DATABRICKS_TF_VERSION=$(terraform version | head -1 | awk '{print $2}' | tr -d v)`. Resolved in newer CLI builds.
- **Wheel build needs internet for `setuptools`.** The bundle uses `uv build` (which bootstraps its own build deps) instead of `pip wheel`; if you swap to `pip`, ensure `setuptools>=64` is reachable.

## Options

| Option | Default | Description |
|---|---|---|
| `fully_qualified_namespace` | *required* | Service Bus namespace, e.g. `<your-iris-namespace>.servicebus.windows.net` |
| `entity_path` | *required* | IRIS queue name, e.g. `iris.<uuid>` |
| `client_id` / `client_secret` / `tenant_id` | *required* | Service principal credentials (resolve via `dbutils.secrets.get` in the notebook) |
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
