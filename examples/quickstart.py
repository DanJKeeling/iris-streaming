# Databricks notebook source
# MAGIC %md
# MAGIC # IRIS Streaming Quickstart
# MAGIC
# MAGIC Streams Elexon's IRIS feed into a Delta table using the
# MAGIC `iris` Spark Structured Streaming source. Configured via job widgets.

# COMMAND ----------

dbutils.widgets.text("secret_scope", "iris")
dbutils.widgets.text("fully_qualified_namespace", "")
dbutils.widgets.text("entity_path", "")
dbutils.widgets.text("table_name", "")
dbutils.widgets.text("checkpoint_path", "")
dbutils.widgets.text("max_messages_per_trigger", "500")

# COMMAND ----------

from iris_connector import register

register(spark)

# COMMAND ----------

scope = dbutils.widgets.get("secret_scope")
client_id = dbutils.secrets.get(scope, "iris-client-id")
client_secret = dbutils.secrets.get(scope, "iris-client-secret")
tenant_id = dbutils.secrets.get(scope, "iris-tenant-id")

stream = (
    spark.readStream
    .format("iris")
    .option("fully_qualified_namespace", dbutils.widgets.get("fully_qualified_namespace"))
    .option("entity_path", dbutils.widgets.get("entity_path"))
    .option("client_id", client_id)
    .option("client_secret", client_secret)
    .option("tenant_id", tenant_id)
    .option("max_messages_per_trigger", dbutils.widgets.get("max_messages_per_trigger"))
    .load()
)

query = (
    stream.writeStream
    .format("delta")
    .option("checkpointLocation", dbutils.widgets.get("checkpoint_path"))
    .trigger(availableNow=True)
    .toTable(dbutils.widgets.get("table_name"))
)
query.awaitTermination()
