"""Stream Elexon IRIS into a Delta table on Databricks (DBR 15.x+).

Prerequisites:
    1. Install the connector on the cluster:
         %pip install /Workspace/path/to/iris-pyspark-connector
       (or build a wheel and install from a Volume)
    2. Create a Databricks secret scope `iris` with keys:
         iris-client-id, iris-client-secret, iris-tenant-id
    3. Create a UC Volume for the checkpoint, e.g.
         /Volumes/main/default/iris/_checkpoints
"""

from iris_connector import register

register(spark)  # noqa: F821

stream = (
    spark.readStream  # noqa: F821
    .format("iris")
    .option("secret_scope", "iris")
    .option("entity_path", "iris.5c9f752d-795a-4986-97cc-8a49f5380c02")
    .option("max_messages_per_trigger", "500")
    .load()
)

(
    stream.writeStream
    .format("delta")
    .option("checkpointLocation", "/Volumes/main/default/iris/_checkpoints/quickstart")
    .trigger(processingTime="2 seconds")
    .toTable("main.default.iris_raw")
)
