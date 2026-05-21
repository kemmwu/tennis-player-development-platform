# Databricks notebook source
# bronze_students_stream.py
# Consumes student intake events from Kafka via Lakeflow Connect
# into bronze.raw_students Delta streaming table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Streaming Ingestion: Student Intake Events
# MAGIC
# MAGIC **Source:** Kafka topic `student_intake_events` via Lakeflow Connect
# MAGIC **Target:** `tennis_dev.bronze.raw_students`
# MAGIC **Pattern:** Spark Structured Streaming · append-only · dedup by intake_id

# COMMAND ----------

from datetime import datetime
from pyspark.sql import functions as F
import uuid
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# COMMAND ----------

# ── CONFIGURATION ─────────────────────────────────────────────────
CATALOG          = "tennis_dev"
SCHEMA           = "bronze"
TABLE            = "raw_students"
FULL_TABLE       = f"{CATALOG}.{SCHEMA}.{TABLE}"
KAFKA_TABLE      = f"{CATALOG}.{SCHEMA}.student_intake_events_raw"
CHECKPOINT_PATH  = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints/students_stream/"
PIPELINE_VERSION = "v1.0"
# ──────────────────────────────────────────────────────────────────

# COMMAND ----------

# ── STEP 1: READ FROM LAKEFLOW CONNECT DELTA TABLE ────────────────
# Lakeflow Connect writes Kafka messages to a Delta table automatically
# We read from that Delta table as a stream

print(f"[{datetime.now()}] Reading from Lakeflow Connect table: {KAFKA_TABLE}")

raw_stream = (
    spark.readStream
    .format("delta")
    .table(KAFKA_TABLE)
)
# ──────────────────────────────────────────────────────────────────

# COMMAND ----------

# ── STEP 2: PARSE AND ENRICH ───────────────────────────────────────

generate_uuid = udf(lambda: str(uuid.uuid4()), StringType())

enriched_stream = (
    raw_stream
    .withColumn("student_id",        generate_uuid())
    .withColumn("_ingested_at",      F.current_timestamp())
    .withColumn("_pipeline_version", F.lit(PIPELINE_VERSION))
    .withColumn("is_active",         F.lit(True))
    .withColumn("valid_from",        F.current_timestamp())
    .withColumn("valid_to",          F.lit(None).cast("timestamp"))
    .withColumn("is_current",        F.lit(True))
)
# ──────────────────────────────────────────────────────────────────

# COMMAND ----------

# ── STEP 3: WRITE TO BRONZE TABLE ─────────────────────────────────
def upsert_intake_event(batch_df, batch_id):
    if batch_df.count() == 0:
        print(f"Batch {batch_id}: empty, skipping.")
        return

    table_exists = spark.catalog.tableExists(FULL_TABLE)

    if not table_exists:
        print(f"Batch {batch_id}: creating table {FULL_TABLE}")
        batch_df.write.format("delta").mode("overwrite").saveAsTable(FULL_TABLE)
    else:
        # Dedup by intake_id
        existing = spark.table(FULL_TABLE).select("intake_id").distinct()
        new_rows = batch_df.join(existing, on="intake_id", how="left_anti")
        count = new_rows.count()
        if count > 0:
            print(f"Batch {batch_id}: inserting {count} new students")
            new_rows.write.format("delta").mode("append").saveAsTable(FULL_TABLE)
        else:
            print(f"Batch {batch_id}: no new students")


query = (
    enriched_stream
    .writeStream
    .foreachBatch(upsert_intake_event)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()
print(f"[{datetime.now()}] Streaming query complete.")
# ──────────────────────────────────────────────────────────────────

# COMMAND ----------

# ── STEP 4: VERIFY ────────────────────────────────────────────────
result = spark.table(FULL_TABLE)
print("\n── bronze.raw_students (stream path) ───────────")
print(f"Total rows: {result.count():,}")
result.orderBy("_ingested_at", ascending=False).show(5)
# ──────────────────────────────────────────────────────────────────
