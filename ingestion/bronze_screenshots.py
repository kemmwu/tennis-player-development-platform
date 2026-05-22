# Databricks notebook source
# bronze_screenshots.py
# Ingests SwingVision screenshots from Volume
# Groups by session using filename convention
# Routes session groups to Claude API extraction

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Ingestion: SwingVision Screenshots
# MAGIC
# MAGIC **Source:** `/Volumes/tennis_dev/bronze/raw_files/screenshots/`
# MAGIC **Target:** `tennis_dev.bronze.raw_screenshots`
# MAGIC **Pattern:** Auto Loader · filename parsing · session grouping

# COMMAND ----------

from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType, TimestampType
)

# COMMAND ----------

# ── CONFIGURATION ─────────────────────────────────────────────────
CATALOG          = "tennis_dev"
SCHEMA           = "bronze"
TABLE            = "raw_screenshots"
FULL_TABLE       = f"{CATALOG}.{SCHEMA}.{TABLE}"

SOURCE_PATH      = f"/Volumes/{CATALOG}/{SCHEMA}/raw_files/screenshots/"
CHECKPOINT_PATH  = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints/screenshots/"
PIPELINE_VERSION = "v1.0"
# ──────────────────────────────────────────────────────────────────

# COMMAND ----------

# ── STEP 1: READ WITH AUTO LOADER ─────────────────────────────────
print(f"[{datetime.now()}] Reading screenshots from {SOURCE_PATH}")

raw_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "binaryFile")
    .option("cloudFiles.schemaLocation", CHECKPOINT_PATH + "schema/")
    .option("pathGlobFilter", "*.png")
    .load(SOURCE_PATH)
)
# ──────────────────────────────────────────────────────────────────

# COMMAND ----------

# ── STEP 2: PARSE FILENAME METADATA ───────────────────────────────
# Filename convention: YYYYMMDDHHNN_firstname_match/training_N.png
# Example: 202503151400_alex_match_1.png

parsed_df = (
    raw_df
    .withColumn("filename",
        F.regexp_extract(F.col("path"), r"([^/]+)\.png$", 1)
    )
    .withColumn("session_date",
        F.to_date(
            F.regexp_extract(F.col("filename"), r"^(\d{8})", 1),
            "yyyyMMdd"
        )
    )
    .withColumn("session_time",
        F.regexp_extract(F.col("filename"), r"^\d{8}(\d{4})", 1)
    )
    .withColumn("player_name",
        F.regexp_extract(F.col("filename"), r"^\d{12}_([^_]+)_", 1)
    )
    .withColumn("session_type",
        F.regexp_extract(F.col("filename"), r"^\d{12}_[^_]+_(match|training)", 1)
    )
    .withColumn("page_number",
        F.expr("try_cast(regexp_extract(filename, '_(match|training)_(\\\\d+)$', 2) as int)")
    )
    .withColumn("session_id",
        F.concat_ws("_",
            F.regexp_extract(F.col("filename"), r"^(\d{12})", 1),
            F.regexp_extract(F.col("filename"), r"^\d{12}_([^_]+)", 1),
            F.regexp_extract(F.col("filename"), r"^\d{12}_[^_]+_(match|training)", 1)
        )
    )
    .withColumn("_source_file",      F.col("path"))
    .withColumn("_ingested_at",      F.current_timestamp())
    .withColumn("_pipeline_version", F.lit(PIPELINE_VERSION))
    .withColumn("_record_hash",      F.sha2(F.col("path"), 256))
    .withColumn("file_size_bytes",   F.col("length").cast(LongType()))
)
# ──────────────────────────────────────────────────────────────────

# COMMAND ----------

# ── STEP 3: DATA QUALITY FILTERS ──────────────────────────────────
validated_df = (
    parsed_df
    .filter(F.col("session_date").isNotNull())
    .filter(F.col("player_name") != "")
    .filter(F.col("session_type").isin("match", "training"))
    .filter(F.col("page_number").isNotNull())
)

print("Filename parsing and quality filters applied.")
# ──────────────────────────────────────────────────────────────────

# COMMAND ----------

# ── STEP 4: WRITE METADATA TO BRONZE ──────────────────────────────
def upsert_screenshots(batch_df, batch_id):
    if batch_df.count() == 0:
        print(f"Batch {batch_id}: empty, skipping.")
        return

    # Keep only metadata columns — not the binary image content
    meta_df = batch_df.select(
        "session_id",
        "session_date",
        "session_time",
        "player_name",
        "session_type",
        "page_number",
        "filename",
        "file_size_bytes",
        "_source_file",
        "_ingested_at",
        "_pipeline_version",
        "_record_hash"
    )

    table_exists = spark.catalog.tableExists(FULL_TABLE)

    if not table_exists:
        print(f"Batch {batch_id}: creating {FULL_TABLE}")
        meta_df.write.format("delta").mode("overwrite").saveAsTable(FULL_TABLE)
    else:
        existing = spark.table(FULL_TABLE).select("_record_hash").distinct()
        new_files = meta_df.join(existing, on="_record_hash", how="left_anti")
        count = new_files.count()
        if count > 0:
            print(f"Batch {batch_id}: inserting {count} new screenshots")
            new_files.write.format("delta").mode("append").saveAsTable(FULL_TABLE)
        else:
            print(f"Batch {batch_id}: no new screenshots")


query = (
    validated_df
    .writeStream
    .foreachBatch(upsert_screenshots)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()
print(f"[{datetime.now()}] Screenshot ingestion complete.")
# ──────────────────────────────────────────────────────────────────

# COMMAND ----------

# ── STEP 5: VERIFY ────────────────────────────────────────────────
result = spark.table(FULL_TABLE)
print(f"\n── bronze.raw_screenshots ───────────────────────")
print(f"Total screenshots:  {result.count():,}")
print(f"Unique sessions:    {result.select('session_id').distinct().count():,}")
print(f"Unique players:     {result.select('player_name').distinct().count():,}")
result.groupBy("session_type").count().show()
result.groupBy("player_name", "session_type").count().orderBy("player_name").show()
# ──────────────────────────────────────────────────────────────────