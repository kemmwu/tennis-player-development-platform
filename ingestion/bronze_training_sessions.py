# Databricks notebook source
# bronze_training_sessions.py
# Ingests training sessions from Parquet in Volume into bronze.raw_training_sessions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Ingestion: Training Sessions
# MAGIC
# MAGIC **Source:** `/Volumes/tennis_dev/bronze/raw_files/training_sessions.parquet`
# MAGIC **Target:** `tennis_dev.bronze.raw_training_sessions`
# MAGIC **Pattern:** Auto Loader · file hash dedup · append-only

# COMMAND ----------

from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType
)

# COMMAND ----------

# ── CONFIGURATION ─────────────────────────────────────────────────
CATALOG          = "tennis_dev"
SCHEMA           = "bronze"
TABLE            = "raw_training_sessions"
FULL_TABLE       = f"{CATALOG}.{SCHEMA}.{TABLE}"

SOURCE_PATH      = f"/Volumes/{CATALOG}/{SCHEMA}/raw_files/"
CHECKPOINT_PATH  = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints/training_sessions/"
PIPELINE_VERSION = "v1.0"
# ──────────────────────────────────────────────────────────────────

# COMMAND ----------

# ── SCHEMA DEFINITION ─────────────────────────────────────────────
session_schema = StructType([
    StructField("session_id",                       StringType(),  True),
    StructField("player_id",                        StringType(),  True),
    StructField("session_date",                     StringType(),  True),
    StructField("session_type",                     StringType(),  True),
    StructField("drills_completed",                 StringType(),  True),
    StructField("raw_note_text",                    StringType(),  True),
    StructField("avg_heart_rate",                   IntegerType(), True),
    StructField("ingested_at",                      StringType(),  True),
    StructField("shots_in",                         DoubleType(),  True),
    StructField("longest_rally",                    IntegerType(), True),
    StructField("rallies_above_5_shots",            DoubleType(),  True),
    StructField("forehand_cross_court_in",          DoubleType(),  True),
    StructField("forehand_down_the_line_in",        DoubleType(),  True),
    StructField("forehand_avg_cross_court_speed",   IntegerType(), True),
    StructField("forehand_avg_down_the_line_speed", IntegerType(), True),
    StructField("forehand_cross_court_deep",        DoubleType(),  True),
    StructField("forehand_down_the_line_deep",      DoubleType(),  True),
    StructField("backhand_cross_court_in",          DoubleType(),  True),
    StructField("backhand_down_the_line_in",        DoubleType(),  True),
    StructField("backhand_avg_cross_court_speed",   IntegerType(), True),
    StructField("backhand_avg_down_the_line_speed", IntegerType(), True),
    StructField("backhand_cross_court_deep",        DoubleType(),  True),
    StructField("backhand_down_the_line_deep",      DoubleType(),  True),
    StructField("avg_ball_speed",                   DoubleType(),  True),
    StructField("max_ball_speed",                   DoubleType(),  True),
])
# ──────────────────────────────────────────────────────────────────

# COMMAND ----------

# ── STEP 1: READ WITH AUTO LOADER ─────────────────────────────────
print(f"[{datetime.now()}] Reading from {SOURCE_PATH}")

raw_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", CHECKPOINT_PATH + "schema/")
    .option("pathGlobFilter", "training_sessions.parquet")
    .load(SOURCE_PATH)
)
# ──────────────────────────────────────────────────────────────────

# COMMAND ----------

# ── STEP 2: ADD METADATA COLUMNS ──────────────────────────────────
enriched_df = (
    raw_df
    .withColumn("_source_file",      F.col("_metadata.file_path"))
    .withColumn("_ingested_at",      F.current_timestamp())
    .withColumn("_pipeline_version", F.lit(PIPELINE_VERSION))
    .withColumn("_record_hash",      F.sha2(F.col("_metadata.file_path"), 256))
)
# ──────────────────────────────────────────────────────────────────

# COMMAND ----------

# ── STEP 3: DATA QUALITY ASSERTIONS ───────────────────────────────
validated_df = (
    enriched_df
    .filter(F.col("session_id").isNotNull())
    .filter(F.col("player_id").isNotNull())
    .filter(F.col("session_date").isNotNull())
    .filter(
        F.col("shots_in").isNull() |
        F.col("shots_in").between(0.0, 1.0)
    )
    .filter(
        F.col("forehand_cross_court_in").isNull() |
        F.col("forehand_cross_court_in").between(0.0, 1.0)
    )
    .filter(
        F.col("backhand_cross_court_in").isNull() |
        F.col("backhand_cross_court_in").between(0.0, 1.0)
    )
    .filter(
        F.col("longest_rally").isNull() |
        (F.col("longest_rally") >= 0)
    )
    .filter(
        F.col("avg_heart_rate").isNull() |
        F.col("avg_heart_rate").between(50, 220)
    )
)

print("Data quality filters applied.")
# ──────────────────────────────────────────────────────────────────

# COMMAND ----------

# ── STEP 4: WRITE TO BRONZE DELTA TABLE ───────────────────────────
def upsert_to_bronze(batch_df, batch_id):
    if batch_df.count() == 0:
        print(f"Batch {batch_id}: empty, skipping.")
        return

    table_exists = spark.catalog.tableExists(FULL_TABLE)

    if not table_exists:
        print(f"Batch {batch_id}: creating table {FULL_TABLE}")
        (
            batch_df
            .write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy("player_id")
            .saveAsTable(FULL_TABLE)
        )
    else:
        existing_ids = (
            spark.table(FULL_TABLE)
            .select("session_id")
            .distinct()
        )
        new_records = batch_df.join(
            existing_ids,
            on="session_id",
            how="left_anti"
        )
        count = new_records.count()
        if count > 0:
            print(f"Batch {batch_id}: inserting {count} new records")
            (
                new_records
                .write
                .format("delta")
                .mode("append")
                .saveAsTable(FULL_TABLE)
            )
        else:
            print(f"Batch {batch_id}: no new records to insert")


query = (
    validated_df
    .writeStream
    .foreachBatch(upsert_to_bronze)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()
print(f"[{datetime.now()}] Streaming query complete.")
# ──────────────────────────────────────────────────────────────────

# COMMAND ----------

# ── STEP 5: VERIFY ────────────────────────────────────────────────
result = spark.table(FULL_TABLE)
count  = result.count()

print(f"\n── bronze.raw_training_sessions ─────────────────")
print(f"Total rows:      {count:,}")
print(f"Unique players:  {result.select('player_id').distinct().count():,}")
print(f"Unique sessions: {result.select('session_id').distinct().count():,}")
result.groupBy("session_type").count().orderBy("count", ascending=False).show()
# ──────────────────────────────────────────────────────────────────

# COMMAND ----------

# ── STEP 6: IDEMPOTENCY TEST ──────────────────────────────────────
print("Idempotency check:")
print(f"  Total rows:    {spark.table(FULL_TABLE).count():,}")
print(f"  Unique IDs:    {spark.table(FULL_TABLE).select('session_id').distinct().count():,}")
print("  If equal, deduplication is working correctly.")
# ──────────────────────────────────────────────────────────────────