# Databricks notebook source
# bronze_match_stats.py
# Ingests match statistics from Parquet in Volume into bronze.raw_match_extractions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Ingestion: Match Stats
# MAGIC
# MAGIC **Source:** `/Volumes/tennis_dev/bronze/raw_files/match_stats.parquet`
# MAGIC **Target:** `tennis_dev.bronze.raw_match_extractions`
# MAGIC **Pattern:** Auto Loader · file hash dedup · append-only

# COMMAND ----------

from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType,
    BooleanType
)

# COMMAND ----------

# ── CONFIGURATION ─────────────────────────────────────────────────
CATALOG          = "tennis_dev"
SCHEMA           = "bronze"
TABLE            = "raw_match_extractions"
FULL_TABLE       = f"{CATALOG}.{SCHEMA}.{TABLE}"

SOURCE_PATH      = f"/Volumes/{CATALOG}/{SCHEMA}/raw_files/"
CHECKPOINT_PATH  = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints/match_stats/"
PIPELINE_VERSION = "v1.0"
# ──────────────────────────────────────────────────────────────────

# COMMAND ----------

# ── SCHEMA DEFINITION ─────────────────────────────────────────────
match_schema = StructType([
    StructField("match_id",               StringType(),  True),
    StructField("player_id",              StringType(),  True),
    StructField("match_date",             StringType(),  True),
    StructField("opponent_name",          StringType(),  True),
    StructField("opponent_utr",           DoubleType(),  True),
    StructField("tournament_name",        StringType(),  True),
    StructField("surface",                StringType(),  True),
    StructField("serves_in_ad",           DoubleType(),  True),
    StructField("serves_in_deuce",        DoubleType(),  True),
    StructField("avg_serve_speed",        IntegerType(), True),
    StructField("max_serve_speed",        IntegerType(), True),
    StructField("returns_in_ad",          DoubleType(),  True),
    StructField("returns_in_deuce",       DoubleType(),  True),
    StructField("avg_return_speed",       IntegerType(), True),
    StructField("max_return_speed",       IntegerType(), True),
    StructField("total_moving_distance",  DoubleType(),  True),
    StructField("winners",                IntegerType(), True),
    StructField("unforced_errors",        IntegerType(), True),
    StructField("break_points_won",       IntegerType(), True),
    StructField("break_points_total",     IntegerType(), True),
    StructField("avg_rally_length",       DoubleType(),  True),
    StructField("player_won",             BooleanType(), True),
    StructField("score",                  StringType(),  True),
    StructField("raw_note_text",          StringType(),  True),
    StructField("source_file",            StringType(),  True),
    StructField("extraction_confidence",  DoubleType(),  True),
    StructField("prompt_version",         StringType(),  True),
    StructField("ingested_at",            StringType(),  True),
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
    .option("pathGlobFilter", "match_stats.parquet")
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
    .filter(F.col("match_id").isNotNull())
    .filter(F.col("player_id").isNotNull())
    .filter(F.col("match_date").isNotNull())
    .filter(
        F.col("serves_in_ad").isNull() |
        F.col("serves_in_ad").between(0.0, 1.0)
    )
    .filter(
        F.col("serves_in_deuce").isNull() |
        F.col("serves_in_deuce").between(0.0, 1.0)
    )
    .filter(
        F.col("winners").isNull() |
        (F.col("winners") >= 0)
    )
    .filter(
        F.col("unforced_errors").isNull() |
        (F.col("unforced_errors") >= 0)
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
            .select("match_id")
            .distinct()
        )
        new_records = batch_df.join(
            existing_ids,
            on="match_id",
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

print("\n── bronze.raw_match_extractions ─────────────────")
print(f"Total rows:      {count:,}")
print(f"Unique players:  {result.select('player_id').distinct().count():,}")
print(f"Unique matches:  {result.select('match_id').distinct().count():,}")
result.groupBy("surface").count().orderBy("surface").show()
# ──────────────────────────────────────────────────────────────────

# COMMAND ----------

# ── STEP 6: IDEMPOTENCY TEST ──────────────────────────────────────
print("Idempotency check:")
print(f"  Total rows:    {spark.table(FULL_TABLE).count():,}")
print(f"  Unique IDs:    {spark.table(FULL_TABLE).select('match_id').distinct().count():,}")
print("  If equal, deduplication is working correctly.")
# ──────────────────────────────────────────────────────────────────
