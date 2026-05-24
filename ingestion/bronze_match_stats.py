# Databricks notebook source
# bronze_match_stats.py
# Ingests synthetic match statistics from Parquet file in Volume
# Writes to tennis_dev.bronze.raw_match_extractions
# Uses pandas batch approach to avoid Auto Loader schema caching issues

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Ingestion: Match Statistics (Synthetic)
# MAGIC
# MAGIC **Source:** `/Volumes/tennis_dev/bronze/raw_files/match_stats.parquet`
# MAGIC **Target:** `tennis_dev.bronze.raw_match_extractions`
# MAGIC **Pattern:** Pandas batch read · type casting · dedup by match_id · append

# COMMAND ----------

import pandas as pd
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DoubleType, LongType, StringType, BooleanType

# COMMAND ----------

# ── CONFIGURATION ─────────────────────────────────────────────────
CATALOG          = "tennis_dev"
SCHEMA           = "bronze"
TABLE            = "raw_match_extractions"
FULL_TABLE       = f"{CATALOG}.{SCHEMA}.{TABLE}"
SOURCE_FILE      = f"/Volumes/{CATALOG}/{SCHEMA}/raw_files/match_stats.parquet"
PIPELINE_VERSION = "v2.0"
# ──────────────────────────────────────────────────────────────────

# COMMAND ----------

# ── STEP 1: READ WITH PANDAS ──────────────────────────────────────
print(f"[{datetime.now()}] Reading {SOURCE_FILE}")
pdf = pd.read_parquet(SOURCE_FILE)
print(f"Parquet rows: {len(pdf):,}")
print(f"Parquet columns: {list(pdf.columns)}")
# ──────────────────────────────────────────────────────────────────

# COMMAND ----------

# ── STEP 2: CREATE SPARK DATAFRAME ────────────────────────────────
df = spark.createDataFrame(pdf)
# ──────────────────────────────────────────────────────────────────

# COMMAND ----------

# ── STEP 3: CAST COLUMNS TO CORRECT TYPES ────────────────────────
df = (
    df
    .withColumn("match_date",    F.col("match_date").cast(DateType()))
    .withColumn("winners",       F.col("winners").cast(LongType()))
    .withColumn("unforced_errors", F.col("unforced_errors").cast(LongType()))
    .withColumn("forehand_winners", F.col("forehand_winners").cast(LongType()))
    .withColumn("forehand_unforced_errors", F.col("forehand_unforced_errors").cast(LongType()))
    .withColumn("backhand_winners", F.col("backhand_winners").cast(LongType()))
    .withColumn("backhand_unforced_errors", F.col("backhand_unforced_errors").cast(LongType()))
    .withColumn("total_points_won", F.col("total_points_won").cast(LongType()))
    .withColumn("total_points_played", F.col("total_points_played").cast(LongType()))
    .withColumn("break_points_won", F.col("break_points_won").cast(LongType()))
    .withColumn("break_points_total", F.col("break_points_total").cast(LongType()))
    .withColumn("break_points_saved", F.col("break_points_saved").cast(LongType()))
    .withColumn("break_points_saved_total", F.col("break_points_saved_total").cast(LongType()))
    .withColumn("aces",          F.col("aces").cast(LongType()))
    .withColumn("service_winners", F.col("service_winners").cast(LongType()))
    .withColumn("double_faults", F.col("double_faults").cast(LongType()))
    .withColumn("first_serves_in", F.col("first_serves_in").cast(LongType()))
    .withColumn("first_serves_total", F.col("first_serves_total").cast(LongType()))
    .withColumn("second_serves_in", F.col("second_serves_in").cast(LongType()))
    .withColumn("second_serves_total", F.col("second_serves_total").cast(LongType()))
    .withColumn("serve_points_won", F.col("serve_points_won").cast(LongType()))
    .withColumn("serve_points_total", F.col("serve_points_total").cast(LongType()))
    .withColumn("first_serves_won", F.col("first_serves_won").cast(LongType()))
    .withColumn("first_serves_won_total", F.col("first_serves_won_total").cast(LongType()))
    .withColumn("second_serves_won", F.col("second_serves_won").cast(LongType()))
    .withColumn("second_serves_won_total", F.col("second_serves_won_total").cast(LongType()))
    .withColumn("return_points_won", F.col("return_points_won").cast(LongType()))
    .withColumn("return_points_total", F.col("return_points_total").cast(LongType()))
    .withColumn("first_returns_won", F.col("first_returns_won").cast(LongType()))
    .withColumn("first_returns_total", F.col("first_returns_total").cast(LongType()))
    .withColumn("second_returns_won", F.col("second_returns_won").cast(LongType()))
    .withColumn("second_returns_total", F.col("second_returns_total").cast(LongType()))
    .withColumn("rallies_1_4_won", F.col("rallies_1_4_won").cast(LongType()))
    .withColumn("rallies_1_4_total", F.col("rallies_1_4_total").cast(LongType()))
    .withColumn("rallies_5_8_won", F.col("rallies_5_8_won").cast(LongType()))
    .withColumn("rallies_5_8_total", F.col("rallies_5_8_total").cast(LongType()))
    .withColumn("rallies_9plus_won", F.col("rallies_9plus_won").cast(LongType()))
    .withColumn("rallies_9plus_total", F.col("rallies_9plus_total").cast(LongType()))
    .withColumn("pages_processed", F.col("pages_processed").cast(LongType()))
    .withColumn("extraction_confidence", F.col("extraction_confidence").cast(DoubleType()))
    .withColumn("player_won",     F.col("player_won").cast(BooleanType()))
)
# ──────────────────────────────────────────────────────────────────

# COMMAND ----------

# ── STEP 4: ADD METADATA COLUMNS ─────────────────────────────────
df = (
    df
    .withColumn("_source_file",      F.lit(SOURCE_FILE))
    .withColumn("_record_hash",      F.sha2(F.col("match_id"), 256))
    .withColumn("_pipeline_version", F.lit(PIPELINE_VERSION))
)
# ──────────────────────────────────────────────────────────────────

# COMMAND ----------

# ── STEP 5: QUALITY FILTER ────────────────────────────────────────
df = (
    df
    .filter(F.col("match_id").isNotNull())
    .filter(F.col("player_id").isNotNull())
    .filter(F.col("match_date").isNotNull())
    .filter(F.col("winners") >= 0)
    .filter(F.col("unforced_errors") >= 0)
)
# ──────────────────────────────────────────────────────────────────

# COMMAND ----------

# ── STEP 6: DEDUPLICATE AGAINST EXISTING TABLE ────────────────────
table_exists = spark.catalog.tableExists(FULL_TABLE)

if not table_exists:
    print(f"Table does not exist — creating {FULL_TABLE}")
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(FULL_TABLE)
else:
    existing  = spark.table(FULL_TABLE).select("match_id").distinct()
    new_rows  = df.join(existing, on="match_id", how="left_anti")
    count     = new_rows.count()
    print(f"New rows to insert: {count:,}")

    if count > 0:
        new_rows.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable(FULL_TABLE)
        print(f"Insert complete.")
    else:
        print("No new records — table already up to date.")
# ──────────────────────────────────────────────────────────────────

# COMMAND ----------

# ── STEP 7: VERIFY ────────────────────────────────────────────────
result = spark.table(FULL_TABLE)
print(f"\n── bronze.raw_match_extractions ──────────────────")
print(f"Total rows:      {result.count():,}")
print(f"Unique players:  {result.select('player_id').distinct().count():,}")
print(f"Unique matches:  {result.select('match_id').distinct().count():,}")
print(f"Date range:      {result.agg(F.min('match_date'), F.max('match_date')).collect()[0]}")
result.groupBy("prompt_version").count().orderBy("count", ascending=False).show()
# ──────────────────────────────────────────────────────────────────
