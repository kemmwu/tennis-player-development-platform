# Databricks notebook source
# bronze_training_sessions.py
# Ingests synthetic training session statistics from Parquet file in Volume
# Writes to tennis_dev.bronze.raw_training_sessions
# Uses pandas batch approach to avoid Auto Loader schema caching issues

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Ingestion: Training Sessions (Synthetic)
# MAGIC
# MAGIC **Source:** `/Volumes/tennis_dev/bronze/raw_files/training_sessions.parquet`
# MAGIC **Target:** `tennis_dev.bronze.raw_training_sessions`
# MAGIC **Pattern:** Pandas batch read · type casting · dedup by session_id · append

# COMMAND ----------

import pandas as pd
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DoubleType, LongType, BooleanType

# COMMAND ----------

# ── CONFIGURATION ─────────────────────────────────────────────────
CATALOG          = "tennis_dev"
SCHEMA           = "bronze"
TABLE            = "raw_training_sessions"
FULL_TABLE       = f"{CATALOG}.{SCHEMA}.{TABLE}"
SOURCE_FILE      = f"/Volumes/{CATALOG}/{SCHEMA}/raw_files/training_sessions.parquet"
PIPELINE_VERSION = "v2.0"
# ──────────────────────────────────────────────────────────────────

# COMMAND ----------

# ── STEP 1: READ REAL DATA TO PRESERVE ───────────────────────────
real_data = spark.table(FULL_TABLE).filter(
    F.col("prompt_version") != "synthetic"
)
print(f"Real rows to preserve: {real_data.count()}")
# ──────────────────────────────────────────────────────────────────

# COMMAND ----------

# ── STEP 2: READ SYNTHETIC PARQUET WITH PANDAS ───────────────────
print(f"[{datetime.now()}] Reading {SOURCE_FILE}")
pdf = pd.read_parquet(SOURCE_FILE)
print(f"Parquet rows: {len(pdf):,}")
# ──────────────────────────────────────────────────────────────────

# COMMAND ----------

# ── STEP 3: CREATE SPARK DATAFRAME + CAST TYPES ──────────────────
syn_data = spark.createDataFrame(pdf)

syn_data = (
    syn_data
    .withColumn("session_date",                     F.col("session_date").cast(DateType()))
    .withColumn("shots_in",                         F.col("shots_in").cast(DoubleType()))
    .withColumn("shots_per_hour",                   F.col("shots_per_hour").cast(LongType()))
    .withColumn("longest_rally",                    F.col("longest_rally").cast(LongType()))
    .withColumn("rallies_above_5_shots",            F.col("rallies_above_5_shots").cast(DoubleType()))
    .withColumn("serves_in_ad",                     F.col("serves_in_ad").cast(DoubleType()))
    .withColumn("serves_in_deuce",                  F.col("serves_in_deuce").cast(DoubleType()))
    .withColumn("avg_serve_speed_ad",               F.col("avg_serve_speed_ad").cast(LongType()))
    .withColumn("avg_serve_speed_deuce",            F.col("avg_serve_speed_deuce").cast(LongType()))
    .withColumn("returns_in_ad",                    F.col("returns_in_ad").cast(DoubleType()))
    .withColumn("returns_in_deuce",                 F.col("returns_in_deuce").cast(DoubleType()))
    .withColumn("avg_return_speed_ad",              F.col("avg_return_speed_ad").cast(LongType()))
    .withColumn("avg_return_speed_deuce",           F.col("avg_return_speed_deuce").cast(LongType()))
    .withColumn("forehand_cross_court_in",          F.col("forehand_cross_court_in").cast(DoubleType()))
    .withColumn("forehand_down_the_line_in",        F.col("forehand_down_the_line_in").cast(DoubleType()))
    .withColumn("forehand_avg_cross_court_speed",   F.col("forehand_avg_cross_court_speed").cast(LongType()))
    .withColumn("forehand_avg_down_the_line_speed", F.col("forehand_avg_down_the_line_speed").cast(LongType()))
    .withColumn("forehand_cross_court_deep",        F.col("forehand_cross_court_deep").cast(DoubleType()))
    .withColumn("forehand_down_the_line_deep",      F.col("forehand_down_the_line_deep").cast(DoubleType()))
    .withColumn("backhand_cross_court_in",          F.col("backhand_cross_court_in").cast(DoubleType()))
    .withColumn("backhand_down_the_line_in",        F.col("backhand_down_the_line_in").cast(DoubleType()))
    .withColumn("backhand_avg_cross_court_speed",   F.col("backhand_avg_cross_court_speed").cast(LongType()))
    .withColumn("backhand_avg_down_the_line_speed", F.col("backhand_avg_down_the_line_speed").cast(LongType()))
    .withColumn("backhand_cross_court_deep",        F.col("backhand_cross_court_deep").cast(DoubleType()))
    .withColumn("backhand_down_the_line_deep",      F.col("backhand_down_the_line_deep").cast(DoubleType()))
    .withColumn("pages_processed",                  F.col("pages_processed").cast(LongType()))
    .withColumn("extraction_confidence",            F.col("extraction_confidence").cast(DoubleType()))
    .withColumn("_source_file",                     F.lit(SOURCE_FILE))
    .withColumn("_record_hash",                     F.sha2(F.col("session_id"), 256))
    .withColumn("_pipeline_version",                F.lit(PIPELINE_VERSION))
)
# ──────────────────────────────────────────────────────────────────

# COMMAND ----------

# ── STEP 4: COMBINE REAL + SYNTHETIC AND OVERWRITE ───────────────
combined = real_data.unionByName(syn_data, allowMissingColumns=True)
print(f"Combined rows: {combined.count():,}")

combined.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(FULL_TABLE)

print("Overwrite complete.")
# ──────────────────────────────────────────────────────────────────

# COMMAND ----------

# ── STEP 5: VERIFY ────────────────────────────────────────────────
result = spark.table(FULL_TABLE)
print(f"\n── bronze.raw_training_sessions ──────────────────")
print(f"Total rows:      {result.count():,}")
print(f"Unique players:  {result.select('player_id').distinct().count():,}")
print(f"Unique sessions: {result.select('session_id').distinct().count():,}")
print(f"Date range:      {result.agg(F.min('session_date'), F.max('session_date')).collect()[0]}")
result.groupBy("prompt_version").count().orderBy("count", ascending=False).show()
# ──────────────────────────────────────────────────────────────────
