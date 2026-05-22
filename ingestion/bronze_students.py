# Databricks notebook source
# bronze_students.py
# Ingests student profiles from CSV in Volume into bronze.raw_students Delta table
# Supports incremental loading via Auto Loader + file hash deduplication

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Ingestion: Students
# MAGIC
# MAGIC **Source:** `/Volumes/tennis_dev/bronze/raw_files/students.csv`
# MAGIC **Target:** `tennis_dev.bronze.raw_students`
# MAGIC **Pattern:** Auto Loader · file hash dedup · append-only

# COMMAND ----------

from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType,
    IntegerType, BooleanType, LongType
)

# COMMAND ----------

# ── CONFIGURATION ─────────────────────────────────────────────────
CATALOG         = "tennis_dev"
SCHEMA          = "bronze"
TABLE           = "raw_students"
FULL_TABLE      = f"{CATALOG}.{SCHEMA}.{TABLE}"

SOURCE_PATH     = f"/Volumes/{CATALOG}/{SCHEMA}/raw_files/"
CHECKPOINT_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints/students/"
PIPELINE_VERSION = "v1.0"
# ──────────────────────────────────────────────────────────────────

# COMMAND ----------

# ── SCHEMA DEFINITION ─────────────────────────────────────────────
# Define schema explicitly — never infer schema in production
student_schema = StructType([
    StructField("student_id",                  StringType(),    True),
    StructField("intake_id",                   StringType(),    True),
    StructField("full_name",                   StringType(),    True),
    StructField("chinese_name",                StringType(),    True),
    StructField("preferred_name",              StringType(),    True),
    StructField("date_of_birth",               StringType(),    True),
    StructField("utr_rating",                  DoubleType(),    True),
    StructField("age_group",                   StringType(),    True),
    StructField("training_frequency_per_week", IntegerType(),   True),
    StructField("dominant_hand",               StringType(),    True),
    StructField("height",                      StringType(),    True),
    StructField("years_playing",               IntegerType(),   True),
    StructField("coach_id",                    StringType(),    True),
    StructField("submitted_at",                StringType(),    True),
    StructField("goals",                       StringType(),    True),
    StructField("injury_history",              StringType(),    True),
    StructField("previous_coaching",           StringType(),    True),
    StructField("competition_level",           StringType(),    True),
    StructField("contact_name",                StringType(),    True),
    StructField("contact_email",               StringType(),    True),
    StructField("kafka_offset",                LongType(),      True),
    StructField("kafka_partition",             IntegerType(),   True),
    StructField("ingested_at",                 StringType(),    True),
    StructField("is_active",                   BooleanType(),   True),
    StructField("valid_from",                  StringType(),    True),
    StructField("valid_to",                    StringType(),    True),
    StructField("is_current",                  BooleanType(),   True),
])
# ──────────────────────────────────────────────────────────────────

# COMMAND ----------

# ── STEP 1: READ WITH AUTO LOADER ─────────────────────────────────
print(f"[{datetime.now()}] Reading from {SOURCE_PATH}")

raw_df = (
    spark.readStream
    .format("cloudFiles")                      # Auto Loader format
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", CHECKPOINT_PATH + "schema/")
    .option("header", "true")
    .option("cloudFiles.inferColumnTypes", "false")
    .option("pathGlobFilter", "students.csv")   # ← add this line
    .schema(student_schema)
    .load(SOURCE_PATH)                          # ← point to folder, not file
)
# ──────────────────────────────────────────────────────────────────

# COMMAND ----------

# ── STEP 2: ADD METADATA COLUMNS ──────────────────────────────────
enriched_df = (
    raw_df
    # Source file path — where did this record come from?
    .withColumn("_source_file",      F.col("_metadata.file_path"))
    # Ingestion timestamp — when did we pick this up?
    .withColumn("_ingested_at",      F.current_timestamp())
    # Pipeline version — which version of this notebook ran?
    .withColumn("_pipeline_version", F.lit(PIPELINE_VERSION))
    # File hash — SHA-256 of the source file path for deduplication
    .withColumn("_record_hash",
        F.sha2(F.col("_metadata.file_path"), 256)
    )
)
# ──────────────────────────────────────────────────────────────────

# COMMAND ----------

# ── STEP 3: DATA QUALITY ASSERTIONS ───────────────────────────────
# These checks happen BEFORE writing to Bronze
# Any record that fails gets dropped and logged

validated_df = (
    enriched_df
    # Critical fields must not be null
    .filter(F.col("student_id").isNotNull())
    .filter(F.col("full_name").isNotNull())
    # UTR must be in valid range if present
    .filter(
        F.col("utr_rating").isNull() |
        (F.col("utr_rating").between(0.0, 16.5))
    )
    # competition_level must be valid value
    .filter(
        F.col("competition_level").isNull() |
        F.col("competition_level").isin("competitive", "recreational")
    )
)

print("Data quality filters applied.")
# ──────────────────────────────────────────────────────────────────

# COMMAND ----------

# ── STEP 4: WRITE TO BRONZE DELTA TABLE ───────────────────────────
# Using foreachBatch for deduplication logic

def upsert_to_bronze(batch_df, batch_id):
    """
    Write batch to bronze table.
    Deduplicates by student_id — if student already exists, skip.
    This keeps Bronze append-only while preventing exact duplicates.
    """
    if batch_df.count() == 0:
        print(f"Batch {batch_id}: empty, skipping.")
        return

    # Check if table exists
    table_exists = spark.catalog.tableExists(FULL_TABLE)

    if not table_exists:
        # First run — create the table
        print(f"Batch {batch_id}: creating table {FULL_TABLE}")
        (
            batch_df
            .write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy("age_group")
            .saveAsTable(FULL_TABLE)
        )
    else:
        # Subsequent runs — only insert records not already present
        existing_ids = (
            spark.table(FULL_TABLE)
            .select("student_id")
            .distinct()
        )
        new_records = batch_df.join(
            existing_ids,
            on="student_id",
            how="left_anti"     # only rows NOT in existing table
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


# Run the streaming write
query = (
    validated_df
    .writeStream
    .foreachBatch(upsert_to_bronze)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)     # process all available files then stop
    .start()
)

query.awaitTermination()
print(f"[{datetime.now()}] Streaming query complete.")
# ──────────────────────────────────────────────────────────────────

# COMMAND ----------

# ── STEP 5: VERIFY THE OUTPUT ─────────────────────────────────────
result = spark.table(FULL_TABLE)
count  = result.count()

print("\n── bronze.raw_students ──────────────────────────")
print(f"Total rows:        {count:,}")
print(f"Unique students:   {result.select('student_id').distinct().count():,}")
print("Age groups:")
result.groupBy("age_group").count().orderBy("age_group").show()
print("Competition level:")
result.groupBy("competition_level").count().show()
# ──────────────────────────────────────────────────────────────────

# COMMAND ----------

# ── STEP 6: IDEMPOTENCY TEST ──────────────────────────────────────
# Run this cell manually to confirm re-running the notebook
# does NOT create duplicate records

print("Idempotency check:")
print(f"  Total rows:   {spark.table(FULL_TABLE).count():,}")
print(f"  Unique IDs:   {spark.table(FULL_TABLE).select('student_id').distinct().count():,}")
print("  If these two numbers are equal, deduplication is working correctly.")
# ──────────────────────────────────────────────────────────────────
