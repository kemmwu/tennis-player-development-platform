# Runbook: Adding a New Data Source

Follow these 5 steps to add any new data source to this platform.

## Step 1: Upload raw file to Volume
Upload the file to:
/Volumes/tennis_dev/bronze/raw_files/

## Step 2: Create ingestion notebook
Copy ingestion/bronze_students.py as a template.
Update:
- TABLE name
- CHECKPOINT_PATH
- Schema definition
- pathGlobFilter to match your filename
- Quality filter conditions
- Deduplication key (the primary key of your table)

## Step 3: Test the notebook manually
Run in Databricks. Verify:
- Row count matches expected
- Metadata columns present (_ingested_at, _source_file, _record_hash)
- Idempotency: second run inserts 0 new rows

## Step 4: Add to Databricks Workflow
Go to Workflows → tennis_ingestion_pipeline → Add task
Set depends_on to the last existing task.

## Step 5: Update documentation
- Add table definition to docs/schema.md
- Add design decision to docs/decisions.md if the source required a non-obvious choice
- Update workflow_config.yml