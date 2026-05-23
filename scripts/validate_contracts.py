# Databricks notebook source
# validate_contracts.py
# Validates Bronze tables against data contracts
# Fails loudly if schema or SLA is violated

# COMMAND ----------

import yaml
from pyspark.sql import functions as F

# COMMAND ----------

CATALOG           = "tennis_dev"
SCHEMA            = "bronze"
CONTRACTS_PATH    = "/Workspace/Users/kemmwu@gmail.com/data_contracts/"
VALIDATION_PASSED = True

errors = []

# COMMAND ----------

def validate_contract(contract_path: str):
    global VALIDATION_PASSED

    with open(contract_path) as f:
        contract = yaml.safe_load(f)["contract"]

    table_name = f"{CATALOG}.{SCHEMA}.{contract['name']}"
    print(f"\nValidating: {table_name}")

    # Check table exists
    if not spark.catalog.tableExists(table_name):
        errors.append(f"TABLE MISSING: {table_name}")
        VALIDATION_PASSED = False
        return

    df = spark.table(table_name)

    # Check min rows
    row_count = df.count()
    min_rows  = contract["sla"].get("min_rows", 1)
    if row_count < min_rows:
        errors.append(
            f"ROW COUNT VIOLATION: {table_name} has {row_count} rows, "
            f"minimum is {min_rows}"
        )
        VALIDATION_PASSED = False

    # Check schema
    actual_cols = {f.name: f.dataType.simpleString()
                   for f in df.schema.fields}

    for col_spec in contract.get("schema", []):
        col_name = col_spec["name"]
        if col_name not in actual_cols:
            errors.append(
                f"MISSING COLUMN: {table_name}.{col_name}"
            )
            VALIDATION_PASSED = False
        elif not col_spec.get("nullable", True):
            null_count = df.filter(F.col(col_name).isNull()).count()
            if null_count > 0:
                errors.append(
                    f"NULL VIOLATION: {table_name}.{col_name} "
                    f"has {null_count} null values"
                )
                VALIDATION_PASSED = False

    print(f"  Rows: {row_count:,}")
    print(f"  Columns checked: {len(contract.get('schema', []))}")
    print(f"  Status: {'PASS' if VALIDATION_PASSED else 'FAIL'}")


# ── RUN VALIDATION ────────────────────────────────────────────────
contracts = [
    CONTRACTS_PATH + "raw_match_extractions.yml",
    CONTRACTS_PATH + "raw_training_sessions.yml",
]

for contract_path in contracts:
    validate_contract(contract_path)

# ── SUMMARY ───────────────────────────────────────────────────────
print(f"\n{'='*50}")
if VALIDATION_PASSED:
    print("ALL CONTRACTS PASSED")
else:
    print("CONTRACT VIOLATIONS FOUND:")
    for error in errors:
        print(f"  - {error}")
    raise Exception(
        f"Data contract validation failed with {len(errors)} violation(s)"
    )
# ──────────────────────────────────────────────────────────────────
