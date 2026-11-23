# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %sql
# MAGIC SELECT 1 AS connection_check

# COMMAND ----------

from pathlib import Path
import json

# Copy this Git folder's absolute path from its Workspace menu; include /Workspace/.
repo_root = Path("/Workspace/Users/jayakumar.ravipati.irs@catalyte.ai/to6-databricks-challenge-starter")
assert (repo_root / "README.md").is_file(), "Check the Git folder path"
practice_result = spark.sql("SELECT 1 AS connection_check").first().asDict()
assert practice_result == {"connection_check": 1}
(repo_root / "outputs").mkdir(exist_ok=True)
(repo_root / "outputs/connection-practice.json").write_text(
    json.dumps(practice_result) + "\n", encoding="utf-8"
)

# COMMAND ----------

import hashlib

INPUT_SCHEMA = "training.to6_challenge3_inputs"
INITIAL_TABLE = f"{INPUT_SCHEMA}.returns_initial"
CORRECTED_TABLE = f"{INPUT_SCHEMA}.returns_corrected"
LINEAGE_VIEW = f"{INPUT_SCHEMA}.source_lineage"
workspace_user = spark.sql("SELECT current_user() AS username").first()["username"]
OUTPUT_SCHEMA = "training.to6_c3_" + hashlib.sha256(workspace_user.encode("utf-8")).hexdigest()[:12]
print({"input_schema": INPUT_SCHEMA, "output_schema": OUTPUT_SCHEMA})

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Understand the input
# MAGIC Record Catalog descriptions, sample observations and the supplied lineage source.

# COMMAND ----------

# DBTITLE 1,Understand the input: descriptions, lineage, samples, full-batch profile
# Section 1: understand both delivered batches (focus: returns_corrected). Observations only;
# the formal quality checks are in section 2.
from pyspark.sql import functions as F

REQUIRED = ["return_id", "tax_year", "income", "deduction"]
BATCHES = {"initial": INITIAL_TABLE, "corrected": CORRECTED_TABLE}
corrected = spark.table(CORRECTED_TABLE)

# 1a. Catalog descriptions and column definitions
for name, t in BATCHES.items():
    table_comment = spark.sql(f"DESCRIBE TABLE EXTENDED {t}").filter("col_name = 'Comment'").first()["data_type"]
    print(f"{t}: {table_comment}")
    display(spark.sql(f"DESCRIBE TABLE {t}"))

# 1b. Supplied lineage view: upstream source of each delivery
display(spark.table(LINEAGE_VIEW))

# 1c. Sample rows (at least three per batch)
for name, t in BATCHES.items():
    print(f"Sample rows: {name}")
    display(spark.table(t).orderBy("tax_year", "return_id").limit(5))

# 1d. Full-batch profile by batch and year
all_rows = spark.table(INITIAL_TABLE).withColumn("batch", F.lit("initial")).unionByName(
    corrected.withColumn("batch", F.lit("corrected")))
profile = all_rows.groupBy("batch", "tax_year").agg(
    F.count("*").alias("rows"),
    F.countDistinct("return_id").alias("distinct_return_ids"),
    *[F.sum(F.col(c).isNull().cast("int")).alias(f"missing_{c}") for c in REQUIRED],
    F.sum((F.col("income") < 0).cast("int")).alias("negative_income"),
    F.sum((F.col("income") == 0).cast("int")).alias("zero_income"),
    F.min("income").alias("min_income"),
    F.max("income").alias("max_income"),
).orderBy("batch", "tax_year")
display(profile)

# 1e. Duplicate return_ids across each whole batch (not only within a year)
display(all_rows.groupBy("batch", "return_id").count().filter("count > 1").withColumnRenamed("count", "copies"))

# 1f. Records with a missing required value
display(all_rows.filter(" OR ".join(f"{c} IS NULL" for c in REQUIRED)))

# 1g. Zero-income records (valid; ratio will be NULL later)
display(all_rows.filter("income = 0").select("batch", "return_id", "tax_year", "income", "deduction").orderBy("batch", "return_id"))

# COMMAND ----------

# DBTITLE 1,Section 1 source reference and observations
# MAGIC %md
# MAGIC ### Source reference and explanation
# MAGIC
# MAGIC * **Inputs:** `training.to6_challenge3_inputs.returns_initial` and `training.to6_challenge3_inputs.returns_corrected`.
# MAGIC * **Catalog descriptions:**
# MAGIC   * Initial: "Initial synthetic delivery from parsed_source; known delivery defects. Validate before feature output."
# MAGIC   * Corrected: "Corrected synthetic delivery from parsed_source; one row per return. Validate required values and return_id uniqueness."
# MAGIC   * The columns have no comments.
# MAGIC * **Lineage (`source_lineage`):** both tables derive from `training.to6_challenge3_inputs.parsed_source`. The corrected delivery uses the same validation rules.
# MAGIC * **Row meaning:** one row = one tax return.
# MAGIC * **Unique key:** `return_id`. `taxpayer_id` is the filer and repeats across years (F0 files R01 in 2022 and R05 in 2023), so it is not a key.
# MAGIC * **Required fields:** `return_id`, `tax_year`, `income`, `deduction`.
# MAGIC
# MAGIC ### Full-batch observations
# MAGIC
# MAGIC | Batch | Year | Rows | Distinct IDs | Missing required | Negative income | Zero income |
# MAGIC | --- | --- | --- | --- | --- | --- | --- |
# MAGIC | initial | 2022 | 5 | 4 | 1 (R02 income) | 0 | 1 (R03) |
# MAGIC | initial | 2023 | 4 | 4 | 0 | 0 | 0 |
# MAGIC | initial | 2024 | 4 | 4 | 0 | 0 | 1 (R09) |
# MAGIC | corrected | 2022 | 4 | 4 | 0 | 0 | 1 (R03) |
# MAGIC | corrected | 2023 | 4 | 4 | 0 | 0 | 0 |
# MAGIC | corrected | 2024 | 4 | 4 | 0 | 0 | 1 (R09) |
# MAGIC
# MAGIC * **Duplicates:** only in the initial batch, where R01 appears twice.
# MAGIC * **Missing values:** only in the initial batch, where R02 (2022) has NULL `income`. In the corrected batch R02 has income 200.
# MAGIC * **Zero income:** R03 (2022) and R09 (2024) in both batches. These are valid returns.
# MAGIC
# MAGIC ### Impact on the next steps
# MAGIC
# MAGIC * Section 2 needs a **uniqueness** check on `return_id` and a **completeness** check on the required fields. Both run before any feature output.
# MAGIC * Unknown income is never replaced with 0, and duplicates are not dropped silently.
# MAGIC * Zero-income returns stay in the feature table with a NULL ratio.
# MAGIC * These are observations only. They do not replace the formal checks in section 2.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Decide which batch is usable
# MAGIC Keep the initial rejection evidence and apply the same checks to the replacement.

# COMMAND ----------

# DBTITLE 1,Validate both batches, preserve rejection evidence, gate feature output
# Section 2: the SAME rules for both batches. Unknown amounts are never replaced with 0; no rows are dropped.
from pyspark.sql import Window

def run_validation(table_name):
    df = spark.table(table_name)
    dup_rows = df.groupBy("return_id").count().filter("count > 1").agg(F.coalesce(F.sum("count"), F.lit(0))).first()[0]
    checks = [
        ("uniqueness", "return_id is unique", int(dup_rows)),
        ("completeness", "income is not NULL", df.filter("income IS NULL").count()),
        ("validity", "income is not negative", df.filter("income < 0").count()),
        ("completeness", "return_id, tax_year, deduction not NULL", df.filter("return_id IS NULL OR tax_year IS NULL OR deduction IS NULL").count()),
        ("validity", "deduction is not negative", df.filter("deduction < 0").count()),
    ]
    results = [{"table": table_name, "rule_type": t, "rule": r, "violating_rows": n, "status": "PASS" if n == 0 else "FAIL"}
               for t, r, n in checks]
    return ("ACCEPTED" if all(x["status"] == "PASS" for x in results) else "REJECTED"), results

def affected_records(table_name):
    copies = F.count("*").over(Window.partitionBy("return_id"))
    null_required = F.greatest(*[F.col(c).isNull() for c in REQUIRED])
    negative = (F.col("income") < 0) | (F.col("deduction") < 0)
    return (spark.table(table_name).withColumn("_copies", copies)
        .withColumn("failed_rule", F.concat_ws("; ",
            F.when(F.col("_copies") > 1, F.lit("uniqueness")),
            F.when(null_required, F.lit("completeness")),
            F.when(negative, F.lit("validity"))))
        .withColumn("rejection_reason", F.concat_ws("; ",
            F.when(F.col("_copies") > 1, F.concat(F.lit("duplicate return_id ("), F.col("_copies").cast("string"), F.lit(" copies)"))),
            *[F.when(F.col(c).isNull(), F.lit(f"missing required {c}")) for c in REQUIRED],
            F.when(negative, F.lit("negative amount"))))
        .filter(F.col("failed_rule") != "")
        .withColumn("source_table", F.lit(table_name))
        .withColumn("validated_at", F.current_timestamp())
        .drop("_copies").orderBy("return_id"))

def require_accepted(table_name, status):
    """Gate: feature output may only be written from an ACCEPTED batch."""
    if status != "ACCEPTED":
        raise ValueError(f"{table_name} {status}: feature output blocked")
    return table_name

initial_status, initial_checks = run_validation(INITIAL_TABLE)
corrected_status, corrected_checks = run_validation(CORRECTED_TABLE)
display(spark.createDataFrame(initial_checks + corrected_checks))

initial_affected = affected_records(INITIAL_TABLE)
corrected_affected = affected_records(CORRECTED_TABLE)
display(initial_affected)
print("Affected rows in corrected batch:", corrected_affected.count())

# Preserve the initial rejection evidence BEFORE any feature output is written
REJECTION_TABLE = f"{OUTPUT_SCHEMA}.initial_rejection"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {OUTPUT_SCHEMA}")
initial_affected.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(REJECTION_TABLE)

try:
    require_accepted(INITIAL_TABLE, initial_status)
    raise AssertionError("Initial batch unexpectedly passed the gate")
except ValueError as e:
    gate_message = str(e)
print("Gate on initial batch:", gate_message)

saved_evidence = spark.table(REJECTION_TABLE)
(repo_root / "outputs/initial_rejection.json").write_text(json.dumps({
    "rejection_reference": REJECTION_TABLE,
    "source_table": INITIAL_TABLE,
    "status": initial_status,
    "rule_results": initial_checks,
    "gate_result": gate_message,
    "affected_records": [r.asDict() for r in saved_evidence.orderBy("return_id").collect()],
    "corrected_status": corrected_status,
    "corrected_rule_results": corrected_checks,
    "next_action": "Reject the whole initial batch; use returns_corrected, which passes the same rules.",
}, indent=2, default=str) + "\n", encoding="utf-8")

# Verify the decision and the preserved evidence by reading it back
assert initial_status == "REJECTED" and corrected_status == "ACCEPTED"
assert saved_evidence.count() == 3
assert saved_evidence.filter("return_id = 'R02' AND income IS NULL").count() == 1   # not zero-filled
BATCH_TABLE = require_accepted(CORRECTED_TABLE, corrected_status)
print({"initial": initial_status, "corrected": corrected_status,
       "rejection_table": REJECTION_TABLE, "batch_for_features": BATCH_TABLE})

# COMMAND ----------

# DBTITLE 1,Section 2 validation findings
# MAGIC %md
# MAGIC ### Validation results (same rules for both batches)
# MAGIC
# MAGIC | Rule | Initial: violating rows | Initial | Corrected: violating rows | Corrected |
# MAGIC | --- | --- | --- | --- | --- |
# MAGIC | return_id is unique | 2 | FAIL | 0 | PASS |
# MAGIC | income is not NULL | 1 | FAIL | 0 | PASS |
# MAGIC | income is not negative | 0 | PASS | 0 | PASS |
# MAGIC | return_id, tax_year, deduction not NULL | 0 | PASS | 0 | PASS |
# MAGIC | deduction is not negative | 0 | PASS | 0 | PASS |
# MAGIC
# MAGIC **Decision:** `returns_initial` is **REJECTED**. `returns_corrected` is **ACCEPTED** and set as `BATCH_TABLE` for feature output.
# MAGIC
# MAGIC **Initial batch affected records (actual rows):**
# MAGIC
# MAGIC | return_id | tax_year | income | deduction | failed_rule | rejection_reason |
# MAGIC | --- | --- | --- | --- | --- | --- |
# MAGIC | R01 | 2022 | 100 | 20 | uniqueness | duplicate return_id (2 copies) |
# MAGIC | R01 | 2022 | 100 | 20 | uniqueness | duplicate return_id (2 copies) |
# MAGIC | R02 | 2022 | NULL | 33 | completeness | missing required income |
# MAGIC
# MAGIC * **Why it fails:** R01 would be counted twice in the features. R02's ratio cannot be computed, and setting its income to 0 would invent a value.
# MAGIC * **Evidence preserved:** `training.to6_c3_aacde3d25b55.initial_rejection` (3 rows; R02 income kept as NULL) and `outputs/initial_rejection.json` (rows, rule results, gate message, next action). Both were read back and verified.
# MAGIC * **Gate:** `require_accepted()` raised "returns_initial REJECTED: feature output blocked" before any feature output.
# MAGIC * **Corrected batch:** 0 affected rows, 12 rows with 12 distinct IDs, income from 0 to 1,200.
# MAGIC * **Not verified:** the negative-income rule could not be tested against a real failure, because neither batch has negative amounts.
# MAGIC * **Next action:** build the features from `returns_corrected`. No check was removed, nothing was zero-filled, and no row was silently dropped.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Deliver a checked feature table
# MAGIC Implement the task's data contract and verify the saved result by reading it back.

# COMMAND ----------

# DBTITLE 1,Build, save and verify return_features
# Section 3: build return_features from the accepted batch only (gate re-checked before writing).
BATCH_TABLE = require_accepted(CORRECTED_TABLE, corrected_status)
FEATURES_TABLE = f"{OUTPUT_SCHEMA}.return_features"
accepted = spark.table(BATCH_TABLE)

# Need-to-know: only return_id, tax_year and the ratio (no taxpayer_id, no raw income/deduction).
# Zero income -> NULL ratio; the return stays in the table.
return_features = accepted.select(
    "return_id",
    "tax_year",
    F.when(F.col("income") == 0, F.lit(None).cast("double"))
     .otherwise(F.col("deduction").cast("double") / F.col("income"))
     .alias("deduction_to_income"),
)
return_features.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(FEATURES_TABLE)

# ---- Verify by reading the SAVED table back ----
saved = spark.table(FEATURES_TABLE)
display(saved.orderBy("tax_year", "return_id"))

# 1. Exactly the three contract columns
assert saved.columns == ["return_id", "tax_year", "deduction_to_income"], saved.columns

# 2. Every accepted return exactly once
source_ids = {r.return_id for r in accepted.select("return_id").collect()}
saved_ids = [r.return_id for r in saved.select("return_id").collect()]
assert len(saved_ids) == len(set(saved_ids)) == len(source_ids) and set(saved_ids) == source_ids

# 3. Nonzero case: expected value derived independently from the raw source row
src_r01 = spark.sql(f"SELECT income, deduction FROM {BATCH_TABLE} WHERE return_id = 'R01'").first()
expected_r01 = src_r01.deduction / src_r01.income          # 20 / 100 = 0.2, plain Python
actual_r01 = saved.filter("return_id = 'R01'").first().deduction_to_income
assert abs(actual_r01 - expected_r01) < 1e-12, (actual_r01, expected_r01)

# 4. Zero-income returns present with NULL ratio (and the only NULLs)
zero_income_ids = {r.return_id for r in accepted.filter("income = 0").select("return_id").collect()}
null_ratio_ids = {r.return_id for r in saved.filter("deduction_to_income IS NULL").select("return_id").collect()}
assert zero_income_ids and null_ratio_ids == zero_income_ids, (zero_income_ids, null_ratio_ids)

# 5. tax_year carried over unchanged
year_mismatch = (saved.join(accepted.select("return_id", F.col("tax_year").alias("src_year")), "return_id")
                 .filter("tax_year <> src_year").count())
assert year_mismatch == 0

print({"table": FEATURES_TABLE, "columns": saved.columns, "rows": len(saved_ids),
       "R01_expected": expected_r01, "R01_saved": actual_r01,
       "zero_income_ids_with_null_ratio": sorted(null_ratio_ids)})

# COMMAND ----------

# DBTITLE 1,return_features verification findings
# MAGIC %md
# MAGIC ### return_features: contract and verification
# MAGIC
# MAGIC * **Table:** `training.to6_c3_aacde3d25b55.return_features`. It is built only from the accepted batch `returns_corrected`, and the gate is re-checked before writing.
# MAGIC * **Columns:** exactly `return_id` (string), `tax_year` (int), `deduction_to_income` (double). `taxpayer_id`, `income` and `deduction` are excluded (need-to-know).
# MAGIC * **Ratio:** `deduction / income`. When `income = 0` the ratio is NULL and the return is kept.
# MAGIC
# MAGIC | Check (on the saved table, read back) | Result |
# MAGIC | --- | --- |
# MAGIC | Columns are exactly the contract | pass |
# MAGIC | Every accepted return once: 12 rows, 12 distinct IDs, same ID set as the source | pass |
# MAGIC | Nonzero case R01: expected 20 / 100 = 0.2 (computed in Python from the source row); saved 0.2 | pass |
# MAGIC | Zero-income R03 (2022) and R09 (2024) present with NULL ratio, and they are the only NULLs | pass |
# MAGIC | `tax_year` matches the source for every return | pass |
# MAGIC | Independent SQL check (full outer join to the source, ratio recomputed): 12 matched, max difference 0.0, 0 year mismatches | pass |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Make the handoff usable
# MAGIC Save the required year tables. Keep references and IDs-and-values evidence for two successful runs.
# MAGIC Write SUBMISSION.md and the small exports inside this Databricks Git folder.

# COMMAND ----------

# DBTITLE 1,Build and verify year split tables
# Section 4a: year split tables built FROM THE SAVED return_features (overwrite, never append).
SPLITS = {"features_train": 2022, "features_validation": 2023, "features_test": 2024}
features_saved = spark.table(FEATURES_TABLE)

for name, year in SPLITS.items():
    (features_saved.filter(F.col("tax_year") == year)
        .write.mode("overwrite").option("overwriteSchema", "true")
        .saveAsTable(f"{OUTPUT_SCHEMA}.{name}"))

# ---- Verify by reading the saved split tables back ----
split_rows = {}
for name, year in SPLITS.items():
    t = spark.table(f"{OUTPUT_SCHEMA}.{name}")
    assert t.columns == ["return_id", "tax_year", "deduction_to_income"], (name, t.columns)
    years = {r.tax_year for r in t.select("tax_year").distinct().collect()}
    assert years == {year}, (name, years)                       # only its assigned year
    split_rows[name] = [(r.return_id, r.tax_year, r.deduction_to_income) for r in t.collect()]

all_split_ids = [rid for rows in split_rows.values() for rid, _, _ in rows]
feature_rows = {(r.return_id, r.tax_year, r.deduction_to_income) for r in features_saved.collect()}
split_union = {row for rows in split_rows.values() for row in rows}

assert len(all_split_ids) == len(set(all_split_ids)), "a return appears in more than one split / twice"
assert split_union == feature_rows, "splits do not reproduce return_features exactly"

summary = spark.createDataFrame(
    [(n, SPLITS[n], len(rows), sorted(rid for rid, _, _ in rows)) for n, rows in split_rows.items()],
    "table string, tax_year int, rows int, return_ids array<string>")
display(summary)
print({"feature_rows": len(feature_rows), "split_rows_total": len(all_split_ids),
       "distinct_ids_in_splits": len(set(all_split_ids))})

# COMMAND ----------

# DBTITLE 1,Year split findings
# MAGIC %md
# MAGIC ### Year split tables
# MAGIC
# MAGIC The split tables are built from the **saved** `return_features` table and written with overwrite, so a rerun replaces them instead of appending.
# MAGIC
# MAGIC | Table | Year | Rows | Return IDs |
# MAGIC | --- | --- | --- | --- |
# MAGIC | `training.to6_c3_aacde3d25b55.features_train` | 2022 | 4 | R01, R02, R03, R04 |
# MAGIC | `training.to6_c3_aacde3d25b55.features_validation` | 2023 | 4 | R05, R06, R07, R08 |
# MAGIC | `training.to6_c3_aacde3d25b55.features_test` | 2024 | 4 | R09, R10, R11, R12 |
# MAGIC
# MAGIC **Checks (on the saved tables, read back):**
# MAGIC * Each table has the same three columns and contains only its assigned year.
# MAGIC * The three tables hold 12 rows and 12 distinct IDs, so every accepted return is in exactly one split.
# MAGIC * Together they reproduce `return_features` exactly: same IDs, years and ratios. R03 (train) and R09 (test) keep their NULL ratio.
# MAGIC * Independent SQL check (UNION ALL of the splits compared with `return_features`): 0 IDs not exactly once, 0 wrong-year rows, 0 missing returns, 0 value mismatches.
# MAGIC
# MAGIC The two-run comparison and exports are in the next cell.

# COMMAND ----------

# DBTITLE 1,Run snapshot, two-run comparison and exports
# Section 4b: record this run's IDs and values, compare with the other run, and export small tables.
# Run 1 writes outputs/run1_snapshot.json; any later full run writes outputs/run2_snapshot.json and is compared with run 1.
from datetime import datetime, timezone

NOTEBOOK_ID = "2616693406010676"
try:
    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    host = ctx.browserHostName().get()
    org_id = ctx.workspaceId().get()
    notebook_url = f"https://{host}/editor/notebooks/{NOTEBOOK_ID}?o={org_id}"
except Exception:
    notebook_url = f"(workspace)/editor/notebooks/{NOTEBOOK_ID}"

EXPORT_TABLES = ["return_features", *SPLITS]

def table_rows(name):
    """Sorted (return_id, tax_year, deduction_to_income) rows read from the SAVED table."""
    return [[r.return_id, r.tax_year, r.deduction_to_income]
            for r in spark.table(f"{OUTPUT_SCHEMA}.{name}").orderBy("return_id").collect()]

snapshot = {
    "notebook_url": notebook_url,
    "executed_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    "output_schema": OUTPUT_SCHEMA,
    "tables": {name: table_rows(name) for name in EXPORT_TABLES},
}

out = repo_root / "outputs"
run1_path, run2_path = out / "run1_snapshot.json", out / "run2_snapshot.json"
this_run_path = run2_path if run1_path.exists() else run1_path
this_run_path.write_text(json.dumps(snapshot, indent=2) + "\n", encoding="utf-8")
print("Snapshot written:", this_run_path.name, snapshot["executed_at_utc"])

# Exports of the saved feature and split tables (IDs and values only)
for name, rows in snapshot["tables"].items():
    (out / f"{name}.json").write_text(json.dumps(
        {"table": f"{OUTPUT_SCHEMA}.{name}", "columns": ["return_id", "tax_year", "deduction_to_income"], "rows": rows},
        indent=2) + "\n", encoding="utf-8")
print("Exported:", [f"{n}.json" for n in EXPORT_TABLES])

# Compare the two runs table by table (IDs and values, not just counts)
if this_run_path == run2_path:
    run1 = json.loads(run1_path.read_text())
    run2 = json.loads(run2_path.read_text())
    per_table = {}
    for name in EXPORT_TABLES:
        a, b = run1["tables"][name], run2["tables"][name]
        ids_b = [r[0] for r in b]
        per_table[name] = {
            "run1_rows": len(a), "run2_rows": len(b),
            "identical_ids_and_values": a == b,
            "no_duplicate_ids_in_run2": len(ids_b) == len(set(ids_b)),
        }
    comparison = {
        "run1": {"notebook_url": run1["notebook_url"], "executed_at_utc": run1["executed_at_utc"]},
        "run2": {"notebook_url": run2["notebook_url"], "executed_at_utc": run2["executed_at_utc"]},
        "tables": per_table,
        "all_identical": all(v["identical_ids_and_values"] and v["no_duplicate_ids_in_run2"] for v in per_table.values()),
    }
    (out / "runs_comparison.json").write_text(json.dumps(comparison, indent=2) + "\n", encoding="utf-8")
    display(spark.createDataFrame([(n, v["run1_rows"], v["run2_rows"], v["identical_ids_and_values"], v["no_duplicate_ids_in_run2"])
                                   for n, v in per_table.items()],
                                  "table string, run1_rows int, run2_rows int, identical boolean, no_duplicates boolean"))
    assert comparison["all_identical"], comparison
    print("Run 1:", run1["executed_at_utc"], "| Run 2:", run2["executed_at_utc"], "| identical:", comparison["all_identical"])
else:
    print("Run 1 recorded. Run the notebook top to bottom again to create run 2 and the comparison.")

# COMMAND ----------

# DBTITLE 1,Two-run comparison findings
# MAGIC %md
# MAGIC ### Two complete runs and exports
# MAGIC
# MAGIC Notebook URL for both runs: https://dbc-48a16403-f66f.cloud.databricks.com/editor/notebooks/2616693406010676?o=7474648218609329
# MAGIC
# MAGIC | Run | Executed at (UTC) | Snapshot |
# MAGIC | --- | --- | --- |
# MAGIC | 1 | 2026-11-23T23:00:32+00:00 | `outputs/run1_snapshot.json` |
# MAGIC | 2 | 2026-11-23T23:03:27+00:00 | `outputs/run2_snapshot.json` |
# MAGIC
# MAGIC Both were full top-to-bottom runs with every code cell succeeding. A first attempt at run 2 failed in this cell's display step because `bool` is not a valid Spark type. It was fixed to `boolean`, and run 2 was repeated in full.
# MAGIC
# MAGIC | Table | Run 1 rows | Run 2 rows | Identical IDs and values | No duplicate IDs |
# MAGIC | --- | --- | --- | --- | --- |
# MAGIC | return_features | 12 | 12 | yes | yes |
# MAGIC | features_train | 4 | 4 | yes | yes |
# MAGIC | features_validation | 4 | 4 | yes | yes |
# MAGIC | features_test | 4 | 4 | yes | yes |
# MAGIC
# MAGIC * The comparison is of the full sorted `(return_id, tax_year, deduction_to_income)` rows read from the saved tables, not of counts. It is saved in `outputs/runs_comparison.json` (`all_identical: true`).
# MAGIC * No appended copies: after two runs `return_features` still has 12 rows and 12 distinct IDs, because every table is written with overwrite.
# MAGIC * Exports, matching run 2's snapshot: `outputs/return_features.json`, `features_train.json`, `features_validation.json`, `features_test.json`.