# My dataset submission

Complete this file in Databricks. Keep README.md as the course navigation page.

## Inputs and outputs

Full input, lineage and output table names; what one row represents; ratio definition.

* Rejected input: `training.to6_challenge3_inputs.returns_initial`.
* Accepted input: `training.to6_challenge3_inputs.returns_corrected`.
* Lineage: `training.to6_challenge3_inputs.source_lineage` shows both derive from `training.to6_challenge3_inputs.parsed_source`.
* One row = one return. `return_id` is the unique key. Required fields: `return_id`, `tax_year`, `income`, `deduction`.
* Output schema: `training.to6_c3_aacde3d25b55`.
* Feature table: `training.to6_c3_aacde3d25b55.return_features`, one row per accepted return, columns `return_id`, `tax_year`, `deduction_to_income`.
* Ratio definition: `deduction_to_income = deduction / income`; NULL when `income = 0` (the return is kept).
* Year tables, built from the saved `return_features`:
  * `training.to6_c3_aacde3d25b55.features_train`: 2022, 4 rows (R01–R04).
  * `training.to6_c3_aacde3d25b55.features_validation`: 2023, 4 rows (R05–R08).
  * `training.to6_c3_aacde3d25b55.features_test`: 2024, 4 rows (R09–R12).

## Checks and decisions

Source references; initial rejection reference, affected-record export and reasons; replacement
checks; independently derived nonzero expected value; saved-table readback including zero income.

Same rules applied to both batches (notebook section 2, `run_validation()`):

| Rule | Initial | Corrected |
| --- | --- | --- |
| return_id is unique | FAIL (2 rows) | PASS |
| income is not NULL | FAIL (1 row) | PASS |
| income is not negative | PASS | PASS |
| return_id, tax_year, deduction not NULL | PASS | PASS |
| deduction is not negative | PASS | PASS |

* Initial batch: REJECTED. R01 (2022) appears twice (uniqueness). R02 (2022) has NULL income (completeness), which is kept as NULL and never set to 0.
* Rejection reference: `training.to6_c3_aacde3d25b55.initial_rejection`, 3 affected rows with `failed_rule`, `rejection_reason`, `source_table` and `validated_at`.
* Affected-record export: `outputs/initial_rejection.json`.
* Gate: `require_accepted()` blocked feature output from the initial batch before anything was written.
* Corrected batch: ACCEPTED. 12 rows, 12 distinct IDs, 4 per year for 2022–2024, no missing or negative values. Zero-income R03 (2022) and R09 (2024) are valid.
* Not verified: the negative-income rule has no real failing case in either batch.

Feature table checks (notebook section 3, on the saved table read back):

* Columns are exactly `return_id`, `tax_year`, `deduction_to_income`. `taxpayer_id` and raw amounts are excluded (need-to-know).
* 12 rows, 12 distinct IDs, the same ID set as `returns_corrected`.
* Nonzero expected value: R01 expected 20 / 100 = 0.2, computed in Python from the source row; the saved value is 0.2.
* Zero income: R03 (2022) and R09 (2024) are present with a NULL ratio, and they are the only NULLs.
* `tax_year` matches the source for every return.
* Independent SQL check (full outer join to the source, ratio recomputed): max difference 0.0, 0 year mismatches.

Year split checks (notebook section 4, on the saved tables read back):

* Each split contains only its assigned year and the same three columns.
* Together the splits hold 12 rows and 12 distinct IDs, so every accepted return is in exactly one split.
* The union of the splits equals `return_features`: same IDs, years and ratios, including NULL for R03 and R09.
* All tables are written with overwrite, so a rerun replaces rather than appends.
* Independent SQL check (UNION ALL of the splits): 0 IDs not exactly once, 0 wrong-year rows, 0 missing returns, 0 value mismatches.

## Two successful runs

For each run: notebook URL and execution timestamp, or Jobs URL/ID. Name the retained exports
or comparison evidence showing identical return IDs and values in the feature and year tables.
Explain how you checked year assignment, one copy of every accepted return and no appended copies.

Both runs were complete top-to-bottom interactive runs of the same notebook. Every code cell succeeded.

* Notebook URL: https://dbc-48a16403-f66f.cloud.databricks.com/editor/notebooks/2616693406010676?o=7474648218609329
* Run 1: 2026-11-23T23:00:32+00:00 (UTC), snapshot `outputs/run1_snapshot.json`.
* Run 2: 2026-11-23T23:03:27+00:00 (UTC), snapshot `outputs/run2_snapshot.json`.
* A first attempt at run 2 failed in the comparison display step (`bool` is not a valid Spark type). It was fixed to `boolean`, and run 2 was repeated in full. The run above is that repeat.

Comparison evidence: `outputs/runs_comparison.json` (`all_identical: true`). Each snapshot holds the full sorted `(return_id, tax_year, deduction_to_income)` rows read from the saved tables.

| Table | Run 1 rows | Run 2 rows | Identical IDs and values |
| --- | --- | --- | --- |
| return_features | 12 | 12 | yes |
| features_train | 4 | 4 | yes |
| features_validation | 4 | 4 | yes |
| features_test | 4 | 4 | yes |

* Year assignment: each split table was read back and contains only its assigned year (2022, 2023, 2024). An independent SQL check found 0 wrong-year rows.
* One copy of every accepted return: the splits together hold 12 rows and 12 distinct IDs, and their union equals `return_features` exactly. The SQL check found 0 IDs not exactly once and 0 missing returns.
* No appended copies: all tables are written with overwrite. After two runs `return_features` still has 12 rows and 12 distinct IDs, and the run 2 snapshot has no duplicate IDs in any table.

## Final files and limitations

List final exports under outputs/, distinguishing practice files. State anything not verified.

Final exports in `outputs/`:

* `return_features.json`: saved feature table (12 rows).
* `features_train.json`, `features_validation.json`, `features_test.json`: saved year tables (4 rows each).
* `initial_rejection.json`: affected initial records with rule, reason, rule results, gate message and next action.
* `run1_snapshot.json`, `run2_snapshot.json`: IDs and values from each run.
* `runs_comparison.json`: table-by-table comparison of the two runs.

Practice file (ungraded, not part of the task): `connection-practice.json`.

Limitations and items not verified:

* The negative-amount rule could not be tested against a real failing case, because neither batch contains negative values.
* The checks enforce this task's data contract only. They do not prove that every source of model leakage is removed. Labels and prediction-time availability are for later modelling work.
* The final commit SHA is reported in Classroom, not here.
