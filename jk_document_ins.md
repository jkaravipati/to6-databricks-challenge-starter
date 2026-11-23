# Challenge 3 guide: prepare the next team's case-selection dataset

Plain-language guide built from the project documents in this folder:
[README.md](README.md), [START_HERE.md](START_HERE.md), [CHALLENGE.md](CHALLENGE.md),
[SUBMISSION.md](SUBMISSION.md), [OPTIONAL_TOOLS.md](OPTIONAL_TOOLS.md) and
[SESSION_RECORD.md](SESSION_RECORD.md). If anything here differs from those files, the source
documents win; [CHALLENGE.md](CHALLENGE.md) is the task contract.

Items marked **[Run in Databricks]** are results you can only get by running the work in
Databricks; this guide does not supply them.

## 1. Challenge summary

* **What it is:** Challenge 3, the first Databricks Challenge ([README.md](README.md)).
* **The story:** a modelling team starts next week. A delivery of synthetic tax-return records has arrived, but the data owner warns that the first batch contains mistakes ([CHALLENGE.md](CHALLENGE.md)).
* **Your job, in one line:** reject the faulty batch, check its replacement, build a feature table and split it into training (2022), validation (2023) and test (2024) years.
* **Why it matters:**
  * The next team will build models on your data; bad rows (missing values, duplicates) would silently corrupt their work.
  * Your notebook and data contract will support later modelling and capstone work.
  * Need-to-know rule: the feature table must not expose the filer identifier or raw amounts.
* **What it is not:** you do not train a model, create target labels or build an XML parser. The checks do not prove all model leakage is eliminated; later modelling must handle that.
* **Where you work:** entirely in Databricks in the browser. Do not develop locally and import it. Genie Code is optional.
* **Builds on the week's practice:** Monday's Catalog and source inspection, Tuesday's completeness, uniqueness and need-to-know checks, Wednesday's saved features and year splits. Names and year boundaries differ from the exercises; read this task's contract rather than copying.

## 2. Decision flowchart

Work through the decisions in order. Each one says what to do for each answer: **STOP**, **RETRY** or **PROCEED**.

1. **Is the environment ready?** Your fork and Git folder exist, the branch is `main`, the setup cell prints your `OUTPUT_SCHEMA`, `SELECT 1 AS connection_check` returns 1, and the practice Commit & Push reaches GitHub.
   * Yes: **PROCEED** to decision 2.
   * No: **STOP**. Missing access is an environment issue; ask the trainer, then **RETRY** decision 1.
2. **Has the trainer opened the Classroom assignment?**
   * Yes: **PROCEED** to inspect the inputs (Outcome 1), then decision 3.
   * No: **STOP** and wait. Do not begin the assessed task.
3. **Does `returns_initial` pass the completeness and uniqueness checks?**
   * No (expected): save the affected records and rejection reasons, write no feature output from this batch, then **PROCEED** to decision 4.
   * Yes (unexpected): **STOP**. The data owner warned this batch has mistakes, so recheck your rules against the full-batch profile. Do not remove checks. **RETRY** decision 3.
4. **Does `returns_corrected` pass the same checks?**
   * Yes: **PROCEED** to build `return_features` and the three split tables, then decision 5.
   * No: **STOP**. Write no features; report the failing records to the trainer. Do not zero-fill or silently drop records.
5. **Do the readback checks pass?** Exactly three columns, every accepted return once, a nonzero ratio matching your source-derived value, zero-income returns present with a null ratio, and every return in exactly one split for its year.
   * Yes: **PROCEED** to decision 6.
   * No: **RETRY**. Fix the code and rerun all stages from the start.
6. **Do two complete successful runs leave identical return IDs and values, with no appended copies?**
   * Yes: **PROCEED** to the exports, `SUBMISSION.md` and Commit & Push, then decision 7.
   * No: **RETRY**. Fix the cause (for example, use overwrite instead of append) and do two fresh full runs.
7. **Does the GitHub commit on `main` contain `notebook.py`, `SUBMISSION.md` and every final export in `outputs/`?**
   * Yes: **PROCEED**. Record the full SHA and submit in Classroom. Done.
   * No: **RETRY**. Add the missing files, Commit & Push again, then recheck.

## 3. What is being asked

### Required (assessed)

The four connected outcomes in [CHALLENGE.md](CHALLENGE.md#task-and-judgment-requirements); you choose the implementation.

* **Outcome 1 - Understand the input**
  * Use Catalog descriptions, at least three sample rows and the supplied lineage view to establish row meaning, relevant fields, the uniqueness identifier and the upstream source.
  * Keep a concise source reference and explanation in the notebook.
  * Inspect the full batch: returns by year, missing required values, duplicate return IDs and zero-income records.
  * Keep the queries and results in the notebook and explain which findings affect your next checks or transformations.
* **Outcome 2 - Decide which batch is usable**
  * Completeness (required-value) and uniqueness checks must reject `returns_initial` before any feature output is written.
  * Preserve the affected records and the reason for rejection.
  * Assess `returns_corrected` with the same rules and use it for the final run.
  * Not acceptable: removing checks, replacing unknown amounts with zero, silently discarding records.
  * Explain the failure and your next action using the actual records and rule.
* **Outcome 3 - Deliver a checked feature table**
  * Save `return_features` with exactly `return_id`, `tax_year`, `deduction_to_income` (deduction divided by income).
  * Zero-income returns stay in the table with a null ratio. Every accepted return appears once.
  * Exclude the filer identifier (`taxpayer_id`) and raw amounts (need-to-know rule).
  * Verify a nonzero ratio against an expected value you derive from the source.
  * Read the saved table back, including a zero-income case.
* **Outcome 4 - Make the handoff usable**
  * From the saved features, save `features_train` (2022), `features_validation` (2023), `features_test` (2024).
  * Every accepted return appears in exactly one split, in its assigned year.
  * Two complete successful runs must leave the same return IDs and values, without appended copies.
  * Write a short handoff (`SUBMISSION.md`) naming inputs, outputs, the ratio definition, checks performed and anything you could not verify.
* **Submission:** commit and push to `main` of your own public fork, verify on GitHub, then submit in Classroom.

### Strongly recommended but ungraded

* Connection practice in [START_HERE.md](START_HERE.md) section 2 (`SELECT 1 AS connection_check`, `outputs/connection-practice.json`, first Commit and Push). It is not assessed; do not turn it in.

### Optional

* A chart in Outcome 1.
* Genie Code (review and verify its suggestions).
* Running with Jobs (then keep the Jobs run URL/ID; interactive runs do not need one).
* Screenshots (may supplement, never replace, code or saved outputs).
* The Codex / Workstation route in [OPTIONAL_TOOLS.md](OPTIONAL_TOOLS.md), only if the trainer selects it. [SESSION_RECORD.md](SESSION_RECORD.md) becomes required only when the trainer assigns that route or enables conversation assessment.

## 4. What is provided

* **Input data** (read-only, schema `training.to6_challenge3_inputs`):
  * `returns_initial` - first, faulty batch; already parsed synthetic data covering 2022-2024.
  * `returns_corrected` - replacement batch, same structure.
  * `training.to6_challenge3_inputs.source_lineage` - supplied lineage view (separate from the Catalog UI's Lineage tab).
  * Data rules: one row = one return; `return_id` identifies the return; `taxpayer_id` identifies the filer (may appear in several years); required fields `return_id`, `tax_year`, `income`, `deduction`; each return exactly once; amounts nonnegative; zero income is valid.
* **Starter files** (in your fork / Git folder):
  * `notebook.py` - scaffold with headings for the four outcomes and a setup cell defining `INPUT_SCHEMA`, `INITIAL_TABLE`, `CORRECTED_TABLE`, `LINEAGE_VIEW` and `OUTPUT_SCHEMA`. (In the Databricks Workspace it shows as `notebook`.)
  * `SUBMISSION.md` - handoff file to fill in.
  * `README.md` - navigation page; leave it in place.
  * `CHALLENGE.md`, `START_HERE.md`, `OPTIONAL_TOOLS.md`, `SESSION_RECORD.md` - instructions.
  * `tools/databricks_platform.py`, `tools/list_codex_sessions.py`, `tests/test_platform.py`, `requirements.txt`, `.github/workflows/tests.yml` - used only by the optional Codex route.
* **Access supplied by the training team:**
  * Databricks workspace link, compute and reviewer instructions (Classroom environment card).
  * Read access to the inputs and Catalog descriptions.
  * An individual writable output schema: `training.to6_c3_` + first 12 hex characters of SHA-256 of your Databricks username (the setup cell calculates it). Do not change it to another person's schema.
  * Missing access is an environment issue for the trainer.
* **Tools:** Databricks notebooks, Catalog Explorer, Git folders, the Databricks file editor, optional Genie Code; GitHub (your own account) and Classroom.

## 5. Expected outputs

| Output | Exact name | Where it belongs | Contents / evidence |
| --- | --- | --- | --- |
| Feature table | `return_features` | `OUTPUT_SCHEMA` | Exactly `return_id`, `tax_year`, `deduction_to_income`; every accepted return once |
| Training split | `features_train` | `OUTPUT_SCHEMA` | 2022 returns from `return_features` |
| Validation split | `features_validation` | `OUTPUT_SCHEMA` | 2023 returns |
| Test split | `features_test` | `OUTPUT_SCHEMA` | 2024 returns |
| Rejection evidence | Your choice of name (table and/or export) | `OUTPUT_SCHEMA` and `outputs/` | Affected `returns_initial` records with rejection reasons |
| Notebook source | `notebook.py` | Repo root, `main` | Implementation, source references, checks, initial rejection, saved-table readback evidence |
| Handoff | `SUBMISSION.md` | Repo root, `main` | Inputs and outputs by full table name, ratio definition, checks and results, run references, list of final exports, anything not verified |
| Exports | Small synthetic files (names your choice) | `outputs/` | Saved feature and split tables, affected initial records with reasons, evidence comparing the two successful runs |
| Run references | - | In `SUBMISSION.md` | For each successful run: notebook URL and execution timestamp (or Jobs run URL/ID) |
| Classroom submission | - | Classroom | Repo base URL `https://github.com/{owner}/{repo}`, branch `main`, final commit SHA (private comment) |

Notes:

* Only write tables in your assigned output schema; never change the shared inputs.
* Row counts alone cannot prove the two runs match; keep actual IDs and values, or a reproducible snapshot comparison.
* A successful replacement run does not replace the initial rejection evidence; keep both.
* Do not put the containing commit SHA in `SUBMISSION.md`; report it in Classroom.
* `outputs/connection-practice.json` is ungraded practice; list your final task exports in `SUBMISSION.md` so reviewers can tell them apart.

## 6. Implementation steps

Follow in order. Each step lists **Do**, **Check** and **Evidence**.

1. **Fork the starter repository** ([START_HERE.md](START_HERE.md) section 1)
   * Do: open https://github.com/GitRollTraining/to6-databricks-challenge-starter, choose **Fork -> Create a new fork** on your personal account, keep `main`.
   * Check: the fork is under your account and public.
   * Evidence: none yet; use this fork throughout.
2. **Link GitHub to Databricks**
   * Do: user menu -> **Settings -> Linked accounts -> Add Git credential**, select GitHub, link your own account.
   * Check: the credential appears in Linked accounts. Never save credentials in a notebook, repo or prompt.
3. **Create the Git folder**
   * Do: **Workspace** -> your user folder -> **Create -> Git folder**, paste your fork's HTTPS URL, select GitHub.
   * Check: the branch shown is `main`; `README.md` is present.
4. **Open the notebook and attach compute**
   * Do: open `notebook.py`, select the compute named in Classroom, run the setup cell.
   * Check: it prints `input_schema` = `training.to6_challenge3_inputs` and your `output_schema` (`training.to6_c3_...`). **[Run in Databricks]** your exact output schema name.
   * Check: if the schema is missing or not writable, stop and contact the trainer.
5. **Connection practice (ungraded)** ([START_HERE.md](START_HERE.md) section 2)
   * Do: run `SELECT 1 AS connection_check` in a SQL cell; run the supplied Python cell with your Git folder's absolute path (including `/Workspace/`) to write `outputs/connection-practice.json`.
   * Check: result is 1; refresh the folder and open the file.
   * Do: click the branch name, review changes, enter a message, **Commit & Push**.
   * Check: both files changed on `main` in GitHub. If a write or push fails, resolve it with the trainer before starting. Do not turn this in.
6. **Wait for the assignment to open**
   * Do: begin the assessed task only when the trainer opens the Classroom assignment ([README.md](README.md)).
7. **Outcome 1a - Inspect metadata, samples and lineage**
   * Do: in Catalog Explorer, read the descriptions of `returns_initial` and `returns_corrected`; query at least three sample rows; query `training.to6_challenge3_inputs.source_lineage`.
   * Check: you can state row meaning, required fields, the uniqueness identifier (`return_id`) and the upstream source.
   * Evidence: queries and results kept in the notebook, plus a short markdown source reference and explanation. **[Run in Databricks]** the upstream source name from the lineage view.
8. **Outcome 1b - Profile the full batches**
   * Do: for each batch, summarize returns by `tax_year`, missing required values, duplicate `return_id`s and zero-income records (chart optional).
   * Check: totals reconcile (sum of per-year counts = total rows).
   * Evidence: queries, results and a note on which findings affect the next checks or transformations. **[Run in Databricks]** actual counts, the specific defective records and the zero-income return IDs.
9. **Outcome 2a - Build the quality gate**
   * Do: write completeness (required fields non-null) and uniqueness (`return_id` exactly once) checks that stop the pipeline before any feature output is written if either fails.
   * Check: test the gate on a known-bad case so you see it fail.
10. **Outcome 2b - Reject the initial batch and preserve evidence**
    * Do: run the gate on `returns_initial`; save every affected record with its rejection reason (for example a table in `OUTPUT_SCHEMA`).
    * Check: the gate failed, no feature tables were written from this batch, and affected records were not altered or zero-filled.
    * Evidence: keep the failed-run reference and the affected records with reasons; explain the failure and next action in the notebook. **[Run in Databricks]** which records failed and which rule each broke.
11. **Outcome 2c - Accept the corrected batch**
    * Do: run the same gate on `returns_corrected`.
    * Check: zero missing required values and zero duplicate IDs; row count equals distinct `return_id` count. If it fails, stop and report to the trainer.
    * Evidence: gate results in the notebook. **[Run in Databricks]** row and distinct-ID counts.
12. **Outcome 3a - Build and save `return_features`**
    * Do: from the accepted batch, compute `deduction_to_income` = deduction / income (null when income is 0); keep only `return_id`, `tax_year`, `deduction_to_income`; save with overwrite (not append) in `OUTPUT_SCHEMA`.
    * Check: no `taxpayer_id`, `income` or `deduction` columns in the result.
13. **Outcome 3b - Verify by reading back**
    * Do: read the saved table back from `OUTPUT_SCHEMA`.
    * Check: columns are exactly the three required; row count = distinct IDs = accepted rows; one nonzero ratio matches your hand-derived expected value from the source; a zero-income return is present with a null ratio.
    * Evidence: readback queries and results in the notebook. **[Run in Databricks]** the chosen return, its source amounts and its expected vs saved ratio.
14. **Outcome 4a - Save the year splits**
    * Do: from the saved `return_features`, write `features_train` (2022), `features_validation` (2023), `features_test` (2024) with overwrite.
    * Check: each table holds only its year; the union of the three equals `return_features` with no overlaps or missing IDs (compare both ways).
    * Evidence: check queries and results. **[Run in Databricks]** per-split row counts.
15. **Outcome 4b - Complete run 1**
    * Do: run all stages top to bottom; capture a snapshot of all return IDs and values (features and splits).
    * Check: the run succeeded end to end.
    * Evidence: save the snapshot under `outputs/`; record the notebook URL and execution timestamp. **[Run in Databricks]** run 1 URL and timestamp.
16. **Outcome 4c - Complete run 2 and compare**
    * Do: rerun all stages; capture a second snapshot; compare IDs and values with run 1.
    * Check: identical IDs and values and no appended copies (row counts unchanged). If not, fix the code and redo steps 15-16.
    * Evidence: run 2 snapshot plus a comparison file under `outputs/`; run 2 URL and timestamp. **[Run in Databricks]** run 2 URL, timestamp and comparison result.
17. **Export small evidence files**
    * Do: from a Python cell, using the practice file-writing pattern, write exports of `return_features`, the three split tables and the affected initial records with reasons to `outputs/`.
    * Check: refresh the folder; each file opens and contains the expected rows.
18. **Write `SUBMISSION.md`**
    * Do: in the Databricks file editor, or with `(repo_root / "SUBMISSION.md").write_text(your_text, encoding="utf-8")`, record: inputs and outputs by full table name, ratio definition, checks and results, both run references, the list of final exports and anything you could not verify.
    * Check: every item listed in [CHALLENGE.md](CHALLENGE.md#what-to-submit-and-how-it-is-checked) is present; no commit SHA included.
19. **Commit and push**
    * Do: review the deliverables, click the branch name, **Commit & Push** to `main`. Leave `README.md` in place.
    * Check: the push succeeded.
20. **Verify on GitHub**
    * Do: open the resulting commit in your fork.
    * Check: it contains `notebook.py`, `SUBMISSION.md` and every final export under `outputs/`.
    * Evidence: record the commit's full SHA. **[Obtain from GitHub]** the final SHA.
21. **Submit in Classroom**
    * Do: **Add or create -> Link** with your repo base URL (`https://github.com/{owner}/{repo}`); add branch `main` and the final SHA in a private comment; **Turn in** by the assignment's deadline.
    * Check: Classroom shows the assignment as turned in; the fork stays public.

Optional Codex route: if the trainer selects it, follow [OPTIONAL_TOOLS.md](OPTIONAL_TOOLS.md) (and [SESSION_RECORD.md](SESSION_RECORD.md) when required) instead of steps 5 and 17-20; the task outcomes and deliverables stay the same.
