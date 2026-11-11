# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

PySpark function library for processing CMS (Centers for Medicare & Medicaid Services) Medicare claims data and integrating it with 20+ external health/socioeconomic datasets (AHA, Census ACS, SDOH, NPI, ACGME, ADI, etc.). Pure Python, no build system or CI/CD -- this is a library of functions imported into analysis projects.

## Dependencies

- PySpark (pyspark.sql.functions, pyspark.sql.types, pyspark.sql.window)
- Python standard library only otherwise (urllib, json, re, itertools, functools)
- No requirements.txt or package manager -- PySpark must be available in the runtime environment

## Architecture

### Data Flow

```
Raw files (20+ sources) 
  -> utilities.get_filenames() / read_data() / get_data()
  -> utilities.prep_*() standardizes each source
  -> cms.schemas provides PySpark StructType schemas for loading
  -> cms.longToShortXW converts long CMS field names to short names
  -> Processing modules (base, mbsf, revenue, line, claims, comorbidities, stays, transfers)
  -> Enriched PySpark DataFrames
```

### Key Modules

- **utilities.py** -- Global constants (yearMin/yearMax, FIPS codes, leap year maps), data file path mappings (`get_filenames()`), data loading (`read_data()`, `get_data()`), and `prep_*()` functions that standardize each external data source
- **cms/base.py** -- Core claim processing: admission/discharge date enrichment, diagnosis/procedure aggregations, day-of-year calculations, prior year lookups
- **cms/schemas.py** -- Static PySpark StructType schema definitions for all claim types (IP, OP, SNF, HHA, Hospice, Carrier) supporting CMS versions J and K
- **cms/longToShortXW.py** -- Static dictionaries mapping long CMS field names to short names
- **cms/mbsf.py** -- Medicare Beneficiary Summary File: enrollment tracking, death date processing, residency, demographics
- **cms/comorbidities.py** -- ICD-10 code mappings for comorbidity flags (Glasheen2019, Quan2005)
- **cms/transfers.py** -- Patient transfer analysis: linking related claims, temporal matching, provider-to-provider patterns
- **cms/stays.py** -- Provider-level aggregations (stroke volume, capabilities, treatment metrics)
- **cms/line.py** -- Line-level claim processing: HCPCS codes, PCP identification, provider specialty
- **cms/revenue.py** -- Revenue center flags: ICU, ED, MRI, CT with claim-level vs line-level aggregation
- **cms/claims.py** -- Joins base claims with summarized revenue/line data

## Column Naming Conventions

Raw CMS columns keep their CMS Field Short Name, which is always UPPERCASE (`REV_CNTR`, `HCPCS_CD`, `DSYSRTKY` -- every one of the ~1000 fields in `cms/schemas.py`). Every column this codebase adds starts with a LOWERCASE character (`ed`, `icu`, `ishStroke`, `providerAnnualVolume`).

The case is therefore what distinguishes a raw CMS field from an enricher-added one, and code may rely on it to select "the columns we added" by regex rather than by a hand-maintained list (see `revenue.get_revenue_summary` / `line.get_line_summary`). Do not add a column whose name starts with an uppercase letter.

The one exception: the date components derived from a CMS date field keep that field's uppercase name as their prefix (`THRU_DT_YEAR`, `ADMSN_DT_DAY`, `DEATH_DT_MONTH`, ... -- see `base.add_admission_date_info`), because they belong to the raw field rather than being a new measure. They are uppercase precisely so that the lowercase-means-ours rule keeps working: anything selecting "our" columns wants the flags and aggregations, not the date parts.

## Function Naming Conventions

Function names encode their behavior:

- **`add_x(df)`** -- adds exactly 1 column named `x`
- **`add_x_info(df)`** -- adds multiple columns (use `printSchema()` to see what was added)
- **`get_x()`** -- returns `x` without modifying input DataFrames
- **`add_x(df, inClaim=True/False)`** -- revenue/line functions: `inClaim=True` adds both `x` and `xInClaim` columns; `inClaim=False` adds only `x`
- **Complex names like `add_pcpHomeVisit`** -- imply AND condition (pcp AND home visit)

### Single vs Multiple DataFrame Arguments

- **One DF arg** -> uses `withColumn()` (no shuffle, lighter computation)
- **Multiple DF args** -> uses `join()` (requires shuffle, heavier computation)

Example: `add_death_date_info(mbsfDF)` enriches in-place; `add_death_date_info(baseDF, mbsfDF)` joins mbsfDF data onto baseDF.

## Window Function Patterns

- Provider-level: `Window.partitionBy(["ORGNPINM","THRU_DT_YEAR"])`
- Claim-level: `Window.partitionBy(["DSYSRTKY","CLAIMNO","THRU_DT"])`

## Tests

- `tests/cms/` -- unit tests on synthetic rows, run locally with `pytest` (Spark 3.5 defaults, see `tests/conftest.py`)
- `tests/cms/test_end_to_end.py` -- the whole chain, `get_data` to `get_transfers`, on invented files that `tests/cms/synthetic.py` writes to a temporary folder (no real data needed), checked against answers worked out by hand from its small cohort. Its external files are made from `utilities.get_filenames`, so a new source needs an entry there
- `tests/cms/test_invariants.py` -- properties that must hold for any input: the same output whatever the row order and partitioning (`in_every_order`, on inputs with ties built in). `test_end_to_end.py` checks the whole chain the same way (a second run on the files shuffled), and that enrichers keep every row and raw value and each `add_x` adds exactly `x`. Known failures are strict xfails that name the code
- `tests/data/test_regression.py` -- regression snapshot, run at OSC with the data tests: aggregates of every column of every output of `tests/chain.py` (the chain the end-to-end test also runs) by year, on the beneficiary sample, saved under `--snapshot-path` (never in the repo or git) and compared with the snapshot before (`tests/data/snapshots.py`). It reports what changed and does not fail on it; the printed report hides counts below 11 and every min and max
- `tests/data/test_benchmarks.py` -- the library against figures CMS publishes, at OSC with the data tests: the Medicare Geographic Variation PUF by state (an extract in `tests/data/benchmarks/`, rebuilt by `scripts/05_geographic_variation.py`), the July MA penetration by county, the hospitals of the POS file. The Geographic Variation comparisons use CMS's Original Medicare definition rebuilt from the mbsf (`tests/data/benchmarks.add_cmsOm`), not the library's `ffs`, whose overlap with it is recorded apart. Bands in `thresholds.benchmarkBands`
- `tests/mutation.py` -- mutation testing: makes one small change at a time to a module (a comparison, a condition, a number, a code list, including those in the condition strings run with `eval`, a window key, a join), runs the module's tests on it in a copy of the repo (`tests/cms/test_<module>.py`, then every other file that calls the module's functions, and for stays and transfers their invariant test, `mutation.otherTests`), and reports the changes no test catches. `test_end_to_end.py` is not run, so a change only it would catch is reported too. Every module of `mutation.allModules` by default, about 2850 mutants, or `python tests/mutation.py cms/revenue.py --workers 2`; the report is rewritten after every mutant. At OSC from the data-test job, set by environment variables as `subspark` passes no options to the script: `RUN_MUTATION=1 MUTATION_WORKERS=16 subspark run.py ...`, after the data tests and the unit tests (`tests/cms`), which always run (the unit tests and mutation in a python of their own with a local Spark, see `tests/data/run.py`)
- Coverage -- `pytest tests/cms --cov --cov-report=term-missing --cov-report=html` (needs `pytest-cov`, settings in `.coveragerc`). A line counts as covered once its Spark `Column` is built, tested or not, so "uncovered" means untested but "covered" only means it ran
- `tests/data/` -- checks of the real CMS and external datasets against what the code assumes (unique join keys, columns present every year, key formats, match rates, row counts kept by the joins). Skipped unless `--run-data`; run at OSC from a batch job with `python tests/data/run.py --years YEARI YEARF [pytest options]`. The rates they measure print as a table at the end; their floors and the known exceptions live in `tests/data/thresholds.py`
