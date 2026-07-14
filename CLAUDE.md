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
