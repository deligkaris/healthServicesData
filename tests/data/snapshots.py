'''Regression snapshots, see test_regression.py: aggregates of every column of every output by year, saved at OSC outside
the repo, and the comparison of one with an earlier one. Aggregates only, never rows; the printed report hides the
small cells, the saved files keep the exact values and stay at OSC.'''
import csv
import datetime
import json
import math
import re
import subprocess
from pathlib import Path

import pyspark.sql.functions as F
from pyspark.sql.types import ArrayType, BooleanType, MapType, NumericType, StructType

#a column with at most this many distinct values in a year also gets a count per value, unless it is a 0/1 kind of flag
#whose sum and nonNull already say it
maxLevels = 30
#columns per aggregation, so that no plan gets thousands of expressions wide
chunkColumns = 100
#sums and means of doubles change in their last bits with the order they add up in
relativeTolerance = 1e-9
#CMS cell size suppression: no count from 1 to 10 is shown
smallCell = 11
reportLines = 200
#the printed report, filled by test_regression.py and shown by conftest.py at the end of the run
report = []

yearColumns = ["RFRNC_YR", "THRU_DT_YEAR", "fromTHRU_DT_YEAR"]
countMeasures = {"rows", "nonNull", "distinctApprox", "sizeSum"}
snapshotName = re.compile(r"^(\d{8}-\d{6})_(.+)_(\d{4})-(\d{4})_n(\d+)$")

def get_year(df):
    for c in yearColumns:
        if c in df.columns:
            return F.col(c)
    #the revenue parts have no date part, THRU_DT is yyyymmdd
    return F.floor(F.col("THRU_DT") / 10000).cast("int") if "THRU_DT" in df.columns else F.lit(None).cast("int")

def _measures(field):
    '''(measure, expression) of a column for the first pass.'''
    (c, kind) = (F.col(f"`{field.name}`"), field.dataType)
    if isinstance(kind, ArrayType):
        #order free, the arrays are in no particular order (see the data dictionary)
        return [("nonNull", F.count(c)), ("sizeSum", F.sum(F.when(c.isNotNull(), F.size(c))))]
    if isinstance(kind, (StructType, MapType)):
        return [("nonNull", F.count(c))]
    measures = [("nonNull", F.count(c)), ("distinctApprox", F.approx_count_distinct(c))]
    if isinstance(kind, BooleanType):
        measures.append(("sum", F.sum(c.cast("int"))))
    elif isinstance(kind, NumericType):
        measures += [("sum", F.sum(c)), ("min", F.min(c)), ("max", F.max(c))]
    return measures

def _number(value):
    #a Decimal sum comes back as Decimal, an integral double stays a double
    return value if value is None or isinstance(value, (int, float)) else float(value)

def get_profile(output, df):
    '''[(output, year, column, measure, value)] of df: rows per year, and per column and year nonNull, distinctApprox,
    sum, min and max of numbers, sizeSum of arrays, and count:<value> of the columns with few distinct values.'''
    year = get_year(df).alias("snapshotYear")
    fields = df.schema.fields
    rows = []
    for start in range(0, len(fields), chunkColumns):
        chunk = fields[start:start + chunkColumns]
        exprs = [e.alias(f"{start + i}|{m}") for i, field in enumerate(chunk) for (m, e) in _measures(field)]
        if start == 0:
            exprs.append(F.count(F.lit(1)).alias("rows"))
        for r in df.groupBy(year).agg(*exprs).collect():
            for (alias, value) in r.asDict().items():
                if alias == "snapshotYear":
                    continue
                if alias == "rows":
                    rows.append((output, r["snapshotYear"], "*", "rows", value))
                    continue
                (index, measure) = alias.split("|")
                rows.append((output, r["snapshotYear"], fields[int(index)].name, measure, _number(value)))
    distinct = {}
    for (_, _, column, measure, value) in rows:
        if measure == "distinctApprox":
            distinct[column] = max(distinct.get(column, 0), value or 0)
    numeric = {f.name for f in fields if isinstance(f.dataType, (NumericType, BooleanType))}
    levels = [c for c, n in distinct.items() if 0 < n <= maxLevels and not (c in numeric and n <= 2)]
    if levels:
        pairs = F.explode(F.array(*[F.struct(F.lit(c).alias("column"), F.col(f"`{c}`").cast("string").alias("value"))
                                    for c in levels])).alias("pair")
        counts = (df.select(year, pairs).select("snapshotYear", "pair.column", "pair.value")
                    .filter(F.col("value").isNotNull()).groupBy("snapshotYear", "column", "value").count().collect())
        rows += [(output, r["snapshotYear"], r["column"], f"count:{r['value']}", r["count"]) for r in counts]
    return rows

def get_commit(repo):
    '''(short commit, whether tracked files have uncommitted changes); ("nocommit", False) without git.'''
    try:
        run = lambda *args: subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=True,
                                           check=True, timeout=60).stdout.strip()
        return run("rev-parse", "--short", "HEAD"), run("status", "--porcelain", "--untracked-files=no") != ""
    except (OSError, subprocess.SubprocessError):
        return "nocommit", False

def get_name(meta, now=None):
    now = now or datetime.datetime.now()
    return (f"{now:%Y%m%d-%H%M%S}_{meta['commit']}{'-dirty' if meta['dirty'] else ''}_"
            f"{meta['years'][0]}-{meta['years'][1]}_n{meta['sample']}")

def save_snapshot(folder, name, rows, meta):
    folder = Path(folder)
    folder.mkdir(parents=True, exist_ok=True)
    with open(folder / f"{name}.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["output", "year", "column", "measure", "value"])
        writer.writerows(rows)
    (folder / f"{name}.json").write_text(json.dumps(meta, indent=1), encoding="utf-8")

def _value(text):
    if text == "":
        return None
    return int(text) if re.fullmatch(r"-?\d+", text) else float(text)

def load_snapshot(folder, name):
    '''{(output, year, column, measure): value} of a saved snapshot.'''
    with open(Path(folder) / f"{name}.csv", newline="", encoding="utf-8") as f:
        return {(r["output"], _value(r["year"]), r["column"], r["measure"]): _value(r["value"]) for r in csv.DictReader(f)}

def find_baseline(folder, name, baseline=None):
    '''The snapshot to compare name with: baseline if given, else the newest earlier one of the same years and sample,
    None if there is none.'''
    if baseline:
        return baseline[:-4] if baseline.endswith(".csv") else baseline
    current = snapshotName.match(name)
    earlier = []
    for path in Path(folder).glob("*.csv"):
        #a comparison, <name>_vs_<baseline>, ends like a snapshot's name too
        m = snapshotName.match(path.stem) if "_vs_" not in path.stem else None
        if m and m.group(3, 4, 5) == current.group(3, 4, 5) and m.group(1) < current.group(1):
            earlier.append(path.stem)
    return max(earlier) if earlier else None

def _same(a, b):
    if a is None or b is None:
        return a is b
    if isinstance(a, int) and isinstance(b, int):
        return a == b
    return math.isclose(a, b, rel_tol=relativeTolerance, abs_tol=1e-12)

def diff_profiles(before, after):
    '''[(output, year, column, measure, before, after)] where the two differ, None on the side a key is missing from.'''
    keys = sorted(set(before) | set(after), key=repr)
    return [(*k, before.get(k), after.get(k)) for k in keys if not _same(before.get(k), after.get(k))]

def shown(profile, output, year, column, measure, value):
    '''value as the printed report may show it: no count from 1 to 10, no min or max, and a sum only over a column
    with at least smallCell values that year.'''
    if value is None:
        return "absent"
    if measure in ("min", "max"):
        return "hidden"
    if measure == "sum" and (profile.get((output, year, column, "nonNull")) or 0) < smallCell:
        return "hidden"
    if measure in countMeasures or measure.startswith("count:") or (measure == "sum" and float(value).is_integer()):
        if 0 < value < smallCell:
            return f"<{smallCell}"
    return f"{value:.6g}" if isinstance(value, float) else str(value)

def report_lines(name, baseline, before, after, diff):
    if baseline is None:
        return [f"{name}: saved, no earlier snapshot of these years and sample to compare with"]
    if not diff:
        return [f"{name} vs {baseline}: no changes"]
    changed = {}
    for (output, _, column, _, _, _) in diff:
        changed[(output, column)] = changed.get((output, column), 0) + 1
    lines = [f"{name} vs {baseline}: {len(diff)} measures changed in {len(changed)} columns (full list in "
             f"{name}_vs_{baseline}.csv)"]
    lines += [f"  {output:<12} {column:<48} {n} measures" for (output, column), n in sorted(changed.items())]
    lines += [f"  {output:<12} {column:<48} {year!s:<5} {measure:<24} "
              f"{shown(before, output, year, column, measure, a)} -> {shown(after, output, year, column, measure, b)}"
              for (output, year, column, measure, a, b) in diff]
    more = len(lines) - reportLines
    return lines[:reportLines] + ([f"  ... {more} more lines in the csv"] if more > 0 else [])

def save_diff(folder, name, baseline, diff):
    with open(Path(folder) / f"{name}_vs_{baseline}.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["output", "year", "column", "measure", "before", "after"])
        writer.writerows(diff)
