'''Helpers the data tests share: each measures one property of a dataframe and returns numbers plus a few examples,
so a failure message fits on one line.'''
import pyspark.sql.functions as F

from tests.data.thresholds import get_floor

examplesShown = 5

class Metrics:
    '''The rates the data tests measure, printed as one table at the end of the run (see conftest.py) so the floors
    in thresholds.py can be set from a run.'''
    def __init__(self):
        self.rows = []

    def add(self, check, key, year, value, floor=None):
        self.rows.append((check, key, year, value, floor))
        return value

def get_duplicate_keys(df, keys):
    '''Returns how many key values sit on more than one row of df, and a few of them.'''
    duplicates = df.groupBy(*keys).count().filter(F.col("count") > 1)
    number = duplicates.count()
    examples = [tuple(r[k] for k in keys) for r in duplicates.limit(examplesShown).collect()] if number else []
    return number, examples

def get_match_rate(df, keys, refDF, refKeys, byCol):
    '''Returns {byCol value: share of the df rows with a non-null key whose key is in refDF}.'''
    ref = (refDF.select(*[F.col(r).alias(k) for k, r in zip(keys, refKeys)])
                .dropna()
                .distinct()
                .withColumn("matchedInRef", F.lit(1)))
    rows = (df.select(byCol, *keys)
              .dropna(subset=keys)
              .join(ref, on=keys, how="left_outer")
              .groupBy(byCol)
              .agg(F.avg(F.coalesce(F.col("matchedInRef"), F.lit(0))).alias("rate"))
              .collect())
    return {r[byCol]: r["rate"] for r in rows}

def get_filled_rate(df, keyCol, valueCol, byCol):
    '''Returns {byCol value: share of the rows with a non-null keyCol that also have a non-null valueCol}, ie the
    match rate of a left join that brought valueCol in on keyCol.'''
    rows = (df.filter(F.col(keyCol).isNotNull())
              .groupBy(byCol)
              .agg(F.avg(F.col(valueCol).isNotNull().cast("int")).alias("rate"))
              .collect())
    return {r[byCol]: r["rate"] for r in rows}

def get_null_rates(df, cols, byCol):
    '''Returns {byCol value: {column: share of null rows}}.'''
    rows = df.groupBy(byCol).agg(*[F.avg(F.col(c).isNull().cast("int")).alias(c) for c in cols]).collect()
    return {r[byCol]: {c: r[c] for c in cols} for r in rows}

def get_counts(df, conditions):
    '''Returns {name: rows of df where the condition is true} for a dict of named boolean columns, in one pass.'''
    row = df.agg(*[F.sum(F.coalesce(c, F.lit(False)).cast("int")).alias(name) for name, c in conditions.items()]).first()
    return {name: (row[name] or 0) for name in conditions}

def get_examples(df, condition, cols):
    '''Returns a few rows of df (cols only) where the condition is true.'''
    return [tuple(r) for r in df.filter(condition).select(*cols).limit(examplesShown).collect()]

def check_rates(metrics, check, key, rates):
    '''Records {year: rate} in metrics and fails on a rate below the floor thresholds.py sets for (check, key).'''
    floor = get_floor(check, key)
    for year, rate in rates.items():
        metrics.add(check, key, year, rate, floor)
    low = {year: round(rate, 4) for year, rate in rates.items() if rate < floor}
    assert not low, f"{check} {key}: {low} below the floor {floor}"
