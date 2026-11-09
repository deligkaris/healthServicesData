'''The conventions of CLAUDE.md that can be read off the source: the data dictionary, add_x adds x, the schemas and their
crosswalk, the column naming. Names built at run time are not seen, tests/data/test_pipeline.py checks the real columns.'''
import ast
import re
from pathlib import Path

from cms.longToShortXW import longToShortXW
from cms.schemas import schemas
from tests.data.test_pipeline import datePart, rawColumns
from tests.data.thresholds import namingExceptions

repoRoot = Path(__file__).parents[2]
cmsFiles = sorted(repoRoot.glob("cms/*.py"))
libraryFiles = cmsFiles + [repoRoot / name for name in ("utilities.py", "geocoding.py", "census.py")]
#the columns stays and transfers add, read off the source; test_end_to_end.py checks the columns that reach each output,
#which are those an analysis keeps (most of what base, mbsf, line and utilities add is scratch dropped on the way)
dictionaryFiles = [repoRoot / "cms" / name for name in ("stays.py", "transfers.py")]

def parse(path):
    return ast.parse(path.read_text(encoding="utf-8"))

def get_strings(node):
    return {n.value for n in ast.walk(node) if isinstance(n, ast.Constant) and isinstance(n.value, str)}

def get_method_calls(tree, *names):
    return [n for n in ast.walk(tree)
            if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) and n.func.attr in names]

def get_new_name(call):
    '''The argument of a withColumn, withColumnRenamed or alias call that names the new column, None if it has none.'''
    index = 1 if call.func.attr == "withColumnRenamed" else 0
    return call.args[index] if len(call.args) > index else None

def get_dropped_columns(tree):
    #from the arguments alone: the call's func also holds the chain before .drop, with the columns that chain adds
    args = [arg for call in get_method_calls(tree, "drop") for arg in call.args]
    #drop(*dropColumns) names its columns where dropColumns is assigned
    lists = {arg.value.id for arg in args if isinstance(arg, ast.Starred) and isinstance(arg.value, ast.Name)}
    assigned = [n.value for n in ast.walk(tree)
                if isinstance(n, ast.Assign) and any(getattr(target, "id", None) in lists for target in n.targets)]
    return {s for node in args + assigned for s in get_strings(node)}

def get_dictionary_entries():
    source = (repoRoot / "r_utilities.R").read_text(encoding="utf-8")
    dictionary = source[source.index("get_data_dictionary <- function"):source.index("resolve_dictionary_column <-")]
    return set(re.findall(r'^\s*"([^"]+)",\s*"', dictionary, re.M))

def is_documented(column, entries):
    #the exact part of r_utilities.R's resolve_dictionary_column: its suffix matches are not proof of an entry either, its
    #check_data_dictionary only reports them
    side = re.match("from|to", column)
    return column in entries or (side is not None and column[side.end():] in entries)

def get_undocumented_columns():
    entries = get_dictionary_entries()
    undocumented = set()
    for path in dictionaryFiles:
        tree = parse(path)
        names = [get_new_name(call) for call in get_method_calls(tree, "withColumn")]
        added = {name.value for name in names if isinstance(name, ast.Constant)}
        undocumented |= {c for c in added - get_dropped_columns(tree) if c[0].islower() and not is_documented(c, entries)}
    return undocumented

def test_added_columns_are_in_data_dictionary():
    undocumented = get_undocumented_columns()
    assert not undocumented, f"no entry in get_data_dictionary (r_utilities.R): {sorted(undocumented)}"

def get_add_x_violations(tree):
    bad = []
    for fn in tree.body:
        if not (isinstance(fn, ast.FunctionDef) and fn.name.startswith("add_")) or "_info" in fn.name:
            continue
        #a wrapper names its column in the add_ it calls, a generic such as add_providerAnnualVolume(col=...) from its input
        called = [n.func for n in ast.walk(fn) if isinstance(n, ast.Call)]
        delegates = any(getattr(f, "attr", getattr(f, "id", "")).startswith("add_") for f in called)
        computed = any(not isinstance(get_new_name(call), ast.Constant)
                       for call in get_method_calls(fn, "withColumn", "withColumnRenamed", "alias"))
        #any literal, not just a withColumn's: a join brings the column in with F.col("x") in a select
        if not (delegates or computed or fn.name[len("add_"):] in get_strings(fn)):
            bad.append(fn.name)
    return bad

def test_add_x_adds_x():
    bad = {path.name: names for path in libraryFiles if (names := get_add_x_violations(parse(path)))}
    assert not bad, f"add_x without a column x: {bad}"

def test_crosswalk_claim_parts_have_schemas():
    #ip, op and mbsf have no crosswalk, their files come with the short names
    assert set(longToShortXW) <= set(schemas)

def test_crosswalk_covers_schema():
    missing = {part: sorted(set(schemas[part].fieldNames()) - set(xw.values())) for part, xw in longToShortXW.items()}
    missing = {part: columns for part, columns in missing.items() if columns}
    assert not missing, f"schema columns no long name maps to: {missing}"

def test_crosswalk_short_names_are_unique():
    #two long names on one short name would give a dataframe with duplicate columns
    repeated = {part: len(xw) - len(set(xw.values())) for part, xw in longToShortXW.items()}
    assert not any(repeated.values()), f"long names sharing a short name: {repeated}"

def test_crosswalk_only_columns_are_version_codes():
    #the diagnosis version codes of hhaBase, hospBase and snfBase are not in the schemas, so enforce_schema would drop them
    #from a file that has them; tests/data/test_files.py's test_columns_are_in_schema says if one does
    extra = {part: sorted(c for c in set(xw.values()) - set(schemas[part].fieldNames()) if "VRSN_CD" not in c)
             for part, xw in longToShortXW.items()}
    extra = {part: columns for part, columns in extra.items() if columns}
    assert not extra, f"crosswalk columns enforce_schema would drop: {extra}"

def test_raw_columns_start_upper_case():
    #the case is what tells a raw field from an added one, see CLAUDE.md
    bad = sorted(c for c in rawColumns if not c[0].isupper())
    assert not bad, f"schema columns not starting upper case: {bad}"

def get_upper_case_names(tree):
    '''The columns tree adds that start with a capital: literals, and f-strings when they start with a constant.'''
    names = [get_new_name(call) for call in get_method_calls(tree, "withColumn", "withColumnRenamed")]
    names = [name.values[0] if isinstance(name, ast.JoinedStr) and name.values else name for name in names]
    return {name.value for name in names
            if isinstance(name, ast.Constant) and isinstance(name.value, str) and name.value[:1].isupper()}

def test_added_columns_start_lower_case():
    #the same rule as test_pipeline.py's test_column_naming, without the data; columns the module drops never reach the
    #user (eg THRU_DT_DAYOFYEAR)
    bad = {}
    for path in cmsFiles:
        tree = parse(path)
        names = get_upper_case_names(tree) - get_dropped_columns(tree) - rawColumns - namingExceptions
        if names := {c for c in names if not datePart.match(c)}:
            bad[path.name] = sorted(names)
    assert not bad, f"added columns not starting lower case: {bad}"
