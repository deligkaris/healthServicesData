'''Mutation testing: makes one small change at a time to a library module (a comparison, a condition, a number, a code
list, a window key, a join), runs the module's tests on it, and reports the changes no test catches, the code whose
result nothing checks.

Usage: python tests/mutation.py [MODULE ...] [--workers N] [--tests TEST ...] [--tmp DIR] [--timeout S] [--out PATH]

    MODULE      a path from the repo root, eg cms/revenue.py (default: every module of allModules)
    --workers   mutants run in parallel, each worker on its own copy of the repo in --tmp (default $TMPDIR), so the
                working tree is never changed; each test run builds its own local Spark, about 2 GB a worker
    --tests     the tests to run (default: tests/cms/test_<module>.py, then those in otherTests)
    --timeout   seconds a mutant's tests may take at least, a timeout counts as caught (default 600); a module whose
                tests take longer than a third of it on the unchanged code gets 3 times that
    --out       where the report is written, after every mutant, so a run stopped midway (a job out of time) leaves
                what it ran (default mutation-<date>.log in the current directory)

The report holds only code. The exit code is 0 once every mutant has run, whatever survived; 2 if the tests fail on
the unchanged code, which would make every mutant look caught. tests/data/run.py --mutation runs this at OSC.'''
import argparse
import ast
import concurrent.futures
import datetime
import os
import queue
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import warnings
from dataclasses import dataclass
from pathlib import Path

repo = Path(__file__).resolve().parents[1]
compareSwaps = {ast.Lt: ("<", "<="), ast.LtE: ("<=", "<"), ast.Gt: (">", ">="), ast.GtE: (">=", ">"),
                ast.Eq: ("==", "!="), ast.NotEq: ("!=", "==")}
#the calls whose list arguments are keys, each one dropped in turn; other lists of constants lose their first and last
keyCalls = {"partitionBy", "groupBy", "orderBy"}
listCalls = {"isin"} | keyCalls
#the copies leave these out
ignored = shutil.ignore_patterns(".git", "htmlcov", "__pycache__", "*.pyc", ".coverage", "mutation-*")

@dataclass
class Mutant:
    module: str
    function: str
    line: int
    description: str
    source: str

def _offsets(source):
    '''Byte offset of the start of each line, as ast columns count bytes.'''
    starts = [0]
    for line in source.encode("utf-8").splitlines(keepends=True):
        starts.append(starts[-1] + len(line))
    return starts

def _span(node, starts):
    return starts[node.lineno - 1] + node.col_offset, starts[node.end_lineno - 1] + node.end_col_offset

def _owners(tree):
    '''{node: name of the innermost function it is in, "<module>" outside one}.'''
    owners = {}
    def visit(node, owner):
        owners[node] = owner
        for child in ast.iter_child_nodes(node):
            visit(child, node.name if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) else owner)
    visit(tree, "<module>")
    return owners

def _docstrings(tree):
    nodes = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) and node.body:
            first = node.body[0]
            if isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant) and isinstance(first.value.value, str):
                nodes.add(first.value)
    return nodes

def _is_condition(node):
    '''A string with a pyspark condition in it, run with eval (eg pcpCond = 'F.col("HCFASPCL").isin([...])').'''
    if not (isinstance(node, ast.Constant) and isinstance(node.value, str) and "F." in node.value):
        return False
    try:
        _parse(node.value, mode="eval")
        return True
    except SyntaxError:
        return False

def _parse(source, mode="exec"):
    #the library's non-raw regex strings ('^[\d]{5}$') warn of an invalid escape on every parse
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return ast.parse(source, mode=mode)

def get_edits(source):
    '''[(start, end, replacement, node, description)] in bytes of source, one per mutant.'''
    tree = _parse(source)
    starts = _offsets(source)
    text = source.encode("utf-8")
    parents = {child: node for node in ast.walk(tree) for child in ast.iter_child_nodes(node)}
    docstrings = _docstrings(tree)
    edits = []
    def between(a, b):
        return _span(a, starts)[1], _span(b, starts)[0]
    for node in ast.walk(tree):
        if isinstance(node, ast.Compare):
            for (left, op, right) in zip([node.left] + node.comparators[:-1], node.ops, node.comparators):
                if type(op) in compareSwaps:
                    (old, new) = compareSwaps[type(op)]
                    #between the two operands there is only this operator, and the parentheses and spaces around it
                    (a, b) = between(left, right)
                    at = text.find(old.encode(), a, b)
                    if at >= 0:
                        edits.append((at, at + len(old), new, node, f"{old} -> {new}"))
        elif isinstance(node, ast.BinOp) and isinstance(node.op, (ast.BitAnd, ast.BitOr)):
            (old, new) = ("&", "|") if isinstance(node.op, ast.BitAnd) else ("|", "&")
            (a, b) = between(node.left, node.right)
            at = text.find(old.encode(), a, b)
            if at >= 0:
                edits.append((at, at + 1, new, node, f"{old} -> {new}"))
        elif isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.Invert):
            (a, _) = _span(node, starts)
            if text[a:a + 1] == b"~":
                edits.append((a, a + 1, "", node, "drop ~"))
        elif isinstance(node, ast.Constant) and type(node.value) is int:
            (a, b) = _span(node, starts)
            for value in sorted({node.value + 1} | ({1 - node.value} if node.value in (0, 1) else set())):
                edits.append((a, b, str(value), node, f"{node.value} -> {value}"))
        elif isinstance(node, (ast.List, ast.Tuple)) and node.elts and all(isinstance(e, ast.Constant) for e in node.elts):
            parent = parents.get(node)
            call = getattr(getattr(parent, "func", None), "attr", None) if isinstance(parent, ast.Call) else None
            if not (call in listCalls or isinstance(parent, ast.Assign)):
                continue
            drop = range(len(node.elts)) if call in keyCalls else sorted({0, len(node.elts) - 1})
            if len(node.elts) == 1 and not isinstance(node, ast.List):
                continue
            for i in drop:
                elts = node.elts
                if len(elts) == 1:
                    (a, b) = _span(elts[0], starts)
                elif i < len(elts) - 1:
                    (a, b) = (_span(elts[i], starts)[0], _span(elts[i + 1], starts)[0])
                else:
                    (a, b) = (_span(elts[i - 1], starts)[1], _span(elts[i], starts)[1])
                edits.append((a, b, "", node, f"drop {elts[i].value!r}"))
        elif isinstance(node, ast.keyword) and node.arg == "how" and isinstance(node.value, ast.Constant) and \
                node.value.value in ("left_outer", "left", "leftouter"):
            (a, b) = _span(node.value, starts)
            quote = text[a:a + 1].decode()
            edits.append((a, b, f"{quote}inner{quote}", node, f"how {node.value.value} -> inner"))
        elif _is_condition(node) and node not in docstrings:
            (a, b) = _span(node, starts)
            literal = text[a:b].decode("utf-8")
            quote = literal[0]
            #only a plain one-line string whose text is its value, so the inner edit can go back between its quotes
            if node.lineno != node.end_lineno or literal[-1] != quote or literal[1:-1] != node.value or quote not in "'\"":
                continue
            for (ia, ib, new, _, description) in get_edits(node.value):
                inner = node.value.encode("utf-8")
                mutated = (inner[:ia] + new.encode("utf-8") + inner[ib:]).decode("utf-8")
                if quote in mutated:
                    continue
                edits.append((a, b, f"{quote}{mutated}{quote}", node, f"{description} in {literal}"))
    return edits

def get_mutants(module, source=None):
    '''The mutants of module, a path from the repo root, each a whole source that parses.'''
    source = source if source is not None else (repo / module).read_text(encoding="utf-8")
    tree = _parse(source)
    owners = _owners(tree)
    text = source.encode("utf-8")
    #the nodes of get_edits come from its own parse, matched back by position
    byPosition = {(n.lineno, n.col_offset, type(n).__name__): owners[n] for n in ast.walk(tree) if hasattr(n, "lineno")}
    mutants = []
    seen = set()
    for (a, b, new, node, description) in get_edits(source):
        mutated = (text[:a] + new.encode("utf-8") + text[b:]).decode("utf-8")
        #dropping the first and the last of a list of one make the same mutant
        if mutated in seen:
            continue
        seen.add(mutated)
        try:
            _parse(mutated)
        except SyntaxError:
            continue
        owner = byPosition.get((node.lineno, node.col_offset, type(node).__name__), "<module>")
        mutants.append(Mutant(module, owner, node.lineno, description, mutated))
    return mutants

#every library module with mutants and tests, the quick ones first, so the report written as the run goes has them early
allModules = ["cms/claims.py", "cms/comorbidities.py", "cms/revenue.py", "cms/line.py", "cms/mbsf.py", "cms/utilities.py",
              "geocoding.py", "utilities.py", "cms/stays.py", "cms/transfers.py", "cms/base.py"]

#the tests of a module in other files, the slowest last: every file that calls a function of the module test_<module>.py
#does not (base's functions are mostly tested where the stays, transfers and nihss pipelines use them, 670 of its 912
#mutants), but test_end_to_end.py, 13 minutes, and the strict xfails of test_invariants.py, which a broken mutant keeps
#failing and only a mutant that happened to fix them would fail, as an XPASS. For stays and transfers, the invariant
#that the output does not depend on row order or partitioning, whose inputs have the ties a window key or an order decides
staysInvariant = "tests/cms/test_invariants.py::TestSameOutputInAnyOrder::test_unique_stays_and_their_source_and_destination"
transfersInvariant = "tests/cms/test_invariants.py::TestSameOutputInAnyOrder::test_node_and_dyad_columns_and_their_priors"
otherTests = {"cms/line.py": ["tests/cms/test_revenue.py"],
              "cms/utilities.py": ["tests/cms/test_base.py", "tests/cms/test_stays.py"],
              "utilities.py": ["tests/cms/test_geocoding.py", "tests/cms/test_line.py", "tests/cms/test_base.py",
                               "tests/cms/test_jc_pipeline.py", "tests/cms/test_stays.py", "tests/cms/test_transfers.py"],
              "cms/stays.py": ["tests/cms/test_base.py", staysInvariant],
              "cms/transfers.py": [transfersInvariant],
              "cms/base.py": ["tests/cms/test_aggregate_summary.py", "tests/cms/test_nihss.py", "tests/cms/test_mbsf.py",
                              "tests/cms/test_stays.py", "tests/cms/test_transfers.py"]}

def default_tests(module):
    '''tests/cms/test_<module>.py, first, as pytest stops at the first failure, then its tests in other files.'''
    return [f"tests/cms/test_{Path(module).stem}.py"] + otherTests.get(Path(module).as_posix(), [])

#spark-submit (slurm-spark-submit at OSC) sets these for the python it starts; a python inheriting them would join that
#JVM, the cluster's driver, instead of starting the local spark of tests/conftest.py
clusterEnvironment = ("PYSPARK_GATEWAY_PORT", "PYSPARK_GATEWAY_SECRET", "PYSPARK_SUBMIT_ARGS")

def local_spark_environment():
    '''os.environ without what ties a python to the spark it was started from, for a subprocess with its own local spark.'''
    return {k: v for k, v in os.environ.items() if k not in clusterEnvironment}

def _environment():
    env = local_spark_environment()
    #no bytecode: a mutant of the same size written within the second of the last one could reuse its stale .pyc
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    return env

#a test that fails because spark's JVM died (killed for memory, eg on a laptop with other spark runs) did not catch the
#mutant, so such a run is an error, not a kill
jvmGone = ("ConnectionRefusedError", "Py4JNetworkError", "Answer from Java side is empty", "OutOfMemoryError")

def run_tests(root, tests, timeout):
    '''killed, survived, timeout or error, of the tests run in root.'''
    command = [sys.executable, "-m", "pytest", *tests, "-x", "-q", "-p", "no:cacheprovider", "--no-header"]
    try:
        run = subprocess.run(command, cwd=root, env=_environment(), capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        return "timeout"
    if run.returncode != 0 and any(marker in run.stdout + run.stderr for marker in jvmGone):
        return "error"
    return {0: "survived", 1: "killed"}.get(run.returncode, "error")

def run(modules, workers=1, tests=None, tmp=None, timeout=600, root=repo, log=sys.stderr, progress=None):
    '''{module: [(mutant, outcome)]}, raising RuntimeError if the tests fail on the unchanged code. progress, if given,
    is called with the results so far and the number of mutants still to run after each mutant.'''
    testsOf = {m: tests or default_tests(m) for m in modules}
    mutants = [mutant for m in modules for mutant in get_mutants(m, (Path(root) / m).read_text(encoding="utf-8"))]
    base = Path(tempfile.mkdtemp(prefix="mutation-", dir=tmp))
    try:
        copies = []
        for i in range(max(1, workers)):
            copy = base / f"worker{i}"
            shutil.copytree(root, copy, ignore=ignored)
            copies.append(copy)
        def baseline(i, m):
            start = time.time()
            return (run_tests(copies[i % len(copies)], testsOf[m], None), time.time() - start)
        #the modules' tests on the unchanged code, side by side on the workers' copies
        with concurrent.futures.ThreadPoolExecutor(len(copies)) as pool:
            baselines = dict(zip(modules, pool.map(baseline, range(len(modules)), modules)))
        failed = [f"{m} ({' '.join(testsOf[m])}): {outcome}" for m, (outcome, _) in baselines.items() if outcome != "survived"]
        if failed:
            raise RuntimeError(f"the tests do not pass on the unchanged code: {'; '.join(failed)}")
        #a timeout counts as caught, so it must mean a mutant that hangs, not tests that are slow (test_transfers takes
        #6 minutes): 3 times the unchanged run, or timeout if longer
        timeoutOf = {m: max(timeout, 3 * seconds) for m, (_, seconds) in baselines.items()}
        print(f"{len(mutants)} mutants of {', '.join(modules)}, {len(copies)} workers", file=log, flush=True)
        work = queue.Queue()
        for mutant in mutants:
            work.put(mutant)
        results = []
        lock = threading.Lock()
        order = {id(m): i for i, m in enumerate(mutants)}
        def by_module():
            return {m: sorted([(mu, o) for (mu, o) in results if mu.module == m], key=lambda r: order[id(r[0])]) for m in modules}
        def worker(copy):
            while True:
                try:
                    mutant = work.get_nowait()
                except queue.Empty:
                    return
                path = copy / mutant.module
                original = path.read_text(encoding="utf-8")
                path.write_text(mutant.source, encoding="utf-8")
                try:
                    outcome = run_tests(copy, testsOf[mutant.module], timeoutOf[mutant.module])
                finally:
                    path.write_text(original, encoding="utf-8")
                with lock:
                    results.append((mutant, outcome))
                    if len(results) % max(1, len(mutants) // 10) == 0:
                        print(f"  {len(results)}/{len(mutants)} mutants run", file=log, flush=True)
                    if progress:
                        progress(by_module(), len(mutants) - len(results))
        threads = [threading.Thread(target=worker, args=(copy,)) for copy in copies]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
    finally:
        shutil.rmtree(base, ignore_errors=True)
    return by_module()

def report(results, workers, minutes, pending=0):
    lines = [f"mutation testing, {datetime.datetime.now():%Y-%m-%d %H:%M}, {workers} workers, {minutes:.0f} min"
             + (f", stopped or still running: {pending} mutants not run yet" if pending else "")]
    for module, rows in results.items():
        count = {o: sum(1 for (_, outcome) in rows if outcome == o) for o in ("killed", "survived", "timeout", "error")}
        tested = len(rows) - count["error"]
        score = (count["killed"] + count["timeout"]) / tested if tested else 0
        lines.append(f"{module}: {len(rows)} mutants, {count['killed']} killed, {count['survived']} survived, "
                     f"{count['timeout']} timeouts, {count['error']} errors, score {score:.2f}")
        if count["error"]:
            lines.append(f"  {count['error']} errors: the tests could not run (eg spark's JVM died), rerun them")
        function = None
        for (mutant, outcome) in rows:
            if outcome != "survived":
                continue
            if mutant.function != function:
                function = mutant.function
                lines.append(f"  {function}")
            lines.append(f"    line {mutant.line}: {mutant.description}")
    return lines

def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("modules", nargs="*", default=allModules)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--tests", nargs="+", default=None)
    parser.add_argument("--tmp", default=os.environ.get("TMPDIR"))
    parser.add_argument("--timeout", type=int, default=600)
    parser.add_argument("--out", default=f"mutation-{datetime.datetime.now():%Y%m%d-%H%M%S}.log")
    args = parser.parse_args(argv)
    start = time.time()
    def write(results, pending=0):
        lines = report(results, args.workers, (time.time() - start) / 60, pending)
        Path(args.out).write_text("\n".join(lines) + "\n", encoding="utf-8")
        return lines
    try:
        results = run(args.modules, args.workers, args.tests, args.tmp, args.timeout, progress=write)
    except RuntimeError as e:
        print(e, file=sys.stderr)
        return 2
    lines = write(results)
    print("\n".join(lines))
    print(f"report written to {args.out}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
