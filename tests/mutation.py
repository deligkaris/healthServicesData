'''Mutation testing: makes one small change at a time to a library module (a comparison, a condition, a number, a code
list, a window key, a join), runs the module's tests on it, and reports the changes no test catches, the code whose
result nothing checks.

Usage: python tests/mutation.py MODULE [MODULE ...] [--workers N] [--tests TEST ...] [--tmp DIR] [--timeout S] [--out PATH]

    MODULE      a path from the repo root, eg cms/revenue.py
    --workers   mutants run in parallel, each worker on its own copy of the repo in --tmp (default $TMPDIR), so the
                working tree is never changed; each test run builds its own local Spark, about 2 GB a worker
    --tests     the tests to run (default: tests/cms/test_<module>.py)
    --timeout   seconds a mutant's tests may take, a timeout counts as caught (default 600)
    --out       where the report is written (default mutation-<date>.txt in the current directory)

The report holds only code. The exit code is 0 once every mutant has run, whatever survived; 2 if the tests fail on
the unchanged code, which would make every mutant look caught. tests/data/run.py --mutation runs this at OSC.'''
import argparse
import ast
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
    for (a, b, new, node, description) in get_edits(source):
        mutated = (text[:a] + new.encode("utf-8") + text[b:]).decode("utf-8")
        try:
            _parse(mutated)
        except SyntaxError:
            continue
        owner = byPosition.get((node.lineno, node.col_offset, type(node).__name__), "<module>")
        mutants.append(Mutant(module, owner, node.lineno, description, mutated))
    return mutants

def default_tests(module):
    return [f"tests/cms/test_{Path(module).stem}.py"]

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

def run_tests(root, tests, timeout):
    '''killed, survived, timeout or error, of the tests run in root.'''
    command = [sys.executable, "-m", "pytest", *tests, "-x", "-q", "-p", "no:cacheprovider", "--no-header"]
    try:
        code = subprocess.run(command, cwd=root, env=_environment(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                              timeout=timeout).returncode
    except subprocess.TimeoutExpired:
        return "timeout"
    return {0: "survived", 1: "killed"}.get(code, "error")

def run(modules, workers=1, tests=None, tmp=None, timeout=600, root=repo, log=sys.stderr):
    '''{module: [(mutant, outcome)]}, raising RuntimeError if the tests fail on the unchanged code.'''
    testsOf = {m: tests or default_tests(m) for m in modules}
    mutants = [mutant for m in modules for mutant in get_mutants(m, (Path(root) / m).read_text(encoding="utf-8"))]
    base = Path(tempfile.mkdtemp(prefix="mutation-", dir=tmp))
    try:
        copies = []
        for i in range(max(1, workers)):
            copy = base / f"worker{i}"
            shutil.copytree(root, copy, ignore=ignored)
            copies.append(copy)
        for m in modules:
            outcome = run_tests(copies[0], testsOf[m], timeout)
            if outcome != "survived":
                raise RuntimeError(f"the tests of {m} ({' '.join(testsOf[m])}) do not pass on the unchanged code: {outcome}")
        print(f"{len(mutants)} mutants of {', '.join(modules)}, {len(copies)} workers", file=log, flush=True)
        work = queue.Queue()
        for mutant in mutants:
            work.put(mutant)
        results = []
        lock = threading.Lock()
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
                    outcome = run_tests(copy, testsOf[mutant.module], timeout)
                finally:
                    path.write_text(original, encoding="utf-8")
                with lock:
                    results.append((mutant, outcome))
                    if len(results) % max(1, len(mutants) // 10) == 0:
                        print(f"  {len(results)}/{len(mutants)} mutants run", file=log, flush=True)
        threads = [threading.Thread(target=worker, args=(copy,)) for copy in copies]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
    finally:
        shutil.rmtree(base, ignore_errors=True)
    order = {id(m): i for i, m in enumerate(mutants)}
    return {m: sorted([(mu, o) for (mu, o) in results if mu.module == m], key=lambda r: order[id(r[0])]) for m in modules}

def report(results, workers, minutes):
    lines = [f"mutation testing, {datetime.datetime.now():%Y-%m-%d %H:%M}, {workers} workers, {minutes:.0f} min"]
    for module, rows in results.items():
        count = {o: sum(1 for (_, outcome) in rows if outcome == o) for o in ("killed", "survived", "timeout", "error")}
        tested = len(rows) - count["error"]
        score = (count["killed"] + count["timeout"]) / tested if tested else 0
        lines.append(f"{module}: {len(rows)} mutants, {count['killed']} killed, {count['survived']} survived, "
                     f"{count['timeout']} timeouts, {count['error']} errors, score {score:.2f}")
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
    parser.add_argument("modules", nargs="+")
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--tests", nargs="+", default=None)
    parser.add_argument("--tmp", default=os.environ.get("TMPDIR"))
    parser.add_argument("--timeout", type=int, default=600)
    parser.add_argument("--out", default=f"mutation-{datetime.datetime.now():%Y%m%d-%H%M%S}.txt")
    args = parser.parse_args(argv)
    start = time.time()
    try:
        results = run(args.modules, args.workers, args.tests, args.tmp, args.timeout)
    except RuntimeError as e:
        print(e, file=sys.stderr)
        return 2
    lines = report(results, args.workers, (time.time() - start) / 60)
    Path(args.out).write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("\n".join(lines))
    print(f"report written to {args.out}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
