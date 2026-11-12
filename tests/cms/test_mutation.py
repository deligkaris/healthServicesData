'''tests/mutation.py, the mutation testing of tests/data/run.py --mutation: the mutants it makes of a source, and the run
of a module's tests on each, on a toy repo with no Spark.'''
import re
import subprocess
import sys
import textwrap

import pytest

from tests import mutation

def descriptions(source):
    return [(m.function, m.line, m.description) for m in mutation.get_mutants("m.py", textwrap.dedent(source))]

def test_comparisons_and_conditions():
    found = descriptions('''
        def icu(df):
            return F.when((F.col("REV_CNTR") >= 200) & ~(F.col("REV_CNTR") != 205), 1)
        ''')
    assert ("icu", 3, ">= -> >") in found and ("icu", 3, "!= -> ==") in found
    assert ("icu", 3, "& -> |") in found and ("icu", 3, "drop ~") in found
    assert ("icu", 3, "200 -> 201") in found and ("icu", 3, "1 -> 0") in found and ("icu", 3, "1 -> 2") in found

def test_code_lists_lose_their_first_and_last_and_window_keys_each_one():
    found = descriptions('''
        codes = ["93304", "93306", "93307"]
        each = Window.partitionBy(["DSYSRTKY", "CLAIMNO", "THRU_DT"])
        df = df.filter(F.col("x").isin(["a"]))
        ''')
    assert [d for (_, line, d) in found if line == 2] == ["drop '93304'", "drop '93307'"]
    assert [d for (_, line, d) in found if line == 3] == ["drop 'DSYSRTKY'", "drop 'CLAIMNO'", "drop 'THRU_DT'"]
    assert [d for (_, line, d) in found if line == 4] == ["drop 'a'"]

def test_a_join_becomes_inner():
    assert ("<module>", 2, "how left_outer -> inner") in descriptions('''
        df = df.join(other, on=["a"], how="left_outer")
        ''')

def test_a_condition_run_with_eval_is_mutated_inside_its_string():
    source = textwrap.dedent('''
        def add_pcp(df):
            pcpCond = 'F.col("HCFASPCL").isin(["01","08","11"])'
            return df.withColumn("pcp", F.when(eval(pcpCond), 1).otherwise(0))
        ''')
    mutants = [m for m in mutation.get_mutants("m.py", source) if m.line == 3]
    assert {m.description.split(" in ")[0] for m in mutants} == {"drop '01'", "drop '11'"}
    assert '''pcpCond = 'F.col("HCFASPCL").isin(["08","11"])\'''' in mutants[0].source

def test_docstrings_comments_and_other_strings_are_left_alone():
    found = descriptions('''
        def f(df):
            """F.col("x") >= 1 in a docstring"""
            #F.col("x") >= 1 in a comment
            return df.withColumn("name", F.regexp_extract(F.col("x"), "^[0-9]{5}$", 0))
        ''')
    assert all(line == 5 for (_, line, _) in found)
    assert {d for (_, _, d) in found} == {"0 -> 1"}

def test_every_mutant_of_the_library_modules_parses():
    #a mutant that does not parse fails its tests on the import, and would count as caught
    for module in mutation.allModules:
        mutants = mutation.get_mutants(module)
        assert len(mutants) > 10, module
        for m in mutants:
            mutation._parse(m.source)
        assert len({m.source for m in mutants}) == len(mutants)

def toy_repo(path, test):
    (path / "tests").mkdir(parents=True)
    (path / "toy.py").write_text(textwrap.dedent('''
        def flag(x):
            return 1 if x >= 200 else 0

        def untested():
            return 5
        '''))
    (path / "tests" / "test_toy.py").write_text(textwrap.dedent(f'''
        import sys
        from pathlib import Path
        sys.path.insert(0, str(Path(__file__).parents[1]))
        import toy

        def test_flag():
            {test}
        '''))
    return path

def test_the_run_reports_what_the_tests_catch(tmp_path):
    root = toy_repo(tmp_path / "repo", "assert (toy.flag(200), toy.flag(199)) == (1, 0)")
    pending = []
    results = mutation.run(["toy.py"], workers=2, tests=["tests/test_toy.py"], tmp=str(tmp_path), root=root,
                           progress=lambda soFar, left: pending.append((sum(map(len, soFar.values())), left)))
    #the report is rewritten after every mutant, so a run stopped midway leaves what it ran
    assert pending[-1] == (len(results["toy.py"]), 0) and all(ran + left == pending[-1][0] for (ran, left) in pending)
    assert mutation.report(results, 2, 1.0, pending=3)[0].endswith("3 mutants not run yet")
    outcomes = {(m.function, m.description): o for (m, o) in results["toy.py"]}
    assert outcomes[("flag", ">= -> >")] == "killed" and outcomes[("flag", "200 -> 201")] == "killed"
    assert outcomes[("untested", "5 -> 6")] == "survived"
    lines = mutation.report(results, 2, 1.0)
    assert lines[1].startswith("toy.py: ") and "  untested" in lines and "    line 6: 5 -> 6" in lines
    #the copies are gone and the repo is as it was
    assert not [p for p in tmp_path.iterdir() if p.name.startswith("mutation-")]
    assert "x >= 200" in (root / "toy.py").read_text()

def test_tests_failing_on_the_unchanged_code_stop_the_run(tmp_path):
    root = toy_repo(tmp_path / "repo", "assert False")
    with pytest.raises(RuntimeError, match="do not pass on the unchanged code"):
        mutation.run(["toy.py"], tests=["tests/test_toy.py"], tmp=str(tmp_path), root=root)

def test_subprocesses_do_not_inherit_the_cluster_spark(monkeypatch):
    #under slurm-spark-submit the job's python has these from spark-submit, and pyspark joins that JVM when it finds them
    for name in ("PYSPARK_GATEWAY_PORT", "PYSPARK_GATEWAY_SECRET", "PYSPARK_SUBMIT_ARGS"):
        monkeypatch.setenv(name, "x")
    monkeypatch.setenv("SPARK_HOME", "/spark")
    env = mutation.local_spark_environment()
    assert not {"PYSPARK_GATEWAY_PORT", "PYSPARK_GATEWAY_SECRET", "PYSPARK_SUBMIT_ARGS"} & set(env)
    assert env["SPARK_HOME"] == "/spark" and mutation._environment()["PYTHONDONTWRITEBYTECODE"] == "1"

def test_the_workers_jvms_size_themselves_for_their_2_cores(monkeypatch):
    monkeypatch.delenv("JDK_JAVA_OPTIONS", raising=False)
    assert mutation._environment()["JDK_JAVA_OPTIONS"] == "-XX:ActiveProcessorCount=2"
    monkeypatch.setenv("JDK_JAVA_OPTIONS", "-Xss4m")
    env = mutation._environment()
    assert env["JDK_JAVA_OPTIONS"] == "-Xss4m -XX:ActiveProcessorCount=2"
    #the spark JVM a worker's tests start reads it
    code = ("from pyspark.sql import SparkSession; s = SparkSession.builder.master('local[1]').getOrCreate(); "
            "print('cores', s.sparkContext._jvm.java.lang.Runtime.getRuntime().availableProcessors()); s.stop()")
    run = subprocess.run([sys.executable, "-c", code], env=env, capture_output=True, text=True, timeout=300)
    assert "cores 2" in run.stdout, run.stderr[-2000:]

def test_a_jvm_that_died_is_an_error_not_a_kill(tmp_path):
    #pytest fails, but on a dead spark JVM, which says nothing of the mutant
    root = toy_repo(tmp_path / "repo", "raise ConnectionRefusedError(61, 'Connection refused')")
    assert mutation.run_tests(root, ["tests/test_toy.py"], timeout=120) == "error"
    root = toy_repo(tmp_path / "repo2", "assert toy.flag(200) == 0")
    assert mutation.run_tests(root, ["tests/test_toy.py"], timeout=120) == "killed"

def test_the_tests_of_each_module_are_there():
    #a test file or id that no longer matches (a renamed test) would stop the run at OSC on its baseline, a job lost
    assert set(mutation.otherTests) <= set(mutation.allModules)
    assert mutation.default_tests("cms/mbsf.py") == ["tests/cms/test_mbsf.py"]
    assert mutation.default_tests("utilities.py")[0] == mutation.default_tests("cms/utilities.py")[0] == "tests/cms/test_utilities.py"
    for module in mutation.allModules:
        for test in mutation.default_tests(module):
            (path, *names) = test.split("::")
            source = (mutation.repo / path).read_text(encoding="utf-8")
            assert all(re.search(rf"(class|def) {name}\b", source) for name in names), test
