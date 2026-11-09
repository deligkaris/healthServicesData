'''tests/mutation.py, the mutation testing of tests/data/run.py --mutation: the mutants it makes of a source, and the run
of a module's tests on each, on a toy repo with no Spark.'''
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
    for module in ("cms/revenue.py", "cms/line.py"):
        mutants = mutation.get_mutants(module)
        assert len(mutants) > 50
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
    results = mutation.run(["toy.py"], workers=2, tests=["tests/test_toy.py"], tmp=str(tmp_path), root=root)
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
