'''Runs the data tests from a batch job, the way an analysis script runs: python tests/data/run.py [options] [pytest options]
eg python tests/data/run.py -k "not carLine" (see tests/conftest.py for the pytest options). The tests run as two pytest
sessions, test_pipeline, test_transfers, test_regression and test_benchmarks second, each printing its own report. When
spark makes no progress for stallMinutes, stalls.py prints what the driver is doing to stderr (under subspark, the
.spark.log, not the .log).

Steps after the data tests run each in a python of its own with a local spark, as the unit tests need. subspark passes
no options to the script, but it submits with --export=ALL, so they are set by environment variables on its command
line, eg
    RUN_MUTATION=1 MUTATION_WORKERS=16 subspark run.py -n 1 -m 20G -c 10 -d 100G -t 27:00:00 -w 1
    RUN_UNIT       1 or 0, the unit tests, pytest tests/cms (default 0)
    RUN_MUTATION   1 or 0, mutation testing of every library module (tests/mutation.allModules), hours; its report,
                   mutation-<date>.txt, is written to the current directory, the job folder under subspark, after every
                   mutant, so a job out of time leaves what it ran (default 0)
    MUTATION_WORKERS  the mutants run in parallel, each a local spark of 2 cores and about 2 GB (default 4)
The defaults are in the block below. Run directly, the options --unit, --mutation and --workers override the variables.
The data tests always run. The steps run are printed first.'''
import argparse
import datetime
import os
import subprocess
import sys
from pathlib import Path

import pytest

import stalls

################
yearInitial = 2018 #set the years to test
yearFinal = 2019
################
pathToAHAData = '/users/PAS2164/deligkaris/DATA/AHA' #the complete AHA data, see sepsis-transfers.py
pathTmp = '/users/PAS2164/deligkaris/DATA/TMP/dataTests' #the enricher tests' sampled data, removed at the end
pathSnapshots = '/users/PAS2164/deligkaris/DATA/SNAPSHOTS' #test_regression's snapshots, kept: never in the repo or git
stallMinutes = 15 #no spark job or task progress for this long prints the driver's state, see stalls.py
stallDumps = 3 #the JVM threads are dumped this many times, stallDumpSeconds apart, to tell a stuck thread from a slow one
stallDumpSeconds = 60
################
#the defaults of the steps of the job, overridden by the environment variables of the docstring
runUnit = False #the unit tests, pytest tests/cms
runMutation = False #mutation testing of every library module, see tests/mutation.py; report in the job folder
mutationWorkers = 4 #mutants run in parallel, each a local spark of 2 cores and about 2 GB
################

def setting(name, default):
    '''The environment variable name, of default's type (1 or 0 for a bool), or default when it is not set.'''
    value = os.environ.get(name)
    if value is None:
        return default
    if isinstance(default, bool):
        if value.strip().lower() not in ("1", "0", "true", "false", "yes", "no"):
            sys.exit(f"{name}={value}: expected 1 or 0")
        return value.strip().lower() in ("1", "true", "yes")
    return type(default)(value)

repo = Path(__file__).resolve().parents[2]
tests = repo / "tests" / "data"
stepOptions = argparse.ArgumentParser(add_help=False)
stepOptions.add_argument("--unit", action="store_true", default=setting("RUN_UNIT", runUnit))
stepOptions.add_argument("--mutation", action="store_true", default=setting("RUN_MUTATION", runMutation))
stepOptions.add_argument("--workers", type=int, default=setting("MUTATION_WORKERS", mutationWorkers))
#the rest of the command line goes to pytest
(steps, pytestOptions) = stepOptions.parse_known_args()
#first in the log, so a job shows at once whether the variables reached it
print(f"run.py steps: data tests yes, unit tests {'yes' if steps.unit else 'no'}, "
      f"mutation {f'yes, {steps.workers} workers' if steps.mutation else 'no'}", flush=True)
#test_pipeline, test_transfers, test_regression and test_benchmarks run get_cms_data on the loaded claims, and pytest prints
#the failures only at the end of a session, so a hang there would take the report of every other test with it
fullPipeline = [str(tests / name) for name in ("test_pipeline.py", "test_transfers.py", "test_regression.py", "test_benchmarks.py")]
sessions = [[str(tests), *[f"--ignore={path}" for path in fullPipeline]], fullPipeline]
#options given on the command line come last, so they override the ones set here
#--tb=short keeps a failure to a few lines in the log, --durations=20 lists the slowest tests for sizing the job,
#faulthandler_timeout prints the python traceback (to stderr) of a test still running after an hour
options = ["--run-data", "--rootdir", str(repo), "--tb=short", "--durations=20", "-o", "faulthandler_timeout=3600",
           "--years", str(yearInitial), str(yearFinal), "--aha-path", pathToAHAData, "--tmp-path", pathTmp,
           "--snapshot-path", pathSnapshots, *pytestOptions]
exitCodes = []
#pytest captures fd 2 during a test and shows it only when the test fails, which a hung test never does, so the report
#goes to a copy of fd 2 made before pytest starts
stopStalls = stalls.start(os.fdopen(os.dup(2), "w"), stallMinutes, stallDumps, stallDumpSeconds)
#both sessions run in this python, the one spark started the driver with, so they share its spark session
exitCodes += [pytest.main([*paths, *options]) for paths in sessions]
#the data session sits idle while the later steps run in their own python, which the watcher would report as a stall
stopStalls.set()
#the later steps build the local[2] session of tests/conftest.py in a python of their own: in this one getOrCreate would
#hand them the cluster session, and with this python's environment they would join the cluster's JVM
sys.path.insert(0, str(repo))
from tests.mutation import local_spark_environment
environment = local_spark_environment()
if steps.unit:
    #one session runs all the unit tests, and the node has the memory the laptop does not (see tests/conftest.py)
    exitCodes.append(subprocess.run([sys.executable, "-m", "pytest", str(repo / "tests" / "cms"), "--rootdir", str(repo),
                                     "-q", "-p", "no:cacheprovider", "--tb=short"], cwd=repo,
                                    env={**environment, "TESTS_SPARK_MEMORY": "4g"}).returncode)
if steps.mutation:
    report = Path.cwd() / f"mutation-{datetime.datetime.now():%Y%m%d-%H%M}.txt"
    exitCodes.append(subprocess.run([sys.executable, str(repo / "tests" / "mutation.py"), "--workers", str(steps.workers),
                                     "--out", str(report)], cwd=repo, env=environment).returncode)
#a -k that selects the tests of one session only leaves the other with none
ran = [code for code in exitCodes if code != pytest.ExitCode.NO_TESTS_COLLECTED]
sys.exit(max(ran) if ran else pytest.ExitCode.NO_TESTS_COLLECTED)
