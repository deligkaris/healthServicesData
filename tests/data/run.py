'''Runs the data tests from a batch job, the way an analysis script runs: python tests/data/run.py [pytest options]
eg python tests/data/run.py -k "not carLine" (see tests/conftest.py for the options). The tests run as two pytest
sessions, test_pipeline and test_transfers second, each printing its own report.'''
import sys
from pathlib import Path

import pytest

################
yearInitial = 2018 #set the years to test
yearFinal = 2019
################
pathToAHAData = '/users/PAS2164/deligkaris/DATA/AHA' #the complete AHA data, see sepsis-transfers.py
pathTmp = '/users/PAS2164/deligkaris/DATA/TMP/dataTests' #the sampled CMS files of the enricher tests, removed at the end
################

repo = Path(__file__).resolve().parents[2]
tests = repo / "tests" / "data"
#test_pipeline and test_transfers run get_cms_data on all the loaded claims, and pytest prints the failures only at the
#end of a session, so a hang there would take the report of every other test with it
fullPipeline = [str(tests / "test_pipeline.py"), str(tests / "test_transfers.py")]
sessions = [[str(tests), *[f"--ignore={path}" for path in fullPipeline]], fullPipeline]
#options given on the command line come last, so they override the ones set here
#--tb=short keeps a failure to a few lines in the log, --durations=20 lists the slowest tests for sizing the job,
#faulthandler_timeout prints the python traceback (to stderr) of a test still running after an hour
options = ["--run-data", "--rootdir", str(repo), "--tb=short", "--durations=20", "-o", "faulthandler_timeout=3600",
           "--years", str(yearInitial), str(yearFinal), "--aha-path", pathToAHAData, "--tmp-path", pathTmp, *sys.argv[1:]]
#both sessions run in this python, the one spark started the driver with, so they share its spark session
exitCodes = [pytest.main([*paths, *options]) for paths in sessions]
#a -k that selects the tests of one session only leaves the other with none
ran = [code for code in exitCodes if code != pytest.ExitCode.NO_TESTS_COLLECTED]
sys.exit(max(ran) if ran else pytest.ExitCode.NO_TESTS_COLLECTED)
