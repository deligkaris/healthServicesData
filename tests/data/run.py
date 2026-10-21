'''Runs the data tests from a batch job, the way an analysis script runs: python tests/data/run.py [pytest options]
eg python tests/data/run.py -k "not carLine" (see tests/conftest.py for the options).'''
import sys
from pathlib import Path

import pytest

################
yearInitial = 2018 #set the years to test
yearFinal = 2019
################
pathToAHAData = '/users/PAS2164/deligkaris/DATA/AHA' #the complete AHA data, see sepsis-transfers.py
################

repo = Path(__file__).resolve().parents[2]
#options given on the command line come last, so they override the ones set here
#--tb=short keeps a failure to a few lines in the log, --durations=20 lists the slowest tests for sizing the job
sys.exit(pytest.main([str(repo / "tests" / "data"), "--run-data", "--rootdir", str(repo), "--tb=short", "--durations=20",
                      "--years", str(yearInitial), str(yearFinal), "--aha-path", pathToAHAData, *sys.argv[1:]]))
