#!/bin/bash
# Installs pytest into ~/.local (pip --user) for the python of the OSC spark module, which tests/data/run.py needs
# when it runs under subspark. Run once on a login node: bash tests/data/install_pytest.sh
set -eo pipefail

sparkModule="spark/3.5.1" #the module subspark loads

#a script run with bash does not always inherit the module shell function
if ! type module >/dev/null 2>&1; then
    source /etc/profile.d/lmod.sh
fi
module load "$sparkModule"

#the interpreter spark starts the driver with
py=$(command -v "${PYSPARK_DRIVER_PYTHON:-${PYSPARK_PYTHON:-python3}}")
echo "python: $py ($("$py" -c 'import sys; print("%d.%d" % sys.version_info[:2])'))"

if "$py" -c "import pytest" 2>/dev/null; then
    "$py" -c "import pytest; print('pytest', pytest.__version__, 'already importable from', pytest.__file__)"
    exit 0
fi

#PYTHONNOUSERSITE or a virtual environment makes python ignore ~/.local, a --user install would not be found
if ! "$py" -c "import site, sys; sys.exit(not site.ENABLE_USER_SITE)"; then
    echo "ERROR: this python ignores ~/.local, install pytest in a venv instead"
    exit 1
fi

#what would go into ~/.local, shown before anything is installed (--dry-run needs pip 22.2+)
plan=$("$py" -m pip install --user --dry-run pytest 2>&1 | grep "^Would install" || true)
echo "${plan:-pip could not list what it would install}"
#pluggy and iniconfig are used by nothing but pytest; any other package (packaging, pygments, tomli, ...) is used by
#other libraries too, and its copy in ~/.local would be the one every python of this version that reads ~/.local loads
others=$(echo "${plan#Would install }" | tr ' ' '\n' | sed 's/-[^-]*$//' |
         awk 'NF && tolower($0) !~ /^(pytest|pluggy|iniconfig)$/' | tr '\n' ' ')
if [[ -z $plan || -n $others ]]; then
    echo "WARNING: ${others:-packages pip could not list }would go into ~/.local, ahead of the copies other python programs of this version use"
    read -r -p "install anyway? [y/N] " answer
    [[ $answer == [yY] ]] || exit 1
fi

"$py" -m pip install --user --quiet --no-warn-script-location pytest
"$py" -c "import pytest; print('pytest', pytest.__version__, 'installed in', pytest.__file__)"
