#!/bin/bash

if [[ ! -z "$ZSH_NAME" ]]; then
    BASE_DIR=$(dirname "$(cd $(dirname ${(%):-%x}) >/dev/null && pwd)")
else
    BASE_DIR=$(dirname "$(cd $(dirname "${BASH_SOURCE[0]}") >/dev/null && pwd)")
fi

export BEAVER_HOME=$BASE_DIR
export PYTHONPATH=$PYTHONPATH:$BEAVER_HOME
export PACKAGE_SERVER=10.239.44.95
export no_proxy="127.0.0.1, localhost, *.intel.com, 10.239.44.*, *.sh.intel.com"
