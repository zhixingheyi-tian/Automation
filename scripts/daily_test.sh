#!/bin/bash
BEAVER_HOME=$(cd $(dirname ${BASH_SOURCE[0]})/..;pwd)

if [ "$#" -ne 3 ]; then
  echo "Usage: $0 compile|deploy|undeploy|gen_data|run|deploy_and_run|update conf_dir plugin" >&2
  exit 1
fi
if ! [ -d "$2" ]; then
  echo "$2 is not a directory" >&2
  exit 1
fi

action=$1
conf_dir=$2
plugin=$3

echo "daily-test $action ..."
python $BEAVER_HOME/benchmark/OAPPerfonSparkSQLwithOAP.py $action $conf_dir $plugin
