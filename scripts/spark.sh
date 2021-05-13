#!/bin/bash
BEAVER_HOME=$(cd $(dirname ${BASH_SOURCE[0]})/..;pwd)

if [ "$#" -lt 2 ]; then
  echo "Usage: $0 compile|deploy|update|restart|undeploy conf_dir" >&2
  exit 1
fi
if ! [ -d "$2" ]; then
  echo "$2 is not a directory" >&2
  exit 1
fi

action=$1
conf_dir=$2
shift 2

echo "$action spark..."
python $BEAVER_HOME/benchmark/TPCDSonSparkSQL.py $action $conf_dir "$@"
