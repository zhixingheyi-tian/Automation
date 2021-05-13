#!/bin/bash

BEAVER_HOME=$(cd $(dirname ${BASH_SOURCE[0]})/..;pwd)
workflow=$(cd $1; pwd)
output_workflow=${workflow}/output/output_workflow
if [ ! -d "${workflow}" ]; then
    echo "Please input right workflow!"
    exit 1
fi
if [ ! -f "${workflow}/.base" ]; then
    echo "Please define .base in your workflow!"
    exit 1
fi

#generate workflow
python $BEAVER_HOME/utils/workflow.py ${workflow}

#compile oap
custom_conf=$(dirname $(find -P $output_workflow -name ".base" | head -1))
source ${BEAVER_HOME}/bin/setup-cluster.sh ${custom_conf}
