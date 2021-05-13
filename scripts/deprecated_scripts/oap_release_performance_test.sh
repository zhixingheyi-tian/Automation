#!/bin/bash
# This script is used to run all configurations one by one.

BEAVER_HOME=$(cd $(dirname ${BASH_SOURCE[0]})/..;pwd)
base_repo=$(cd $1; pwd)
repo=${base_repo}/output/output_workflow

if [ ! -f "${base_repo}/.base" ]; then
    echo "Please define .base in your workflow! "
    exit 1
fi

#generate workflow
python $BEAVER_HOME/utils/workflow.py ${base_repo}

#compile oap
compile_repo=$(dirname $(find -P $repo -name ".base" | head -1))
python $BEAVER_HOME/core/oap.py compile $compile_repo oap

#oap-cache
if [ ! -d "$repo/oap-cache/" ]; then
   echo "No oap-cache repo is created, skip oap-cache."
else
    sh $BEAVER_HOME/scripts/oap_release_tpc_workload_all_config.sh -r -w tpcds -d $repo/oap-cache/ -i 3 -q all -s hao.jin@intel.com.com,xiangxiang.shen@intel.com,kunshang.ji@intel.com
fi

#oap-shuffle
if [ ! -d "$repo/oap-shuffle/RPmem-shuffle/" ]; then
    echo "No RPmem-shuffle repo is created, skip RPMem-shuffle."
else
    index=0
    for dir in `ls $repo/oap-shuffle/RPmem-shuffle/`;
    do
        if [[ $dir  =~ "TERASORT" ]]
        then
	        sh $BEAVER_HOME/scripts/oap_release_hibench_all_config.sh -r -d $repo/oap-shuffle/RPmem-shuffle/ -q  $index -s hao.jin@intel.com,xiangxiang.shen@intel.com,eugene.ma@intel.com -w micro/terasort
	    elif [[ $dir  =~ "TPCDS" ]]
	    then
            sh $BEAVER_HOME/scripts/oap_release_tpc_workload_all_config.sh -r -w tpcds -d $repo/oap-shuffle/RPmem-shuffle/ -i 1 -q $index -s hao.jin@intel.com,xiangxiang.shen@intel.com,eugene.ma@intel.com
	    else
	        echo "$dir is not used to generate data!"
	    fi
	    index=$(( $index + 1 ))
	done
fi

#oap-data-source
if [ ! -d "$repo/oap-data-source/" ]; then
    echo "No oap-data-source repo is created, skip oap-data-source."
else
    sh $BEAVER_HOME/scripts/oap_release_tpc_workload_all_config.sh -r -w tpch -d $repo/oap-data-source/ -i 1 -q all -s hao.jin@intel.com,hongze.zhang@intel.com
fi

#oap-native-sql
if [ ! -d "$repo/oap-native-sql/" ]; then
    echo "No oap-native-sql repo is created, skip oap-native-sql."
else
    sh $BEAVER_HOME/scripts/oap_release_tpc_workload_all_config.sh -r -w tpch -d $repo/oap-native-sql/ -i 1 -q all -s hao.jin@intel.com,yuan.zhou@intel.com
fi

#oap-spark
if [ ! -d "$repo/oap-spark/" ]; then
    echo "No oap-spark repo is created, skip oap-spark."
else
    sh $BEAVER_HOME/scripts/oap_release_hibench_all_config.sh -r -d $repo/oap-spark/ -q all -s hao.jin@intel.com,xiangxiang.shen@intel.com,yuqiang.ye@intel.com -w ml/kmeans
fi

#oap-mllib
if [ ! -d "$repo/oap-mllib/" ]; then
    echo "No oap-mllib repo is created, skip oap-mllib."
else
    sh $BEAVER_HOME/scripts/oap_release_hibench_all_config.sh -r -d $repo/oap-mllib/ -q all -s hao.jin@intel.com -w ml/kmeans
fi
