#!/bin/bash
# This script is used to run all configurations one by one.

source /root/.bashrc
BEAVER_HOME=$(cd $(dirname ${BASH_SOURCE[0]})/..;pwd)
base_repo=$(cd $1; pwd)
repo=${base_repo}/output/output_workflow

if [ ! -f "${base_repo}/.base" ]; then
    echo "Please define .base in your workflow! "
    exit 1
fi

#generate workflow
python $BEAVER_HOME/utils/workflow.py ${base_repo}

if [ ! -d "${repo}/oap-dataGen" ]; then
    echo "Please define oap-dataGen configurations on ${base_repo}/.base"
    exit 1
else
    repo=$(cd $repo/oap-dataGen; pwd)
    for list in `ls $repo`
    do
        if [ -d $repo/$list ]; then
             # oap-cache use typical scripts to gen data.
             if [[ $list =~ "oap_cache" ]]; then
                 sh $BEAVER_HOME/scripts/spark.sh update $repo/$list
                 sh $BEAVER_HOME/scripts/spark.sh gen_data $repo/$list
             # oap-native-sql and oap-data-source
             elif [[ $list =~ "oap_native_sql_and_arrow_data_source" ]]; then
                 sh $BEAVER_HOME/scripts/tpc_h.sh update $repo/$list
                 sh $BEAVER_HOME/scripts/tpc_h.sh gen_data $repo/$list
             # oap-rpmem-shuffle
             elif [[ $list =~ "oap_rpmem_shuffle_terasort" ]]; then
                  python $BEAVER_HOME/benchmark/HBonSparkSQL.py update $repo/$list
                  python $BEAVER_HOME/benchmark/HBonSparkSQL.py gen_data $repo/$list micro/terasort
             elif [[ $list =~ "oap_rpmem_shuffle_TPCDS" ]]; then
                 sh $BEAVER_HOME/scripts/spark.sh update $repo/$list
                 sh $BEAVER_HOME/scripts/spark.sh gen_data $repo/$list
             # oap-spark
             elif [[ $list =~ "oap_spark_kmeans" ]]; then
                 python $BEAVER_HOME/benchmark/HBonSparkSQL.py update $repo/$list
                 python $BEAVER_HOME/benchmark/HBonSparkSQL.py gen_data $repo/$list ml/kmeans
             fi
        fi
    done
fi
