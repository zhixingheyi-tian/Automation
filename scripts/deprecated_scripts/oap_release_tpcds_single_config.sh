#!/bin/bash
# This script is used to run one configuration.

BEAVER_HOME=$(cd $(dirname ${BASH_SOURCE[0]})/..;pwd)
PACKAGE_SERVER=`cat $BEAVER_HOME/bin/start-beaver.sh | grep PACKAGE_SERVER | awk -F "=" '{print $2}'`
daily_date=`date +%Y_%m_%d`

scp_result_to_server(){
    result_folder=$1
    ssh root@$PACKAGE_SERVER "mkdir -p /srv/my/repo/oap_release_performance_result/$daily_date"
#    ssh-keygen -R 10.239.47.111
    scp -r $result_folder root@$PACKAGE_SERVER:/srv/my/repo/oap_release_performance_result/$daily_date/`basename $result_folder`_`hostname`
}

analyze(){
    lastPath=${resDir}/../last_test_info
    [ -f $lastPath ] && . $lastPath
    if [ ${#lastResPath[@]} -gt 0 ]; then
        python $SPARK_SQL_PERF_HOME/tpcds_script/*/analyze.py ${lastResPath[@]} $resDir ${resDir}/result.html
    else
        python $SPARK_SQL_PERF_HOME/tpcds_script/*/analyze.py $resDir $resDir ${resDir}/result.html
    fi
    echo "lastResPath=(${resDir})" > $lastPath
    cp -r $resDir/../output $resDir/
}

echo original parameters=[$@]
args=`getopt -a -o rgi:d:s: -l rerun,gen,iteration:,dir:,send: -- "$@"`
echo ARGS=[$args]
eval set -- "${args}"

echo formatted parameters=[$@]

runType=""
iteration=1
while true
do
    case "$1" in
    -r|--rerun)
	runType="rerun"
	;;
	-g|--gen)
	runType="gen"
	;;
    -i|--iteration)
        iteration=$2
        shift
        ;;
    -d|--dir)
        repo=$2
        shift
        ;;
    -s|--send)
        contact=$2
        mailList=(${contact//,/ })
        shift
        ;;
    --)
	shift
	break
	;;
esac
shift
done


if [ ! -d "$repo" ]; then
    echo "Usage: $0 -r|--rerun|-g|--gen  -d|--dir conf_dir  -i|--iteration iteration_num -s|--send mailList" >&2
    exit 1
else
    repo=$(cd $repo; pwd)
fi



if [ "${runType}" = "rerun" ]; then
    echo "RUNING TPCDS"
    cd $BEAVER_HOME
    mailTitle="Oap TPC-DS Benchmark Reporter: `basename $repo`"
    today=$(date +%Y_%m_%d-%H:%M:%S)
    i=0
    while [ -d $repo/`basename $repo`_${today}_$i ]; do (( i++ )); done
    resDir=$repo/`basename $repo`_${today}_$i
    mkdir -p $resDir
    echo "Running $repo"
    sh $BEAVER_HOME/scripts/spark.sh update $repo oap
    if [ "$?" -ne 0 ]; then
        echo " ERROR UPDATING!"
        echo -e "Hello guys, TPC-DS case $repo update failed, please check your cluster!" | mail -s "$mailTitle" ${mailList[@]}
        exit 1
    fi
    sh $BEAVER_HOME/scripts/tpc_ds.sh run $repo $iteration
    if [ "$?" -ne 0 ]; then
        echo " ERROR RUNING!"
        echo -e "Hello guys, TPC-DS case $repo running failed, please check your cluster!" | mail -s "$mailTitle" ${mailList[@]}
        exit 1
    else
        cp -r /opt/Beaver/spark-sql-perf/tpcds_script/*/tpcds/logs $resDir
        echo "The final result is saved at $resDir"
        analyze
        scp_result_to_server $resDir
    fi
    mail -s "$(echo -e "$mailTitle $list\nContent-Type: text/html; charset=utf-8")" ${mailList[@]} < ${resDir}/result.html
elif [ "${runType}" = "gen" ]; then
    sh $BEAVER_HOME/scripts/spark.sh deploy $repo oap
    sh $BEAVER_HOME/scripts/spark.sh gen_data $repo
else
    echo "Usage: $0 -r|--rerun|-g|--gen  -d|--dir conf_dir  -i|--iteration iteration_num -s|--send mailList" >&2
fi