#!/bin/bash

SPARK_HOME=/opt/Beaver/spark
OAP_HOME=/root
scriptName=$(basename $BASH_SOURCE)
workDir=`cd ${BASH_SOURCE%/*}; pwd`

# source environment variables and check requirements
function sourceEnv {
    [ -f $OAP_HOME/.bash_profile ] && . $OAP_HOME/.bash_profile
    [ -f $OAP_HOME/.bash_login ] && . $OAP_HOME/.bash_login
    [ -f $OAP_HOME/.profile ] && . $OAP_HOME/.profile
    [ -f $OAP_HOME/.bashrc ] && . $OAP_HOME/.bashrc
}

# create task directory and run periodic task
function runTask {
    if [ "$?" -ne 0 ]; then return 1; fi
    cd ${workDir}/${resDir}
    oap_version=`cat $dir_repo/output/spark/spark-defaults.conf | grep spark.executor.extraClassPath | awk -F " " '{print $2}' | awk -F "-with" '{print $1}' |  awk -F "./oap-" '{print $2}'`
    mkdir -p ./src/test/oap-perf-suite/conf
    cp -r ../src/test/oap-perf-suite/conf/oap-benchmark-default.conf ./src/test/oap-perf-suite/conf
    cp -r ../oap-${oap_version}-test-jar-with-dependencies.jar ./
    $SPARK_HOME/bin/spark-submit \
    --master yarn \
    --deploy-mode client \
    --class org.apache.spark.sql.OapPerfSuite \
    ${workDir}/oap-${oap_version}-test-jar-with-dependencies.jar \
    -r 3 \
    1>./testres \
    2>&3
}

function beforeRun {
    cd $workDir
    resDir=oap_perf_suite_result
    rm -rf $resDir
    mkdir -p $resDir
    today=$(date +%Y_%m_%d)
    i=0
    while [ -d ${today}_$i ]; do (( i++ )); done
    res_bak_dir=${today}_$i
    mkdir $res_bak_dir && cd $resDir
    fail="false"
    sourceEnv
}


function main {
    args=`getopt -a -o rd: -l rerun,dir: -- "$@"`
    eval set -- "${args}"
    runType=""
    dir_repo="~/"
    while true
    do
        case "$1" in
        -r|--rerun)
	    runType="rerun"
	    ;;
	    -d|--dir)
	    dir_repo="$2"
	    shift
	    ;;
        --)
	    shift
	    break
	    ;;
	esac
    shift
    done

    if [ ! -d "$dir_repo" ]; then
        echo "Please input right dir_repo where you will keep results finally " >&2
        exit 1
    else
        dir_repo=$(cd $dir_repo; pwd)
    fi

    if [ "${runType}" = "rerun" ]; then
	    beforeRun
        exec 3>./testlog
        if [ "$?" -ne 0 ]; then fail="true"; fi
        [ $fail = "false" ] && runTask
        if [ "$?" -ne 0 ]; then fail="true"; fi
        if [ $fail = "true" ]; then
            echo -e "Hello guys, oap-perf suite test:`basename $dir_repo` fails due to following reason. Details:\n""$(cat ${workDir}/${resDir}/testlog)"
            exit 1
        else
            rm -rf $workDir/$resDir/*.jar
            rm -rf $workDir/$resDir/src
            cp -r $dir_repo/output $workDir/$resDir/conf
            cp -r $workDir/$resDir/* $workDir/$res_bak_dir
            echo "final result is save in $workDir/$res_bak_dir"
        fi
    fi
}

if [ -n "$*" ]; then
    main $*;
else
    main
fi
