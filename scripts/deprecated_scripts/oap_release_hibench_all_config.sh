#!/bin/bash
# This script is used to run all configurations one by one.

BEAVER_HOME=$(cd $(dirname ${BASH_SOURCE[0]})/..;pwd)
PACKAGE_SERVER=`cat $BEAVER_HOME/bin/start-beaver.sh | grep PACKAGE_SERVER | awk -F "=" '{print $2}'`
daily_date=`date +%Y_%m_%d`
script_name=$(basename $0)
commit_id=`cd $BEAVER_HOME/package/source_code/oap && git rev-parse HEAD`
if [ ! $commit_id ]; then
  commit_id="base"
fi

function sourceEnv {
    [ -f /root/.bash_profile ] && . /root/.bash_profile
    [ -f /root/.bash_login ] && . /root/.bash_login
    [ -f /root/.profile ] && . /root/.profile
    [ -f /root/.bashrc ] && . /root/.bashrc
}

function contains() {
    n=$#
    value="${@: -1}"
    for ((i=1;i < $#;i++)) {
        if [ "${!i}" == "${value}" ]; then
            echo 0
            return 0
        fi
    }
    echo 1
}

update_common_repo() {
    for list in `ls $repo`
    do
        if [ -d $repo/$list ] && [ $list != "common_repo" ]; then
            echo "Update common_config for $list"
            cp -r $repo/common_repo/* $repo/$list
        else
            echo "$list is a file"
        fi
    done
}

function checkCrontab {
    scriptPath=${BEAVER_HOME}/scripts/$script_name
    if ! crontab -l 2>&1 | grep -q "$scriptPath -r -d $repo -i $iteration -q $configs"; then
        if crontab -l 2>&1 | grep -q "no crontab for"; then
            echo "30 2 * * * sh ${scriptPath} -r -d $repo -i $iteration -q $configs > $repo/crontab.log" | crontab -
        else
            (crontab -l | grep -v "$scriptPath"; echo "3 * * * * sh ${scriptPath} -r -d $repo -i $iteration -q $configs > $repo/crontab.log") | crontab -
        fi
    fi
}

scp_result_to_server(){
    result_folder=$1
    ssh root@$PACKAGE_SERVER "mkdir -p /srv/my/repo/oap_release_performance_result/$daily_date"
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
    cp -r $resDir/../output $resDir/conf
}

echo original parameters=[$@]
args=`getopt -a -o rcpd:q:s:w: -l rerun,cron,plugin,dir:,queue:,send:,workload: -- "$@"`
echo ARGS=[$args]
eval set -- "${args}"

echo formatted parameters=[$@]

plugin="oap"
runType=""
while true
do
    case "$1" in
    -r|--rerun)
	    runType="rerun"
	    ;;
    -c|--cron)
	    runType="cron"
	    ;;
	  -p|--plugin)
	    plugin=""
	    ;;
    -d|--dir)
        repo=$2
        shift
        ;;
    -w|--workload)
        workload=$2
        shift
        ;;
    -q|--queue)
        configs=$2
        config_list=(${configs//,/ })
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
    echo "Usage: $0 -r|--rerun|-c|--cron|-p|--plugin  -d|--dir conf_dir -q|--queue repo_order -s|--send mailList -w|--workload workload" >&2
    exit 1
else
    repo=$(cd $repo; pwd)
    if [ -d "$repo/common_repo" ]; then
        update_common_repo
    fi
fi


if [ "${runType}" = "rerun" ]; then
    echo "RUNING HiBench"
    mailTitle="Oap HiBench Benchmark Reporter: `basename $repo` ${commit_id}"
    sourceEnv
    cd $BEAVER_HOME
    j=0
    for list in `ls $repo`
    do
        if [ -d $repo/$list ] && [ $list != "common_repo" ]; then
            if [ $(contains "${config_list[@]}" "all") -eq 0 ]; then
                today=$(date +%Y_%m_%d-%H:%M:%S)
                i=0
                while [ -d $repo/$list/${list}_${today}_$i ]; do (( i++ )); done
                resDir=$repo/$list/${list}_${today}_${i}_${commit_id}
                mkdir -p $resDir
                echo "Runing $list"
                python $BEAVER_HOME/benchmark/HBonSparkSQL.py update $repo/$list $plugin
                if [ "$?" -ne 0 ]; then
                    echo " ERROR Updating!"
                    echo -e "Hello guys, HiBench case $list update configuration failed, please check your cluster!" | mail -s "$mailTitle" ${mailList[@]}
                    exit 1
                fi
                python $BEAVER_HOME/benchmark/HBonSparkSQL.py run $repo/$list $workload
                if [ "$?" -ne 0 ]; then
                    echo " ERROR RUNING!"
                    echo -e "Hello guys, HiBench case $list running failed, please check your cluster!" | mail -s "$mailTitle" ${mailList[@]}
                    exit 1
                else
                    cp -r /opt/Beaver/hibench/report $resDir
                    echo "The final result is saved at $resDir"
                    scp_result_to_server $resDir
                fi
                mail -s "$(echo -e "$mailTitle\nContent-Type: text/html; charset=utf-8")" ${mailList[@]} < ${resDir}/report/hibench.report
            else
                if [ $(contains "${config_list[@]}" "$j") -eq 0 ]; then
                    today=$(date +%Y_%m_%d-%H:%M:%S)
                    i=0
                    while [ -d $repo/$list/${list}_${today}_$i ]; do (( i++ )); done
                    resDir=$repo/$list/${list}_${today}_$i
                    mkdir -p $resDir
                    echo "Runing $list"
                    python $BEAVER_HOME/benchmark/HBonSparkSQL.py update $repo/$list $plugin
                    if [ "$?" -ne 0 ]; then
                        echo " ERROR Updating!"
                        echo -e "Hello guys, HiBench case $list update configuration failed, please check your cluster!" | mail -s "$mailTitle" ${mailList[@]}
                        exit 1
                    fi
                    python $BEAVER_HOME/benchmark/HBonSparkSQL.py run $repo/$list $workload
                    if [ "$?" -ne 0 ]; then
                        echo " ERROR Running! HiBench case $list running failed, please check your cluster!" | mail -s "$mailTitle" ${mailList[@]}
                        exit 1
                    else
                        cp -r /opt/Beaver/hibench/report $resDir
                        echo "The final result is saved at $resDir"
                        scp_result_to_server $resDir
                    fi
                    mail -s "$(echo -e "$mailTitle\nContent-Type: text/html; charset=utf-8")" ${mailList[@]} < ${resDir}/report/hibench.report
                fi
                j=$[$j+1];
            fi
        fi
    done
elif [ "${runType}" = "cron" ]; then
    # crontab only edited by root
    [ `id -u` -eq 0 ] && checkCrontab
    sourceEnv
else
    echo "Usage: $0 -r|--rerun|-c|--cron  -d|--dir conf_dir -q|--queue repo_order -s|--send mailList -w|--workload workload" >&2
fi
