#!/bin/bash
# This script is used to run all configurations one by one.

BEAVER_HOME=$(cd $(dirname ${BASH_SOURCE[0]})/..;pwd)
script_name=$(basename $0)

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
    if ! crontab -l 2>&1 | grep -q "$scriptPath -r -d $repo -q $configs"; then
        if crontab -l 2>&1 | grep -q "no crontab for"; then
            echo "30 2 * * * sh ${scriptPath} -r -d $repo -q $configs > $repo/crontab.log" | crontab -
        else
            (crontab -l | grep -v "$scriptPath"; echo "30 2 * * * sh ${scriptPath} -r -d $repo -q $configs > $repo/crontab.log") | crontab -
        fi
    fi
}

echo original parameters=[$@]
args=`getopt -a -o rcpd:q: -l rerun,cron,plugin,dir:queue: -- "$@"`
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
    -q|--queue)
        configs=$2
        config_list=(${configs//,/ })
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
    echo "Usage: $0 -r|--rerun|-c|--cron|-p|--plugin  -d|--dir conf_dir -q|--queue repo_order" >&2
    exit 1
else
    repo=$(cd $repo; pwd)
    if [ -d "$repo/common_repo" ]; then
        update_common_repo
    fi
fi


if [ "${runType}" = "rerun" ]; then
    echo "RUNING DailyTest"
    sourceEnv
    cd $BEAVER_HOME
    j=0
    for list in `ls $repo`
    do
        if [ -d $repo/$list ] && [ $list != "common_repo" ]; then
            if [ $(contains "${config_list[@]}" "all") -eq 0 ]; then
                sh $BEAVER_HOME/scripts/daily_test.sh update $repo/$list $plugin
                sh $BEAVER_HOME/scripts/daily_test.sh run $repo/$list $plugin
            else
                if [ $(contains "${config_list[@]}" "$j") -eq 0 ]; then
                    sh $BEAVER_HOME/scripts/daily_test.sh update $repo/$list $plugin
                    sh $BEAVER_HOME/scripts/daily_test.sh run $repo/$list $plugin
                fi
                j=$[$j+1];
            fi

        fi
    done
elif [ "${runType}" = "cron" ]; then
    # crontab only edited by root
    [ `id -u` -eq 0 ] && checkCrontab
else
    echo "Usage: $0 -r|--rerun|-c|--cron  -d|--dir conf_dir -q|--queue repo_order" >&2
fi
