#!/bin/bash
# This script is used to run one configuration.

BEAVER_HOME=$(cd $(dirname ${BASH_SOURCE[0]})/..;pwd)
echo original parameters=[$@]
args=`getopt -a -o rgpd: -l rerun,gen,plugin,dir: -- "$@"`
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
    -g|--gen)
        runType="gen"
        ;;
    -p|--plugin)
        plugin=""
        ;;
    -d|--dir)
        repo=$2
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
    echo "Usage: $0 -r|--rerun|-g|--gen|-p|--plugin  -d|--dir conf_dir" >&2
    exit 1
else
    repo=$(cd $repo; pwd)
fi

if [ "${runType}" = "rerun" ]; then
    echo "RUNING DailyTest"
    cd $BEAVER_HOME
    sh $BEAVER_HOME/scripts/daily_test.sh update $repo  $plugin
    sh $BEAVER_HOME/scripts/daily_test.sh run $repo $plugin
elif [ "${runType}" = "gen" ]; then
    sh $BEAVER_HOME/scripts/daily_test.sh deploy $repo $plugin
    sh $BEAVER_HOME/scripts/daily_test.sh gen_data $repo $plugin
else
    echo "Usage: $0 -r|--rerun|-g|--gen|-p|--plugin  -d|--dir conf_dir" >&2
fi
