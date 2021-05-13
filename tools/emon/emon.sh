#!/bin/bash

action=$1

EMON_HOME={%emon.home%}
source $EMON_HOME/sep_vars.sh

if [ $action == "start" ]; then
    socket_number=`lscpu | grep Socket | awk '{print $2}'`
    cpu_mode=`lscpu | grep "Model name:" | awk '{print$6}'`
    generation_num=`echo ${cpu_mode:1:1}`
    if [ $generation_num == "1" ]; then
        generation="skx"
    elif [ $generation_num == "2" ]; then
        generation="clx"
    elif [ $generation_num == "3" ] || [ $generation_num == "0" ]; then
        generation="icx"
    fi
    events_file=${generation}-${socket_number}s-events.txt
    export HISTIGNORE='*sudo -S*'
    if [ -z "`/usr/sbin/lsmod |grep sepint4_1`" ]; then
      pushd $EMON_HOME/sepdk/src
      #remove emon driver
      echo "welcome1" | sudo -S -k ./rmmod-sep
      popd
    fi

    pushd $EMON_HOME/sepdk/src
    echo "Installing SEP driver..."
    echo "welcome1" | sudo -S -k ./insmod-sep -g wheel
    popd

    LOG_HOME=$EMON_HOME/emon_data/$(cat /etc/hostname)
    rm -rf LOG_HOME
    mkdir -p $LOG_HOME
    $EMON_HOME/bin64/emon -v > ${LOG_HOME}/emon-v.dat 2>&1 &
    $EMON_HOME/bin64/emon -M > ${LOG_HOME}/emon-M.dat 2>&1 &
    $EMON_HOME/bin64/emon -i $EMON_HOME/emon_scripts/${events_file} > $LOG_HOME/emon.dat 2>&1 &

elif [ $action == "stop" ]; then
    $EMON_HOME/bin64/emon -stop
else
    echo "Unsupported action!"
    exit 1
fi



