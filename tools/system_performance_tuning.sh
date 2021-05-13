#!/bin/bash

# USE Integrate baseline performance System - related parameters.
# version 1.0
# Author: feiren 2017-07-07

slaves_hosts=$1
# system_performance_tuning(){
#     sysctl -w net.core.somaxconn=1024
#     sysctl -w vm.swappiness=0
#     echo never > /sys/kernel/mm/transparent_hugepage/defrag
#     cpupower frequency-set -g performance
# }

# echo "system performance tuning......"
# system_performance_tuning > /dev/null 2>&1
function usage {
    echo "Usage: system_performance_tuning.sh slaves_hosts_file"
    echo "/opt/Beaver/hadoop/etc/hadoop/slaves"
    exit 1
}

which pssh > /dev/null 2>&1
if [ $? -ne 0 ]; then
    echo "Script must be run where pssh is installed"
    exit 1
fi
# Sanity checking.
if [ X"$slaves_hosts" = "X" ]; then
    usage
fi
psshfun(){
pssh -h $slaves_hosts -l root sysctl -w net.core.somaxconn=1024
pssh -h $slaves_hosts -l root sysctl -w vm.swappiness=0
pssh -h $slaves_hosts -l root echo never > /sys/kernel/mm/transparent_hugepage/defrag
pssh -h $slaves_hosts -l root cpupower frequency-set -g performance
}
psshfun > /dev/null 2>&1
wait
echo "pssh command is over..."
exit 0
