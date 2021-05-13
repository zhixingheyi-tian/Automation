#!/bin/bash
set -x
set -e

file_name=$1
function=$2
capacity=$3
run_times=$4
Beaver_home=$5
unique_time=$6
cd $Beaver_home

bin/runBBonHoS.py $function $Beaver_home/milestone/baseline
save_result=/home/result/$unique_time/file_name/deploy
make -p $save_result
cp -r $BB_HOME/logs/ $save_result
sed -i "s/CLEAN_DATA,//g" $BB_HOME/conf/bigBench.properties

for((times = 1; times <= $run_times; times++))
do
    save_result=/home/result/$unique_time/file_name/times
    make -p $save_result
 #   $BB_HOME/bin/bigBench runBenchmark -f $capacity
    cp -r $BB_HOME/logs/ $save_result
done

bin/runBBonHoS.py undeploy $Beaver_home/milestone/baseline


