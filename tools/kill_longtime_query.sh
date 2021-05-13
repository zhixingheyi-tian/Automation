#!/bin/bash

# pid_time=($(ps -eo pid,etime,cmd|grep -v 'grep'|grep 'tpcds_bin_partitioned_parquet_3000'|awk '{print$1,$2}'))
# pid=${pid_time[0]}
# etime=${pid_time[1]}
# echo $pid,$etime
# hour=$(echo $etime|awk -F[:] '{print$1}')
# echo $hour
while :; do
    pid_time=($(ps -eo pid,etime,cmd|grep -v 'grep'|grep "/q.*\.sql"|awk '{print$1,$2,$NF}'))
    pid=${pid_time[0]}
    etime=${pid_time[1]}
    query=${pid_time[2]}
    hour=$(echo $etime|awk -F[:] '{print$3}')
    if [ $hour ]; then
        kill -9 $pid
        echo "kill: $etime $query"
    elif [ ! ${pid} ];then
        break
    fi
    sleep 3600
done
