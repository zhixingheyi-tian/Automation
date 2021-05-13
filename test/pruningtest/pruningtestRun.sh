#!/bin/bash
pssh -h /opt/Beaver/hadoop/etc/hadoop/slaves "echo 3|sudo tee /proc/sys/vm/drop_caches"
str=""
for i in "$@";do
    column="${column}a.a$i,"
done
column=${column::-1}
resultdir='/opt/Beaver/result/pruningtest'
mkdir -p $resultdir
echo "starting pruning test,result will be put in $resultdir"
hive -e "use nested_column;select $column from parquet limit 100;" > $resultdir/"${column}"result.log 2>&1
echo "pruning $column">$resultdir/"${column}"time.log
grep "Time taken" $resultdir/"${column}"result.log >$resultdir/"${column}"time.log
echo "test over"