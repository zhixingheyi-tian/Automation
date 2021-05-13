#!/bin/bash
HADOOP_HOME=$1
echo ""> TestDFSIO_results.log
hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-*-tests.jar TestDFSIO -write -nrFiles 10 -fileSize 5 -bufferSize 67108864 >/dev/null 2>&1
cat TestDFSIO_results.log
rm -rf TestDFSIO_results.log
#echo "The result is placed in file TestDFSIO_results.log in the current path"
