#!/bin/bash
if [ $# -eq 0 ]
  then
    echo "you should enter 3 arguments like
    args1: the root directory abspath/Beaver/test/pruningtest.
    args2: hive address like hdfs://bdpe**:9000.
    args3: the data scale you want to gen, 1 means 1g."
else
    hive -f $1/createtextfile.sql
    hive -f $1/createparquet.sql
    echo "generate data,this may take a while"
    cd $1/DataGen
    mvn clean install
    mvn exec:java -Dexec.args="$2 $3"
    hive -f $1/insertinto.sql
fi

