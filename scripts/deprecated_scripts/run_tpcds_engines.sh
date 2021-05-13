#!/bin/bash
path=$(dirname $0)
path=${path/\./$(pwd)}
SPARK_PHIVE_HOME="/opt/Beaver/spark-Phive"
#HIVE_HOME="/opt/Beaver/hhive-2.2.0.parquet.vectorization"
HIVE_HOME="/opt/Beaver/hive"
BEAVER_PATH="${path}/Beaver"
QUERY_DIR="${BEAVER_PATH}/repo/TPCDS_99/99queries"
QUERY_CONF="${BEAVER_PATH}/repo/TPCDS_99"
STATISTIC=$(date +"%Y-%m-%d-%H-%M")
backupdir="/opt/New_emerging_SQL_comparative_analysis"
log_dir="/opt/Beaver/result/logs"
mkdir -p ${log_dir}
dbname_parquet="tpcds_bin_partitioned_parquet_3000"
dbname_orc="tpcds_bin_partitioned_orc_3000"

run_queries() {
    #echo $i,$2,$queries
    #sleep 5s
    #exit 0
    if [ $1 = 'Impala' ];then
        export PATH=$PATH:$IMPALA_HOME/shell
        sp_cmd="uptime"
        cmd="${IMPALA_HOME}/shell/impala-shell -d ${dbname_parquet}"
        QUERY_DIR="${QUERY_CONF}/99queries.Impala"
        testresult="^Fetched.*row"
    elif [ $1 = 'Presto' ];then
        sp_cmd="uptime"
        cmd="sh ${PRESTO_HOME}/presto --server $(hostname):9090 --catalog hive --schema ${dbname_parquet}"
        testresult="-oi failed:"
    elif [ $1 = 'Sparksql' ];then
        unset SPARK_HOME
        sp_cmd="uptime"
        cmd="${SPARK_PHIVE_HOME}/bin/beeline -u jdbc:hive2://$(hostname):10000 -i ${QUERY_DIR}/dbname.txt"
            testresult="selected.*seconds"
        #cmd="${SPARK_PHIVE_HOME}/bin/spark-sql --database ${dbname_parquet}"
        #testresult="^Time.*taken:"

    elif [ $1 = 'Spark' ];then
        sed -i "/^set hive.execution.engine/cset hive.execution.engine=spark;" ${QUERY_CONF}/spark/testbench.settings
        cmd="${HIVE_HOME}/bin/hive -i ${QUERY_CONF}/spark/testbench.settings --database ${dbname_parquet}"
        arg="--hiveconf spark.app.name=${1}_${i}"
        testresult="^Time.*seconds"

    elif [ $1 = 'LLAP' ];then
        HIVE_HOME="/opt/Beaver/hhive-2.2.0.parquet.vectorization"
        sed -i "/^set hive.execution.engine/cset hive.execution.engine=tez;" ${QUERY_CONF}/llap/testbench.settings
        cmd="${HIVE_HOME}/bin/hive -i ${QUERY_CONF}/llap/testbench.settings --database ${dbname_orc}"
        testresult="^Time.*seconds"
    elif [ $1 = 'TEZ' ];then
        #export HIVE_VERSION=2.2.0.parquet.vectorization
        sed -i "/^set hive.execution.engine/cset hive.execution.engine=tez;" ${QUERY_CONF}/tez/testbench.settings
        arg="--hiveconf hive.session.id=${1}_${i}"
        cmd="${HIVE_HOME}/bin/hive -i ${QUERY_CONF}/tez/testbench.settings --database ${dbname_orc}"
        testresult="^Time.*seconds"
    else
        exit 0
    fi
    for t in ${queries};do
        echo "start run query:q${t}_${i}"
        start=$(date +%s)
        if [ "$arg" ];then
            $cmd ${arg}_q${t} -f ${QUERY_DIR}/q${t}.sql > ${log_dir}/q${t}_${i}.log 2>&1
        else
            $cmd -f ${QUERY_DIR}/q${t}.sql > ${log_dir}/q${t}_${i}.log 2>&1
        fi
        wait
        end=$(date +%s)
        time=$(( $end - $start ))
        failed=$(grep -io 'failed:' ${log_dir}/q${t}_${i}.log|head -n 1)
        RES=$(grep ${testresult} ${log_dir}/q${t}_${i}.log|sed 'N;s/\n/ :/')
        if [ "${failed}" ];then
            echo "q${t}_${i} $time ${failed} $RES" >> ${log_dir}/result_${i}.log
        else
            echo "q${t}_${i} $time $RES" >> ${log_dir}/result_${i}.log
        fi
        echo "stop query:q${t}_${i}"
    done
}
if [ $2 ]; then
    queries=$(echo $2|sed 's/,/ /g')
else
    #queries="6"
    # queries=$(seq 1 99)  # $(seq 99 -1 1)
    queries="1 2 6 7 8 9 10 13 15 17 19 25 26 28 31 33 34 44 45 46 47 48 49 50 51 52 53 54 55 56 57 58 59 60 61 65 66 68 69 71 73 76 83 90 97"
fi
if [ $1 ];then
    start_all=$(date +%s)
    for ((i=0;i<1;i++)); do
        run_queries $1 $i &
    done
    wait
    end_all=$(date +%s)
    total_time=$(( $end_all - $start_all ))
    echo "${end_all} - ${start_all}  = ${total_time}" > ${log_dir}/total_time.txt
else
    echo '$0 $1=[Impala,Presto,Sparksql,LLAP,TEZ,MR] $2=[32,44,55]'
fi
