#!/bin/bash

SPARK_PHIVE_HOME="/opt/Beaver/spark-Phive"
queriesdir="/opt/Beaver/feiren_tpcds_queries"
STATISTIC=$(date --date="+1 day" +"%Y-%m-%d")
#Query_log="/opt/New_emerging_SQL_comparative_analysis/logs/Sparksql/$STATISTIC}"
log_dir="/opt/Beaver/feiren_tpcds_queries/logs/"
unset SPARK_HOME
#$SPARK_PHIVE_HOME/sbin/start-thriftserver.sh
#sleep 10
for t in $(seq 99);do
    start=$(date +%s)
    #$SPARK_PHIVE_HOME/bin/spark-sql --database tpcds_bin_partitioned_parquet_3000 -f ${queriesdir}/q${t}.sql > ${log_dir}/q${t}.log 2>&1
    $SPARK_PHIVE_HOME/bin/beeline -u jdbc:hive2://bdpe101:10000 -i ${queriesdir}/dbname.txt -f ${queriesdir}/q${t}.sql > ${log_dir}/q${t}.log 2>&1
    wait $! #$!表示上个子进程的进程号
     
    RES=$(grep -P '^Time.*Fetched.*row' ${log_dir}/q${t}.log|sed 'N;s/\n/ :/')
    end=$(date +%s)
    time=$(( $end - $start ))
    echo "q${t} $time $RES" >> ${log_dir}/result.log
done
mv ${log_dir}/* /opt/New_emerging_SQL_comparative_analysis/logs/Sparksql/${STATISTIC}/
