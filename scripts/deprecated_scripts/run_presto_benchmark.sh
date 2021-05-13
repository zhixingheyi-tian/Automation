#!/bin/bash
cd /opt/Beaver/presto/
#path=$(dirname $0)
#path=${path/\./$(pwd)}
#echo $path $0
#STATISTIC=$(date +%Y%m%d%H%M%S)
QUERY_DIR="/opt/Beaver/feiren_tpcds_queries"
STATISTIC=$(date --date="+1 day" +"%Y-%m-%d")
log_dir=logs
mkdir ${log_dir}

:<< BLOCK

if [ $1 ]; then
    queries=$(echo $1|sed 's/,/ /g')
else
    queries=$(seq 99)
fi

#exit 0

for t in ${queries}
do 
  echo "Running query q${t}"
  start=$(date +%s)
    
sh presto --server bdpe101:9090 --catalog hive --schema tpcds_bin_partitioned_parquet_3000 -f ${QUERY_DIR}/q${t}.sql > ${log_dir}/q${t}.log 2>&1
  end=$(date +%s)
  time=$(( $end - $start ))
  RES=$(grep -P '^Time.*Fetched.*row' ${log_dir}/q${t}.log|sed 'N;s/\n/ :/')
  #echo ${RES}
  echo "q${t}: $time $RES " >> ${log_dir}/result.log
done
echo "all queries execution are finished, please check logs for the result!"
BLOCK
sh presto-benchmark-driver --sql /opt/Beaver/feiren_tpcds_queries/sql/  --suite-config /opt/Beaver/feiren_tpcds_queries/suite.json --server bdpe101:9090 --catalog hive --runs 1 --warm 0 >> $log_dir/result.log &
wait

mv ${log_dir}/* /opt/New_emerging_SQL_comparative_analysis/logs/Presto/${STATISTIC}/
