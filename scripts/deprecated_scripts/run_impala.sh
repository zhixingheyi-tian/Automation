#!/bin/bash
cd /opt/Beaver/impala-tpcds-kit/
source ./tpcds-env.sh
#path=$(dirname $0)
#path=${path/\./$(pwd)}
#echo $path $0
#STATISTIC=$(date +%Y%m%d%H%M%S)
QUERY_DIR="/opt/Beaver/feiren_tpcds_queries"
STATISTIC=$(date --date="+1 day" +"%Y-%m-%d")
log_dir="/opt/Beaver/feiren_tpcds_queries/logs/"
#Query_log="/opt/New_emerging_SQL_comparative_analysis/logs/Impala/${STATISTIC}/Query_log"
mkdir ${log_dir}
if [ $1 ]; then
    queries=$(echo $1|sed 's/,/ /g')
    
    #Query_log=logs_${STATISTIC}
else
    queries=$(seq 1 99)
    #run_query=$(ls ${QUERY_DIR}/*.sql|awk -F [/] '{print$NF}')
fi

#echo $queries
#exit 0

for t in ${queries}
do 
  echo "Running query q${t}"
  start=$(date +%s)
  impala-shell -d tpcds_bin_partitioned_parquet_3000 -f ${QUERY_DIR}/q${t}.sql &> ${log_dir}/q${t}.log 
  end=$(date +%s)
  time=$(( $end - $start ))
  RES=$(grep -P '^Fetched.*row' ${log_dir}/q${t}.log|sed 'N;s/\n/ :/')
  #echo ${RES}
  echo "q${t} $time $RES " >> ${log_dir}/result.log
done
echo "all queries execution are finished, please check logs for the result!"
cp ${log_dir}/result.log /opt/New_emerging_SQL_comparative_analysis/logs/Impala/${STATISTIC}/
mv ${log_dir}/* /opt/New_emerging_SQL_comparative_analysis/logs/Impala/${STATISTIC}/Query_log/
