#!/bin/bash
#cd /opt/Beaver/presto/
#path=$(dirname $0)
#path=${path/\./$(pwd)}
#echo $path $0
#STATISTIC=$(date +%Y%m%d%H%M%S)
QUERY_DIR="/opt/Beaver/feiren_tpcds_queries"
STATISTIC=$(date --date="+1 day" +"%Y-%m-%d")
log_dir="/opt/Beaver/feiren_tpcds_queries/logs/"

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
  hive -i ${QUERY_DIR}/testbench.settings --hiveconf llap.hive=q${t} -f ${QUERY_DIR}/q${t}.sql > ${log_dir}/q${t}.log 2>&1
  end=$(date +%s)
  time=$(( $end - $start ))
  RES=$(grep -P '^Time taken:.*seconds' ${log_dir}/q${t}.log|sed 'N;s/\n/ :/')
  #echo ${RES}
  echo "q${t} $time $RES " >> ${log_dir}/result.log
done
echo "all queries execution are finished, please check logs for the result!"
mv ${log_dir}/* /opt/New_emerging_SQL_comparative_analysis/logs/LLAP/${STATISTIC}/
