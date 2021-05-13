#!/bin/bash
#source other scripts under optimization folder
PATDIR="/home/PAT/PAT"

function usage() {
  printf "Usage: gen_to_gen_run.sh [path_to_property]"
}

function pat_run() {
  cmd=`echo ${@:1}`
  date=$(date +%Y-%m-%d-%H-%M)
  sed -i "/^CMD_PATH:/cCMD_PATH: $cmd" $PATDIR/PAT-collecting-data/config
  cd $PATDIR/PAT-collecting-data && ./pat run baseline
  wait
  echo $L
  cd $PATDIR/PAT-post-processing && ./pat-post-process.py
  wait
  cp $PATDIR/PAT-collecting-data/results/baseline/instruments/PAT-Result.pdf $save_result/
  wait
  mv $PATDIR/PAT-collecting-data/results/baseline{,_$date}

}

function runMilestone() {
  action=$1
  optimization_name=$2
  benchmark_type=$3
  data_scales=$4
  run_times=$5
  shared=$6
  if [ "$action" = "deploy_run" ]; then
    mkdir -p $RESULT_DIR/$optimization_name/old_history_server_log
    hadoop fs -copyToLocal /spark-history-server $RESULT_DIR/$optimization_name/old_history_server_log
  fi

  ansible bigbro -m shell -a "echo 3 > /proc/sys/vm/drop_caches"
  mkdir -p $BEAVER_PATH/milestone/optimizations/work_folder/
  rm -rf $BEAVER_PATH/milestone/optimizations/work_folder/*

  if [ "X$shared" != "X"  ];then
     \cp -rf $BEAVER_PATH/milestone/optimizations/$shared/* $BEAVER_PATH/milestone/optimizations/work_folder/
  fi
  cp -rf $BEAVER_PATH/milestone/optimizations/$optimization_name/* $BEAVER_PATH/milestone/optimizations/work_folder/
  mkdir -p $BEAVER_PATH/milestone/optimizations/work_folder/output/
  \cp -rf $BEAVER_PATH/milestone/optimizations/work_folder/hadoop/ $BEAVER_PATH/milestone/optimizations/work_folder/output/

  if [ "$benchmark_type" = "bb_on_hos" ]; then
    python $BEAVER_PATH/bin/runBBonHoS.py $action $BEAVER_PATH/milestone/optimizations/work_folder no_run_benchmark
    # cp -r $BB_HOME/logs/ $RESULT_DIR/$optimization_name/1
    # sed -i "s/CLEAN_ALL,DATA_GENERATION,//g" $BB_HOME/conf/bigBench.properties
  elif [ "$benchmark_type" = "ds_on_hos" ] || [ "$benchmark_type" = "ds_on_hos_tp" ]; then
    # tpc_ds_folder=`date +%Y-%m-%d`
    python $BEAVER_PATH/bin/runTPCDSonHoS.py $action $BEAVER_PATH/milestone/optimizations/work_folder no_run_benchmark
    # mkdir -p $RESULT_DIR/$optimization_name/1
    # cp -r /opt/Beaver/result/TPC-DS/$tpc_ds_folder* $RESULT_DIR/$optimization_name/1
  fi

  for((times = 1; times <= $run_times; times++))
  do
    save_result=$RESULT_DIR/$optimization_name/$times
    mkdir -p $save_result
    ansible bigbro -m shell -a "echo 3 > /proc/sys/vm/drop_caches"
    # echo 3 > /proc/sys/vm/drop_caches
    if [ "$benchmark_type" = "bb_on_hos" ]; then
      unset SPARK_HOME
      cmd="$BB_HOME/bin/bigBench runBenchmark -f $data_scales"
      pat_run $cmd
      cp -r $BB_HOME/logs/ $save_result
    elif [ "$benchmark_type" = "ds_on_hos" ]; then
      tpc_ds_home=/opt/Beaver/TPC-DS
      tpc_ds_folder=`date +%Y-%m-%d-%H-%M-%S`
      mkdir -p $save_result/$tpc_ds_folder
      cmd="/home/workspace/run_tpcds_pat.sh"
      #cmd="perl $tpc_ds_home/runSuite.pl tpcds $data_scales > $save_result/$tpc_ds_folder/result.log"
      pat_run $cmd
      cp -r $tpc_ds_home/sample-queries-tpcds/*.log $save_result/$tpc_ds_folder
    elif [ "$benchmark_type" = "ds_on_hos_tp" ]; then
      tpc_ds_home=/opt/Beaver/TPC-DS
      tpc_ds_folder=`date +%Y-%m-%d-%H-%M-%S`
      mkdir -p $save_result/$tpc_ds_folder
      cmd="/home/workspace/run_tpcds_tp_pat.sh"
      pat_run $cmd
      cp -r $tpc_ds_home/sample-queries-tpcds/*.log $save_result/$tpc_ds_folder
    fi
  done

  mkdir -p $RESULT_DIR/$optimization_name/res/
  if [ "$benchmark_type" = "bb_on_hos" ]; then
    python $BEAVER_PATH/utils/format_bb_result.py $RESULT_DIR/$optimization_name/$run_times/logs/times.csv $RESULT_DIR/$optimization_name/res/$optimization_name.xls $run_times
    python $BEAVER_PATH/utils/format_bb_result.py compare /home/beaver_result/QA.xls $RESULT_DIR/$optimization_name/res/$optimization_name.xls
  #elif [ "$benchmark_type" = "ds_on_hos" ]; then
  #  ;
  fi
}

function undeploy(){
  optimization_name=$1
  benchmark_type=$2

  mkdir -p $RESULT_DIR/$optimization_name/old_history_server_log
  hadoop fs -copyToLocal /spark-history-server $RESULT_DIR/$optimization_name/old_history_server_log
  if [ "$benchmark_type" = "bb_on_hos" ]; then
    $BEAVER_PATH/bin/runBBonHoS.py undeploy $BEAVER_PATH/milestone/optimizations/$optimization_name
  elif [ "$benchmark_type" = "ds_on_hos" ] || [ "$benchmark_type" = "ds_on_hos_tp" ]; then
    $BEAVER_PATH/bin/runTPCDSonHoS.py undeploy $BEAVER_PATH/milestone/optimizations/$optimization_name
  fi
}

if [ "$#" -ne 1 ]; then
  usage
  exit 1
fi

timestamp=`date +%s`
date=`date +%Y%m%d`
unique_time=$date-$timestamp

BEAVER_PATH="$(cd "`dirname "$0"`"/../..; pwd)"
RESULT_DIR="/home/beaver_result/$unique_time"

export PYTHONPATH=$PYTHONPATH:$BEAVER_PATH

OLD_IFS="$IFS"
IFS=" "

if [ -d $RESULT_DIR ]; then
  mkdir -p $RESULT_DIR
fi

optimizelist=""


while IFS='' read -r line || [[ -n "$line" ]]; do
  arr=($line)
  optimization_name=${arr[0]}
  benchmark_type=${arr[1]}
  action=${arr[2]}

  optimizelist=$optimizelist,$optimization_name
  if [ "$action" = "deploy_run" ] || [ "$action" = "deploy_run_without_hadoop" ]; then
    echo "Run $benchmark_type benchmark for optimization $optimization_name at data scales ${arr[3]} GB for ${arr[4]} time(s)"
    runMilestone $action $optimization_name $benchmark_type ${arr[3]} ${arr[4]} ${arr[5]}
  elif [ "$action" = "undeploy" ]; then
    echo "Undeploy for optimization $optimization_name"
    undeploy $optimization_name $benchmark_type
  fi
done < "$1"

optimizelist=${optimizelist:1}
if [ "$benchmark_type" = "bb_on_hos" ]; then
  python $BEAVER_PATH/utils/format_bb_result.py milestone $RESULT_DIR $optimizelist
elif [ "$benchmark_type" = "ds_on_hos" ]; then
  python $BEAVER_PATH/utils/format_ds_result.py $RESULT_DIR/$optimization_name
fi
cp -rf $RESULT_DIR /opt/smbdata/

IFS="$OLD_IFS"
