#!/bin/bash
set -x
kudu_root=/root/workspace/kudu
bin_path=${kudu_root}/build/release/bin
count=${1:-data}
#op_count=100000000
op_count=1000
# shards default alien to CPU core number
#block_size=10000000
#block_size=490000
block_size=18750
requestdistribution="sequential"
id=${3}
#debug="--unlock_experimental_flags=true --nvm_cache_debug=true"
debug=""
encryption_disable="--rpc_authentication=disabled --rpc-encryption=disabled"
cache_cfg=""
other_cfg="$encryption_disable $debug"
service_num_cfg="--rpc_service_queue_length=100"
common_cfg="$service_num_cfg"
#stay consistent with default value (50) of rpc_service_queue_length
thread_number=100

metrics_str=""
#metrics_str="?metrics=block_cache_misses,block_cache_inserts,block_cache_lookups,block_cache_misses_caching,block_cache_hits,block_cache_hits_caching,block_cache_usage"

dir_prefix=/root/workspace/xc
master_dir_prefix=${dir_prefix}/${count}/${id}/master
master_wal_dir=${master_dir_prefix}/wal
master_data_dir=${master_dir_prefix}/data
master_log_dir=${master_dir_prefix}/log

tserver_dir_prefix=${dir_prefix}/${count}/${id}/tserver
tserver_wal_dir=${tserver_dir_prefix}/wal
tserver_data_dir=${tserver_dir_prefix}/data
tserver_log_dir=${tserver_dir_prefix}/log

ycsb_home=/root/workspace/ycsb-0.12.0

function ntpd_sync(){
  systemctl start ntpd
  systemctl enable ntpd
  systemctl restart ntpd
}

function start_tmaster(){
  mkdir -p ${master_dir_prefix}
  mkdir -p ${master_log_dir}
  ${bin_path}/kudu-master --fs_wal_dir=${master_wal_dir} --fs_data_dirs=${master_data_dir} --log_dir=${master_log_dir} ${common_cfg} &
}

function stop_tmaster(){
  # TODO: A better way
  ps -ef | grep kudu-master | grep -v grep  | tr -s ' ' | cut -d ' ' -f 2 | xargs kill -9
}

function start_tserver(){
  mkdir -p ${tserver_dir_prefix}
  mkdir -p ${tserver_log_dir}
  ${bin_path}/kudu-tserver --fs_wal_dir=${tserver_wal_dir} --fs_data_dirs=${tserver_data_dir} --log_dir=${tserver_log_dir} ${cache_cfg} ${other_cfg} ${common_cfg} --scanner_default_batch_size_bytes=16384 & 
}

function stop_tserver(){
  # TODO: A better way
  ps -ef | grep kudu-tserver | grep -v grep  | tr -s ' ' | cut -d ' ' -f 2 | xargs kill -9
}

function update_conf(){
	sed -r -i "s/(recordcount=).*/\1${count}/" ${ycsb_home}/workloads/workload-xc 
	sed -r -i "s/(operationcount=).*/\1${op_count}/" ${ycsb_home}/workloads/workload-xc
	sed -r -i "s/(requestdistribution=).*/\1${requestdistribution}/" ${ycsb_home}/workloads/workload-xc
}

function update_read_conf(){
	sed -r -i "s/(recordcount=).*/\1${count}/" ${ycsb_home}/workloads/workload-xc
	op_count=`expr $count / $thread_number`
	sed -r -i "s/(operationcount=).*/\1${op_count}/" ${ycsb_home}/workloads/workload-xc
}

function load_data(){
  ${ycsb_home}/bin/ycsb load kudu -threads ${thread_number} -P ${ycsb_home}/workloads/workload-xc -p kudu_table_num_replicas=1
}

function run_benchmark(){
  ${ycsb_home}/bin/ycsb run kudu  -threads ${thread_number} -P ${ycsb_home}/workloads/workload-xc -p kudu_table_num_replicas=1
}

function dump_metrics(){
  unset http_proxy && curl -s "http://10.0.2.202:8050/metrics${metrics_str}"
} 

function ensure_service_up(){
	master=`ps -ef | grep kudu | grep -v grep | grep master`
	tserver=`ps -ef | grep kudu | grep -v grep | grep tserver`
	attempts=1

	while [ $attempts -le 10 ]; do 
		if [ `echo $tserver | wc -l` -le 0 ]; then
			echo "Starting TServer..."
			start_tserver
			sleep 10
		fi	

		if [ `echo $master | wc -l` -le 0 ]; then
			echo "Starting Master..."
			start_tmaster
			sleep 10
		fi
		((attempts++))
	done
}


case "$2" in
	"dram")
		cache_cfg="--block_cache_capacity_mb=${block_size}"
		;;
	"persistent_disk")
		cache_cfg="--unlock_experimental_flags=true --block_cache_type=NVM --nvm_cache_persistent=true --block_cache_capacity_mb=${block_size} --nvm_cache_path=/mnt/DP_disk2/kuduTest"
		;;
	"persistent_nvm")
		cache_cfg="--unlock_experimental_flags=true --block_cache_type=NVM --nvm_cache_persistent=true --block_cache_capacity_mb=${block_size} --nvm_cache_path=/mnt/pmem8p1"
		;;
	"volatile_disk")
		cache_cfg="--unlock_experimental_flags=true --block_cache_type=NVM --block_cache_capacity_mb=${block_size} --nvm_cache_persistent=false --nvm_cache_path=/mnt/DP_disk2/kuduTest"
		;;
	"volatile_nvm")
		cache_cfg="--unlock_experimental_flags=true --block_cache_type=NVM --nvm_cache_persistent=false --block_cache_capacity_mb=${block_size} --nvm_cache_path=/mnt/pmem8p1"
		;;
	*)
		echo "usage: sh benchmark [count in KB] [dram|persistent_disk|volatile_disk|volatile_nvm] [id] [gen data]"
		exit 1
esac

echo "Stop tmaster"
stop_tmaster

echo "Stop tserver"
stop_tserver

echo "Clean up OS cache"
echo 3 > /proc/sys/vm/drop_caches && swapoff -a && swapon -a

ntpd_sync

#clean up disk
rm -rf /mnt/pmem8p1/*
rm -rf /mnt/DP_disk2/kuduTest/*

start_tmaster
start_tserver

ensure_service_up


update_conf

if [ "$4" = true ]
then
  load_data
fi

dump_metrics
sleep 10

echo "Load Block Cache"
echo 3 > /proc/sys/vm/drop_caches && swapoff -a && swapon -a
run_benchmark
dump_metrics
sleep 10

echo 3 > /proc/sys/vm/drop_caches && swapoff -a && swapon -a
run_benchmark
dump_metrics

free -g
