#!/bin/bash
set -x

function emon_info(){
	emon -v > $res_path/emon-v.dat
	emon -M > $res_path/emon-m.dat
}

# 10GB
#size=10000000

size=100000000
#100MB
#size=1000000
#size=100

log_id=`date +%Y-%m-%d-%H-%M-%S`
id=2018-03-07-13-32-47
res_path=/root/workspace/xc/res/$log_id
latest_path=/root/workspace/xc/res/latest

rm -rf $latest_path
mkdir -p $res_path
ln -s $res_path $latest_path

cp /root/workspace/normal_benchmark.sh $res_path

#sh benchmark.sh $size dram $id true > /dev/null 2>&1

#sh /root/workspace/normal_benchmark.sh $size volatile_nvm $id false > $res_path/normal_volatile_nvm.log 2>&1
sh /root/workspace/normal_benchmark.sh $size dram $id false > $res_path/normal_dram.log 2>&1

#sh scale_benchmark.sh $size dram $id false > $res_path/dram.log 2>&1
#sh scale_benchmark.sh $size volatile_disk $id false > $res_path/volatile_disk.log 2>&1

#sh /root/workspace/scale_benchmark.sh $size volatile_nvm $id false > $res_path/volatile_nvm.log 2>&1
#sh /root/workspace/scale_benchmark.sh $size dram $id false  > $res_path/dram.log 2>&1
#sh scale_benchmark.sh $size volatile_disk $id false > $res_path/volatile_disk.log 2>&1

#sh emon_benchmark.sh $size dram $id false > $res_path/emon_dram.log 2>&1
#sh emon_benchmark.sh $size volatile_disk $id false > $res_path/emon_volatile_disk.log 2>&1

#sh emon_benchmark.sh $size volatile_nvm $id false > $res_path/emon_volatile_nvm.log 2>&1

#sh old_benchmark.sh $size volatile_nvm $id false > $res_path/old_volatile_nvm.log 2>&1

#sh old_benchmark.sh $size dram $id false > $res_path/old_dram.log 2>&1

#sh benchmark.sh $size persistent_nvm $id false > $res_path/persistent_nvm.log 2>&1
#sh benchmark.sh $size persistent_disk $id false > $res_path/persistent_disk.log 2>&1
