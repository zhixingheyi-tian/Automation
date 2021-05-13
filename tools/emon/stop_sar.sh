#!/bin/bash

MASTER_USER=sparkuser
MASTER_IP=sr272

EMON_HOME=/home/sparkuser/sep_private_5.9_linux_05060205ddfd359/bin64
source /home/sparkuser/sep_private_5.9_linux_05060205ddfd359/sep_vars.sh

$EMON_HOME/emon -stop
pid_sar=$(/usr/sbin/pidof 'sar')
sudo kill $pid_sar
echo "stop collect data"


dir=/home/sparkuser/sar_data
data_dir=/home/sparkuser/data
for a in `ls $dir`
do
	if [ -d $a ];then
		echo $a
		log_dir=$dir/$a
		mv /home/sparkuser/hadoop-2.7.5/logs/userlogs/* $log_dir
		cp $dir/process.cmd $log_dir
		cp $dir/chart_format_icx_2s.txt $log_dir
                cp $dir/icx-2s.xml $log_dir
                cp $dir/edp.rb $log_dir
		sadf -U -- -A $log_dir/$a.sar.raw > $log_dir/$a.sar.log
		$dir/sar2csv.pl  $log_dir/$a.sar.log  $log_dir/$a.sar.csv
		$dir/csv2sum.pl  $log_dir/$a.sar.csv  $log_dir/$a.csv.summary
		b=`cat /etc/hostname`
		mv $log_dir $log_dir$b
		scp -r $log_dir$b $MASTER_USER@$MASTER_IP:$1
		mv $log_dir$b $data_dir

	fi
done


