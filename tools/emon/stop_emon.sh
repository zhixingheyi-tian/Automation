#!/bin/bash

EMON_HOME={%emon.home%}/bin64
source {%emon.home%}/sep_vars.sh

$EMON_HOME/emon -stop
#pid_sar=$(/usr/sbin/pidof 'sar')
#sudo kill $pid_sar
#echo "stop collect data"


dir=/home/emon/sar_data
data_dir=/home/emon/data
for a in `ls $dir`
do
	if [ -d $a ];then
		echo $a
		log_dir=$dir/$a
		#mv /home/sparkuser/hadoop-2.7.5/logs/userlogs/* $log_dir
#		cp $dir/process.cmd $log_dir
#		cp $dir/chart_format_icx_2s.txt $log_dir
#                cp $dir/icx-2s.xml $log_dir
#                cp $dir/edp.rb $log_dir
#		sadf -U -- -A $log_dir/$a.sar.raw > $log_dir/$a.sar.log
#		$dir/sar2csv.pl  $log_dir/$a.sar.log  $log_dir/$a.sar.csv
#		$dir/csv2sum.pl  $log_dir/$a.sar.csv  $log_dir/$a.csv.summary
		b=`cat /etc/hostname`
		mv $log_dir $log_dir$b
		mv $log_dir$b $data_dir

	fi
done


