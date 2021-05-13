#!/bin/bash
function service_check(){
var1=`python -c 'import test_utils;print test_utils.check_service()'`
if [ $var1 -eq 0 ]
then
	if [ -f /opt/Beaver/BB/logs/times.csv ]
	then
		while read line
		do
			echo $line|cut -d "|" -f 6,7,11 >>log.txt
		done </opt/Beaver/BB/logs/times.csv
	else
		echo "/opt/Beaver/BB/logs/times.csv did not exsit">>log.txt
		exit 1
	fi
else
	echo "one or more services did not start">>log.txt
	exit 1
fi
}
