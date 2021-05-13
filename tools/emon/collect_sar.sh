#!/bin/bash


export HISTIGNORE='*sudo -S*'
if [ -z "`/usr/sbin/lsmod |grep sepint4_1`" ]; then
  #pushd /mnt/workloads/sep_private_5_4_linux_579171/1/sep_private_5.4_linux_579171/sepdk/src
  pushd /home/emon/sep_private_5.9_linux_05060205ddfd359/sepdk/src
  #remove emon driver
  echo "welcome1" | sudo -S -k ./rmmod-sep
  popd
fi

seq=`date +%m%d%H%M`
#pushd /mnt/workloads/sep_private_5_4_linux_579171/1/sep_private_5.4_linux_579171/sepdk/src
pushd /home/emon/sep_private_5.9_linux_05060205ddfd359/sepdk/src
echo "Installing SEP driver..."
echo "welcome1" | sudo -S -k ./insmod-sep -g wheel
popd

EMON_HOME=/home/emon/sep_private_5.9_linux_05060205ddfd359/bin64
LOG_HOME=/home/emon/sar_data/$seq
mkdir -p $LOG_HOME
source /home/emon/sep_private_5.9_linux_05060205ddfd359/sep_vars.sh
#echo "SAR data collection Start: `date`"
#sar -A -o $LOG_HOME/$seq.sar.raw 2 60000 > $LOG_HOME/$seq.collect_sar.log &
$EMON_HOME/emon -v > ${LOG_HOME}/emon-v.dat 2>&1 &
$EMON_HOME/emon -M > ${LOG_HOME}/emon-M.dat 2>&1 &
$EMON_HOME/emon -i /home/emon/sar_data/clx-2s-events.txt > $LOG_HOME/emon.dat 2>&1 &


