#!/bin/bash
iperf3 -s >/dev/null 2>&1
if [ -z $1 ]||[ -z $2 ];then
echo "Please input 2 variable like 'serverid' 'clientid'"
else
ssh root@$2 'iperf3 -c '$1' -i 2' > netio_result.log
cat netio_result.log
rm -rf netio_result.log
#echo "The result is placed in file netio_results.log in the current path"
fi
