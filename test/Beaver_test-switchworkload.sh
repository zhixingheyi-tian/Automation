#!/bin/bash

set -e
source ./test_utils.sh

python ./auto_deploy.py

echo "run BB on HoS(switchworkload)">>log.txt
../benchmark/BBonHoS.py deploy_and_run /home/custom/
service_check
../benchmark/BBonHoS.py undeploy /home/custom/
echo "run BB on SparkSQL(switchworkload)">>log.txt
../benchmark/BBonSparkSQL.py deploy_and_run /home/custom1/
service_check
../benchmark/BBonSparkSQL.py undeploy /home/custom1/
../benchmark/TPCDSonHoS.py deploy_and_run /home/custom2
../benchmark/TPCDSonHoS.py undeploy /home/custom2
