#!/bin/bash

set -e
source ./test_utils.sh

python ./auto_deploy.py
echo "run BB on HOS with conf1(replaceconf)">>log.txt
../benchmark/BBonHoS.py deploy_and_run /home/custom/
service_check
echo "run BB on HOS with conf2(replaceconf)">>log.txt
../benchmark/BBonHoS.py update_and_run /home/custom/
service_check
../benchmark/BBonHoS.py undeploy /home/custom/
