#!/bin/bash

set -e
source ./test_utils.sh

python ./auto_deploy.py
echo "run BB on HOS with HIVE_VERSION=2.2.0(differentpatch1)">>log.txt
sed -i 's/^HIVE_VERSION=.*/HIVE_VERSION=2.2.0/' /home/custom/env.conf
../benchmark/BBonHoS.py deploy_and_run /home/custom/
service_check
../benchmark/BBonHoS.py undeploy /home/custom/
echo "run BB on HOS with HIVE_VERSION=2.2.0(differentpatch2)">>log.txt
sed -i 's/^HIVE_VERSION=.*/HIVE_VERSION=2.2.0/' /home/custom/env.conf
../benchmark/BBonHoS.py deploy_and_run /home/custom/
service_check
../benchmark/BBonHoS.py undeploy /home/custom/
