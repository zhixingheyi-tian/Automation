#!/bin/bash
source /root/.bashrc
source /etc/bashrc
source /etc/profile

BEAVER_HOME=$(cd $(dirname ${BASH_SOURCE[0]})/../..;pwd)
CONFS_HOME=$BEAVER_HOME/repo/confs
python $BEAVER_HOME/bin/run_conda_build.py --confs $CONFS_HOME/spark_oap_conda_build_conf/oap_release_conda_build_0.8.x,$CONFS_HOME/spark_oap_conda_build_conf/oap_release_conda_build
