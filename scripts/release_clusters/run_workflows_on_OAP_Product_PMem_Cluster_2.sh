#!/bin/bash
source /root/.bashrc
source /etc/bashrc
source /etc/profile

BEAVER_HOME=$(cd $(dirname ${BASH_SOURCE[0]})/../..;pwd)
WORKFLOWS_HOME=$BEAVER_HOME/repo/workflows
python $BEAVER_HOME/bin/run_workflows.py --plugins  oap  --workflows $WORKFLOWS_HOME/oap_release_pmem_cluster_2_performance,$WORKFLOWS_HOME/oap_release_pmem_cluster_2_performance_0.8.x,$WORKFLOWS_HOME/oap_release_pmem_cluster_2_gold
