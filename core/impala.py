#!/usr/bin/python

from core.jdk import *
from utils.node import *

def deploy_impala(beaver_env,slaves):
    deploy_impala_internal(beaver_env, slaves)
    stop_impala_service(slaves)

def undeploy_impala(slaves):
    stop_impala_service(slaves)
    clean_impala(slaves)

def stop_impala_service(slaves):
    print (colors.LIGHT_BLUE + "Stop impala related services, it may take a while..." + colors.ENDC)
    for node in slaves:
        ssh_execute(node, "source $IMPALA_HOME/bin/impala-config.sh &&"
                          "$IMPALA_HOME/bin/start-impala-cluster.py --kill")

def clean_impala(slaves):
    for node in slaves:
        ssh_execute(node, "rm -rf /opt/Beaver/impala")
        ssh_execute(node, "source ~/.bashrc")

def deploy_impala_internal(beaver_env, slaves):
    clean_impala(slaves)

    copy_packages(slaves, "impala", beaver_env.get("IMPALA_VERSION"))
    print (colors.LIGHT_BLUE + "Copy hadoop conf to impala" + colors.ENDC)
    cmd = "cd $IMPALA_HOME && yes | cp -f $HADOOP_HOME/etc/hadoop/core-site.xml fe/src/test/resources"
    for node in slaves:
        ssh_execute(node, cmd)
    
    master = get_master_node(slaves)   
    cmd = "cd $IMPALA_HOME && yes | cp -f $HIVE_HOME/conf/hive-site.xml fe/src/test/resources"
    ssh_execute(node, master)

def start_impala_service(slaves):
    print (colors.LIGHT_BLUE + "Starting Impala..." + colors.ENDC)
    master = get_master_node(slaves)
    impalad_args = "--impalad_args " + "\"-state_store_host=" + master.ip \
                  + " -catalog_service_host=" + master.ip + "\"";

    ssh_execute(master, "source $IMPALA_HOME/bin/impala-config.sh &&"
                          "$IMPALA_HOME/bin/start-impala-cluster.py -s 1 " + impalad_args + " &")

    for node in slaves:
        ssh_execute(node, "source $IMPALA_HOME/bin/impala-config.sh &&"
                          "$IMPALA_HOME/bin/start-impala-cluster.py -r -s 1 " + impalad_args + " &")


def run_impala_tpcds(beaver_env, slaves):
    print (colors.YELLOW + "Deploy TPCDS for impala" + colors.ENDC)
    copy_packages(slaves, "impala-tpcds-kit", beaver_env.get("IMPALA_TPCDS_VERSION"))
    for node in slaves:
        ssh_execute(node, "cd $IMPALA_TPCDS_HOME && "
                          "cd tpcds-kit/tools && "
                          "make OS=LINUX && "
                          "cat $HADOOP_HOME/etc/hadoop/slaves > $IMPALA_TPCDS_HOME/dn.txt")

    master = get_master_node(slaves)
    ssh_execute(master, "cat $HADOOP_HOME/etc/hadoop/slaves > $IMPALA_TPCDS_HOME/dn.txt")
    ssh_execute(master, "export IMPALA_TPCDS_SCALE=" + beaver_env.get("IMPALA_TPCDS_SCALE") +
                " && cd $IMPALA_TPCDS_HOME && "
                "./set-nodenum.sh && "
                "./hdfs-mkdirs.sh && "
                "./gen-dims.sh && ./run-gen-facts.sh")
    ssh_execute(master, "source $IMPALA_HOME/bin/impala-config.sh && "
                "cd $IMPALA_TPCDS_HOME &&"
                "./impala-create-external-tables.sh && "
                "./impala-load-dims.sh && "
                "./impala-load-facts.sh &&"
                "./impala-compute-stats.sh && "
                "sh ./run_query.sh")

