#!/usr/bin/python

from core.maven import *
from utils.util import *
from utils.node import *

PRESTO_TPCDS_KIT_COMPONENT = "presto_tpcds_kit"
def deploy_tpc_ds_kit_for_presto(master, beaver_env, custom_conf):
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(custom_conf)
    clean_tpc_ds_kit_for_presto(master)
    presto_tpcds_kit_version = beaver_env.get("PRESTO_TPCDS_KIT_VERSION")
    copy_packages([master], PRESTO_TPCDS_KIT_COMPONENT, presto_tpcds_kit_version)
    print (colors.LIGHT_BLUE + "presto_tpcds_kit deploy success" + colors.ENDC)


def clean_tpc_ds_kit_for_presto(master):
    print("Clean presto_tpcds_kit")
    ssh_execute(master, "rm -rf /opt/Beaver/presto_tpcds_kit*")


