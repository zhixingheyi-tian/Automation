#!/usr/bin/python

from cluster.HiveOnSpark import *
from core.bigbench import *


def deploy(custom_conf):
    print (colors.LIGHT_BLUE + "Deploy BigBench" + colors.ENDC)
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(custom_conf)
    undeploy_hive_on_spark(custom_conf, beaver_env)
    deploy_hive_on_spark(custom_conf)
    start_hive_on_spark(custom_conf)


def usage():
    print("Usage: pruningtest/deployHoS.py [path/to/conf]/n")
    exit(1)


if __name__ == '__main__':
    args = sys.argv
    if len(args) < 1:
        usage()
    conf_path = os.path.abspath(args[1])
    deploy(conf_path)
