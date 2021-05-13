#!/usr/bin/python

from core.presto import *
from core.hive_tpc_ds import *
from cluster.HiveOnSpark import *
from core.presto_tpcds_kit import *

def deploy_and_run(custom_conf):
    print (colors.LIGHT_BLUE + "Deploy Presto" + colors.ENDC)
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(custom_conf)
    undeploy_presto(slaves)
    deploy_presto(custom_conf, master, slaves, beaver_env)
    stop_presto_service(slaves)
    undeploy_hive_on_spark(custom_conf, beaver_env)
    deploy_hive_on_spark(custom_conf)
    start_hive_on_spark(custom_conf)
    deploy_hive_tpc_ds(custom_conf, master)
    generate_tpc_ds_data_onhive(master, custom_conf, beaver_env)
    deploy_tpc_ds_kit_for_presto(master, beaver_env, custom_conf)
    start_presto_service(slaves)
    run_presto_tpc_ds(master, beaver_env, custom_conf)


def usage():
    print("Usage: benchmark/TPCDSonPresto.py [action] [path/to/conf]/n")
    print("   Action option includes: deploy_and_run, undeploy /n")
    print("           deploy_and_run means remove all and redeploy a new run /n")
    print("           undeploy means remove all components based on the specified configuration /n")
    exit(1)

def undeploy(custom_conf):
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(custom_conf)
    undeploy_presto(slaves)
    undeploy_hive_on_spark(custom_conf, beaver_env)
    undeploy_hive_tpc_ds(master)


if __name__ == '__main__':
    args = sys.argv
    if len(args) < 2:
        usage()
    action = args[1]
    conf_path = os.path.abspath(args[2])
    if action == "deploy_and_run":
        deploy_and_run(conf_path)
    elif action == "undeploy":
        undeploy(conf_path)
    else:
        usage()