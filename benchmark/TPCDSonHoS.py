#!/usr/bin/python

from cluster.HiveOnSpark import *
from core.hive_tpc_ds import *


def deploy_tpc_ds(custom_conf):
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    deploy_hive_tpc_ds(custom_conf, master)


def update_and_run(custom_conf):
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(custom_conf)
    populate_hive_on_spark_conf(custom_conf)
    restart_hive_on_spark(custom_conf)
    populate_hive_tpc_ds_conf(master, custom_conf, beaver_env)
    run_hive_tpc_ds(master, custom_conf, beaver_env)


def deploy_and_run(custom_conf, use_pat):
    print (colors.LIGHT_BLUE + "Deploy TPC-DS" + colors.ENDC)
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(custom_conf)
    undeploy_hive_on_spark(custom_conf, beaver_env)
    deploy_hive_on_spark(custom_conf)
    start_hive_on_spark(custom_conf)
    deploy_hive_tpc_ds(custom_conf, master)
    run_hive_tpc_ds(master, custom_conf, beaver_env, slaves, use_pat)


def undeploy(custom_conf):
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(custom_conf)
    undeploy_hive_on_spark(custom_conf, beaver_env)
    undeploy_hive_tpc_ds(master)


def usage():
    print("Usage: benchmark/TPCDSonHoS.sh [action] [path/to/conf]/n")
    print("   Action option includes: deploy_and_run, update_and_run, undeploy, run /n")
    print("           update_and_run means just replacing configurations and trigger a run /n")
    print("           deploy_and_run means remove all and redeploy a new run /n")
    print("           undeploy means remove all components based on the specified configuration /n")
    exit(1)

if __name__ == '__main__':
    args = sys.argv
    if len(args) < 3:
        usage()
    action = args[1]
    conf_path = os.path.abspath(args[2])
    use_pat = False
    if len(args) > 3:
        if args[3] == "-pat":
            use_pat = True
    if action == "update_and_run":
        update_and_run(conf_path)
    elif action == "deploy_and_run":
        deploy_and_run(conf_path, use_pat)
    elif action == "undeploy":
        undeploy(conf_path)
    elif action == "run":
        run_tpcds_direct(conf_path)
    else:
        usage()

