#!/usr/bin/python

import sys
from cluster.SparkSQL import *
from core.bigbench import *

spark_Phive_component="spark-Phive"
def deploy_bigbench(custom_conf):
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    #beaver_env = get_merged_env(custom_conf)
    #spark_Phive_version = beaver_env.get("SPARK_PHIVE_VERSION")
    #update_spark_ml(custom_conf)
    #copy_hive_site(master,spark_Phive_version)
    deploy_bb(custom_conf, master, spark_Phive_component)


def update_and_run(custom_conf, use_pat):
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(custom_conf)
    spark_Phive_version = beaver_env.get("SPARK_PHIVE_VERSION")
    undeploy_spark(master)
    deploy_spark(custom_conf, master, slaves, beaver_env)
    populate_spark_sql_conf(custom_conf)
    restart_spark_sql(custom_conf)
    undeploy_bb(master, spark_Phive_version, spark_Phive_component)
    deploy_bigbench(custom_conf)
    populate_bb_conf(master, custom_conf, beaver_env)
    if use_pat:
        run_BB_PAT(master, slaves, beaver_env, custom_conf)
    else:
        run_BB(master, beaver_env)


def deploy_and_run(custom_conf, use_pat):
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(custom_conf)
    undeploy_spark_sql(custom_conf, beaver_env)
    deploy_spark_sql(custom_conf)
    start_spark_sql(custom_conf)
    deploy_bigbench(custom_conf)
    if use_pat:
        run_BB_PAT(master, slaves, beaver_env, custom_conf)
    else:
        run_BB(master, beaver_env)


def undeploy(custom_conf):
    beaver_env = get_merged_env(custom_conf)
    undeploy_spark_sql(custom_conf, beaver_env)


def usage():
    print("Usage: benchmark/BBonHoS.sh [action] [path/to/conf] [-pat]/n")
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
        update_and_run(conf_path, use_pat)
    elif action == "deploy_and_run":
        deploy_and_run(conf_path, use_pat)
    elif action == "undeploy":
        undeploy(conf_path)
    elif action == "run":
        run_BB_direct(conf_path, use_pat)
    else:
        usage()

