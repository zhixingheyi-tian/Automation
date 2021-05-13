#!/usr/bin/python

import sys
from cluster.SparkSQLwithPlugin import *
from core.spark_sql_perf import *
from core.tpcds_kit import *
from core.pat import *
from core.oap_perf import *

def source_code_compile(custom_conf, plugins):
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(custom_conf)
    spark_sql_compile_with_plugin(custom_conf, plugins)

def gen_data(custom_conf):
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(custom_conf)
    #gen_tpcds_data(master, beaver_env)
    oap_perf_gen_data(master, beaver_env)

def run_query(custom_conf):
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(custom_conf)
    #run_tpcds_query(master, beaver_env)
    return oap_perf_run_query(custom_conf, master, beaver_env)

def deploy(custom_conf, plugins):
    print(colors.LIGHT_BLUE + "Deploy TPC-DS" + colors.ENDC)
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(custom_conf)
    undeploy_spark_sql(custom_conf, beaver_env)
    deploy_spark_sql_with_plugin(custom_conf, plugins)
    start_spark_sql(custom_conf)
    deploy_tpcds_kit(custom_conf, master, slaves)
    deploy_spark_sql_perf(custom_conf, master, test_mode="oap")
    deploy_pat(custom_conf, master)
    deploy_oap_perf(custom_conf, master, slaves, beaver_env)

def deploy_and_run(custom_conf, plugins):
    deploy(custom_conf, plugins)
    gen_data(custom_conf)
    run_query(custom_conf)

def update(custom_conf, plugins):
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(custom_conf)
    populate_spark_sql_conf_with_plugin(custom_conf, plugins)
    restart_spark_sql(custom_conf)
    # deploy_tpcds_kit(custom_conf, master, slaves)
    undeploy_spark_sql_perf(master)
    deploy_spark_sql_perf(custom_conf, master, test_mode="oap")
    deploy_oap_perf(custom_conf, master, slaves, beaver_env)

def update_and_run(custom_conf, plugins):
    update(custom_conf, plugins)

def undeploy(custom_conf, plugins):
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(custom_conf)
    undeploy_spark_sql_with_plugin(custom_conf, plugins)
    undeploy_spark_sql_perf(master)
    clean_oap_perf(master, beaver_env)
    undeploy_spark_sql_perf(master)
    undeploy_spark_sql_perf(master)
    clean_oap_perf(master, beaver_env)

def usage():
    print("Usage: benchmark/OAPPerfonSparkSQLwithOAP.sh [action] [conf_dir] [plugins] /n")
    print("   Action option includes: compile, deploy, update_and_run, gen_data, run, deploy_and_run, restart, undeploy /n")
    print("           compile       : [conf_dir] [plugins], compile spark and oap, plugins can be empty. /n")
    print("           deploy        : [conf_dir] [plugins], deploy spark cluster and related components including Hadoop/n")
    print("           update        : [conf_dir] [plugins], replace configurations and restart cluster/n")
    print("           gen_data      : [conf_dir], generate TPC-DS data/n")
    print("           run           : [conf_dir], run test /n")
    print("           deploy_and_run: [conf_dir] , deploy spark cluster and related components including Hadoop, generate TPC-DS data and run test/n")
    print("           update_and_run: [conf_dir] [plugins], just replace configurations, restart the cluster and trigger a run /n")
    print("           restart       : [conf_dir] [plugins], restart the cluster/n")
    print("           undeploy      : [conf_dir] [plugins], clean all. /n")

    exit(1)


if __name__ == '__main__':
    args = sys.argv
    if len(args) < 3:
        usage()
    action = args[1]
    conf_path = os.path.abspath(args[2])
    plugins = ""
    if len(args) == 4:
        plugins = args[3]
    if action == "update_and_run":
        update_and_run(conf_path, plugins)
    elif action == "compile":
        source_code_compile(conf_path, plugins)
    elif action == "gen_data":
        gen_data(conf_path)
    elif action == "run":
        run_query(conf_path)
    elif action == "deploy":
        deploy(conf_path, plugins)
    elif action == "deploy_and_run":
        deploy_and_run(conf_path, plugins)
    elif action == "update":
        update(conf_path, plugins)
    elif action == "undeploy":
        undeploy(conf_path, plugins)
    elif action == "restart":
        restart_spark_sql(conf_path)
    else:
        usage()
