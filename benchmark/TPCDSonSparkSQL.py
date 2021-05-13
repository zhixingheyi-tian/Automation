#!/usr/bin/python

from cluster.SparkSQLwithPlugin import *
from core.spark_sql_perf import *
from core.tpcds_kit import *
from core.pat import *
from core.emon import *

def compile_spark(custom_conf, plugins):
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
    gen_tpc_data(master, custom_conf, beaver_env, "tpcds")

def run_query(custom_conf, iteration):
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(custom_conf)
    run_emon(slaves, beaver_env)
    status = run_tpc_query(master, slaves, beaver_env, iteration, "tpcds")
    stop_emon(slaves, beaver_env)
    return status

def run_query_with_validation(custom_conf, runType):
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(custom_conf)
    return run_tpc_query_validation(master, slaves, beaver_env, "tpcds", runType)

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
    deploy_spark_sql_perf(custom_conf, master, test_mode= plugins)
    deploy_emon(custom_conf, slaves)
    deploy_pat(custom_conf, master)

def deploy_and_run(custom_conf, plugins, iteration):
    deploy(custom_conf, plugins)
    gen_data(custom_conf)
    run_query(custom_conf, iteration)

def update(custom_conf, plugins):
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(custom_conf)
    populate_spark_sql_conf_with_plugin(custom_conf, plugins)
    restart_spark_sql(custom_conf)
    deploy_tpcds_kit(custom_conf, master, slaves)
    undeploy_spark_sql_perf(master)
    deploy_spark_sql_perf(custom_conf, master, test_mode= plugins)
    deploy_emon(custom_conf, slaves)

def update_and_run(custom_conf, plugins, iteration):
    update(custom_conf, plugins)
    

def undeploy(custom_conf, plugins):
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(custom_conf)
    undeploy_spark_sql_with_plugin(custom_conf, plugins)
    undeploy_spark_sql_perf(master)

def restart(custom_conf):
    stop_spark_sql(custom_conf)
    start_spark_sql(custom_conf)

def usage():
    print("Usage: benchmark/TPCDSonSparkSQL.sh [action] [conf_dir] [plugins] [iteration]/n")
    print("   Action option includes: compile, deploy, update, gen_data, run, deploy_and_run, update_and_run, restart, undeploy /n")
    print("           compile       : [conf_dir] [plugins], plugins can be empty. /n")
    print("           deploy        : [conf_dir] [plugins], deploy spark cluster and related components including Hadoop/n")
    print("           update        : [conf_dir] [plugins], just replace configurations and restart cluster/n")
    print("           gen_data      : [conf_dir] generate TPC-DS data/n")
    print("           run           : [conf_dir] [iteration], the default value of iteration is '1' if iteration is empty /n")
    print("           deploy_and_run: [conf_dir] [plugins] [iteration], plugins can be empty "
          "                           and the default value of iteration is '1' if iteration is empty /n")
    print("           update_and_run: [conf_dir] [plugins] [iteration] just replace configurations, restart the cluster and trigger a run /n")
    print("           restart       : [conf_dir] restart the cluster/n")
    print("           undeploy      : [conf_dir] [plugins], plugins can be empty. /n")
    print("           run_validation: [conf_dir] [runType], if runType is \"baseline\" then run queries without plugin; if runType is \"validation\", then run tpch with plugin and compare the result /n")
    exit(1)


if __name__ == '__main__':
    args = sys.argv
    if len(args) < 3:
        usage()
    action = args[1]
    conf_path = args[2]

    if action == "compile":
        if len(args) == 4:
            plugins = args[3].split(",")
        else:
            plugins = ""
        compile_spark(conf_path, plugins)
    elif action == "deploy":
        if len(args) == 4:
            plugins = args[3].split(",")
        else:
            plugins = ""
        deploy(conf_path, plugins)
    elif action == "update":
        if len(args) == 4:
            plugins = args[3].split(",")
        else:
            plugins = ""
        update(conf_path, plugins)
    elif action == "gen_data":
        gen_data(conf_path)
    elif action == "run":
        if len(args) == 4:
            iteration = args[3]
        else:
            iteration = 1
        run_query(conf_path, iteration)
    elif action == "deploy_and_run":
        if len(args) == 5:
            plugins = args[3].split(",")
            iteration = args[4]
        elif len(args) == 4:
            if args[3].isdigit():
                plugins = ""
                iteration = args[3]
            else:
                plugins = args[3].split(",")
                iteration = 1
        else:
            plugins = ""
            iteration = 1
        deploy_and_run(conf_path, plugins, iteration)
    elif action == "update_and_run":
        if len(args) == 5:
            plugins = args[3].split(",")
            iteration = args[4]
        elif len(args) == 4:
            if args[3].isdigit():
                plugins = ""
                iteration = args[3]
            else:
                plugins = args[3].split(",")
                iteration = 1
        else:
            plugins = ""
            iteration = 1
        update_and_run(conf_path, plugins, iteration)
    elif action == "undeploy":
        if len(args) == 4:
            plugins = args[3].split(",")
        else:
            plugins = ""
        undeploy(conf_path, plugins)
    elif action == "restart":
        restart(conf_path)
    elif action == "run_validation":
        if len(args) == 4:
            runType = args[3]
        else:
            runType = "validation"
        run_query_with_validation(conf_path, runType)
    else:
        usage()
