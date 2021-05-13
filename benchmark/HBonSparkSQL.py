#!/usr/bin/python

from cluster.SparkSQLwithPlugin import *
from core.spark_sql_perf import *
from core.hibench import *
from core.pat import *
from core.hibench import *
from core.emon import *

def compile_spark(custom_conf, plugins):
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(custom_conf)
    spark_sql_compile_with_plugin(custom_conf, plugins)


def gen_data(custom_conf, workload):
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(custom_conf)
    gen_hibench_data(master, custom_conf, beaver_env, workload)


def run_workload(custom_conf, workload):
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(custom_conf)
    run_emon(slaves, beaver_env)
    status = run_hibench(master, slaves, beaver_env, workload)
    stop_emon(slaves, beaver_env)
    return status


def deploy(custom_conf, plugins):
    print(colors.LIGHT_BLUE + "Deploy HiBench" + colors.ENDC)
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(custom_conf)
    undeploy_spark_sql(custom_conf, beaver_env)
    deploy_spark_sql_with_plugin(custom_conf, plugins)
    start_spark_sql(custom_conf)
    deploy_hibench(custom_conf, master)
    deploy_emon(custom_conf, slaves)


def deploy_and_run(custom_conf, plugins, workload):
    deploy(custom_conf, plugins)
    gen_data(custom_conf, workload)
    run_workload(custom_conf, workload)


def update(custom_conf, plugins):
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(custom_conf)
    populate_spark_sql_conf_with_plugin(custom_conf, plugins)
    restart_spark_sql(custom_conf)
    update_copy_hibench_conf(master, custom_conf, beaver_env)
    deploy_emon(custom_conf, slaves)


def update_and_run(custom_conf, plugins, workload):
    update(custom_conf, plugins)
    run_workload(custom_conf, workload)

def undeploy(custom_conf, plugins):
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(custom_conf)
    undeploy_spark_sql_with_plugin(custom_conf, plugins)
    undeploy_hibench(master)


def restart(custom_conf):
    stop_spark_sql(custom_conf)
    start_spark_sql(custom_conf)


def usage():
    print("Usage: benchmark/HBonSparkSQL.sh [action] [conf_dir] [plugins] [workload]/n")
    print("   Action option includes: compile, deploy, update, gen_data, run, deploy_and_run, update_and_run, restart, undeploy /n")
    print("           compile       : [conf_dir] [plugins], plugins can be empty. /n")
    print("           deploy        : [conf_dir] [plugins], deploy spark cluster and related components including Hadoop/n")
    print("           update        : [conf_dir] [plugins], just replace configurations and restart cluster/n")
    print("           gen_data      : [conf_dir] [workload], generate [workload] data/n")
    print("           run           : [conf_dir] [workload], workload] cannot be empty /n")
    print("           deploy_and_run: [conf_dir] [plugins] [workload], plugins can be empty and [workload] cannot be empty /n")
    print("           update_and_run: [conf_dir] [plugins] [workload] just replace configurations, restart the cluster and trigger a run /n")
    print("           restart       : [conf_dir] restart the cluster/n")
    print("           undeploy      : [conf_dir] [plugins], plugins can be empty. /n")
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
        workload = ""
        if len(args) == 4:
            workload = args[3]
        else:
            usage()
            exit(1)
        gen_data(conf_path, workload)
    elif action == "run":
        workload = ""
        if len(args) == 4:
            workload = args[3]
        else:
            usage()
            exit(1)
        run_workload(conf_path, workload)
    elif action == "deploy_and_run":
        plugins = ""
        workload = ""
        if len(args) == 5:
            plugins = args[3].split(",")
            workload = args[4]
        elif len(args) == 4:
            if "/" in args[3]:
                workload = args[3]
            else:
                usage()
                exit(1)
        else:
            usage()
            exit(1)
        deploy_and_run(conf_path, plugins, workload)
    elif action == "update_and_run":
        plugins = ""
        workload = ""
        if len(args) == 5:
            plugins = args[3].split(",")
            workload = args[4]
        elif len(args) == 4:
            if "/" in args[3]:
                workload = args[3]
            else:
                usage()
                exit(1)
        else:
            usage()
            exit(1)
        update_and_run(conf_path, plugins, workload)
    elif action == "undeploy":
        if len(args) == 4:
            plugins = args[3].split(",")
        else:
            plugins = ""
        undeploy(conf_path, plugins)
    elif action == "restart":
        restart(conf_path)
    else:
        usage()
