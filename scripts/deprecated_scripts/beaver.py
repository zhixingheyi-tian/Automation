#!/usr/bin/python

import sys
import benchmark.BBonHoS
import benchmark.BBonSparkSQL

from core.bigbench import *
from cluster.HiveOnSpark import *
from utils.util import *
from utils.node import *


current_path = os.path.dirname(os.path.abspath(__file__))
project_path = os.path.dirname(current_path)
sys.path.append(project_path)

spark_Phive_component = "spark-Phive"

def undeploy_components(custom_conf, hadoop_flg, hive_flg, spark_flg, bb_flg):
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    if hadoop_flg:
        undeploy_hadoop(master, slaves, custom_conf)
    if hive_flg:
        undeploy_hive(master)
    if spark_flg:
        undeploy_spark(master)
    if bb_flg:
        deploy_bb(custom_conf, master, spark_Phive_component)


def deploy_components(custom_conf, hadoop_flg, hive_flg, spark_flg, bb_flg):
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(custom_conf)
    if hadoop_flg:
        deploy_hadoop(custom_conf, master, slaves, beaver_env)
    if hive_flg:
        deploy_hive(custom_conf, master, beaver_env)
    if spark_flg:
        deploy_spark(custom_conf, master, slaves, beaver_env)
        copy_lib_for_spark(master, slaves, beaver_env, custom_conf, True)
    if bb_flg:
        deploy_bb(custom_conf, master, spark_Phive_component)


def update_component_conf(custom_conf, hadoop_flg, hive_flg, spark_flg, bb_flg):
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(custom_conf)
    if hadoop_flg:
        update_copy_hadoop_conf(custom_conf, master, slaves, beaver_env)
    if hive_flg:
        update_copy_hive_conf(custom_conf, master, beaver_env)
    if spark_flg:
        update_copy_spark_conf(master, slaves, custom_conf, beaver_env)
    if bb_flg:
        update_copy_bb_conf(master, custom_conf, beaver_env)


def restart_services(custom_conf, hadoop_flg, hive_flg, spark_flg):
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(custom_conf)
    if hadoop_flg:
        start_hadoop_service(master, slaves, beaver_env)
    if hive_flg:
        start_hive_service(master, beaver_env)
    if spark_flg:
        start_spark_service(master, beaver_env)


def stop_services(custom_conf, hadoop_flg, hive_flg, spark_flg):
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    if hadoop_flg:
        stop_hadoop_service(master, slaves)
    if hive_flg:
        stop_hive_service(master)
    if spark_flg:
        stop_spark_service(master)


def run_bigbench(custom_conf):
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(custom_conf)
    run_BB(master, beaver_env)


def run_BB(custom_conf, hos_flg, sparkSQL_flg, run_flg):
    if run_flg == "deploy_and_run":
        if hos_flg:
            BBonHoS.deploy_and_run(custom_conf)
        if sparkSQL_flg:
            BBonSparkSQL.deploy_and_run(custom_conf)
    if run_flg == "update_and_run":
        if hos_flg:
            BBonHoS.update_and_run(custom_conf)
        if sparkSQL_flg:
            BBonSparkSQL.update_and_run(custom_conf)


def run_BB_only(custom_conf, phases, query_list):
    if len(phases) == 0:
        usage()
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(custom_conf)
    phase_list = phases.split(',')
    result_str = ''
    gendata_flg = False
    load_flg = False
    power_flg = False
    throughput_flg = False
    benchmark_flg = False
    for phase in phase_list:
        if phase == "load":
            load_flg = True
            benchmark_flg = True
        elif phase == "power":
            power_flg = True
            benchmark_flg = True
        elif phase == "throughput":
            throughput_flg = True
            benchmark_flg = True
        elif phase == 'gendata':
            gendata_flg = True
        else:
            usage()
    if gendata_flg:
        result_str = 'DATA_GENERATION'
    if benchmark_flg:
        if gendata_flg:
            result_str += ',BENCHMARK_START'
        else:
            result_str = 'BENCHMARK_START'
    if load_flg:
        result_str += ',CLEAN_LOAD_TEST,LOAD_TEST'
    if power_flg:
        result_str += ',POWER_TEST'
    if throughput_flg:
        result_str += ',THROUGHPUT_TEST_1'
    if benchmark_flg:
        result_str += ',BENCHMARK_STOP'
    run_BB_with_template(custom_conf, result_str, query_list, master, slaves, beaver_env)
    process_BB_result_log(beaver_env)


def usage():
    print("Usage: scripts/beaver.py [component] [action] [path/to/conf] [phases_for_run_bb_only] [query_list]/n")
    print("   Component option includes: Hadoop, HiveOnSpark, HiveOnTez, SparkSQL /n")
    print("   Action option includes: deploy, undeploy, replace_conf, start, stop,"
          " deploy_and_run, update_and_run, run_bb_only /n")
    print("           deploy means just replacing configurations and trigger a run /n")
    print("           undeploy means remove all and redeploy a new run /n")
    print("   For Action run_bb_only: gendata,load,power,throughput should be set "
          "for the desired phases and query_list can be set as 1,2,3,7-10")
    exit(1)

if __name__ == '__main__':
    args = sys.argv
    if len(args) < 4:
        usage()
    component = args[1]
    actions = args[2]
    conf_path = os.path.abspath(args[3])

    hadoop_flg = False
    hive_flg = False
    spark_flg = False
    bb_flg = False
    hos_flg=False
    sparkSQL_flg=False

    if component == "hadoop":
        hadoop_flg = True
    if component == "hive":
        hive_flg = True
    if component == "spark":
        spark_flg = True
    if component == "bb":
        bb_flg = True
    if component == "hos":
        hos_flg = True
    if component == "sparksql":
        sparkSQL_flg = True

    actions_list = actions.split(',')
    for action in actions_list:
        if action == "deploy":
            deploy_components(conf_path, hadoop_flg, hive_flg, spark_flg,bb_flg)
        elif action == "undeploy":
            undeploy_components(conf_path, hadoop_flg, hive_flg, spark_flg,bb_flg)
        elif action == "replace_conf":
            update_component_conf(conf_path, hadoop_flg, hive_flg, spark_flg, bb_flg)
        elif action == "start":
            restart_services(conf_path, hadoop_flg, hive_flg, spark_flg)
        elif action == "stop":
            stop_services(conf_path, hadoop_flg, hive_flg, spark_flg)
        elif action == "deploy_and_run":
            run_BB(conf_path, hos_flg, sparkSQL_flg, action)
        elif action == "update_and_run":
            run_BB(conf_path, hos_flg, sparkSQL_flg, action)
        elif action == "run_bb_only":
            query_list = "1-30"
            if len(args) > 5:
                query_list = args[5]
            run_BB_only(conf_path, args[4], query_list)
        else:
            usage()
