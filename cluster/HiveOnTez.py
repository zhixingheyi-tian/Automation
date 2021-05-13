#!/usr/bin/python

import sys
from utils.node import *
from utils.util import *
from core.hadoop import *
from core.hive import *
from core.tez import *
from core.spark import *

def deploy_hive_on_tez(custom_conf):
    # Deploy Hadoop
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(custom_conf)
    deploy_hadoop(custom_conf, master, slaves, beaver_env)

    # Deploy Hive
    deploy_hive(custom_conf, master, beaver_env)
    # Deploy Spark
    deploy_spark(custom_conf, master, slaves, beaver_env)
    # Deploy Tez
    deploy_tez_internal(custom_conf, master, beaver_env)

def undeploy_hive_on_tez(custom_conf, beaver_env):
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    undeploy_hadoop(master, slaves, custom_conf, beaver_env)
    undeploy_hive(master)
    undeploy_tez(master)
    undeploy_spark(master)

def populate_hive_on_tez_conf(custom_conf):
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(custom_conf)
    deploy_tez_internal(custom_conf, master, beaver_env)
    update_copy_hadoop_conf(custom_conf, master, slaves, beaver_env)
    update_copy_hive_conf(custom_conf, master, beaver_env)
    update_copy_spark_conf(master, slaves, custom_conf, beaver_env)
    copy_tez_conf_to_hadoop(custom_conf,[master], beaver_env)

def start_hive_on_tez(custom_conf):
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(custom_conf)
    start_hadoop_service(master, slaves, beaver_env)
    copy_tez_package_to_hadoop(master)
    start_hive_service(master, beaver_env)
    start_spark_history_server(master, beaver_env)

def stop_hive_on_tez(custom_conf):
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    stop_spark_history_server(master)
    stop_hive_service(master)
    stop_hadoop_service(master, slaves)

def restart_hive_on_tez(custom_conf):
    stop_hive_on_tez(custom_conf)
    start_hive_on_tez(custom_conf)
