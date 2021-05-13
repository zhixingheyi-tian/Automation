#!/usr/bin/python

import sys
from utils.node import *
from core.presto import *

def deploy_and_run(custom_conf):
    print (colors.LIGHT_BLUE + "Deploy Presto" + colors.ENDC)
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(custom_conf)
    undeploy_presto(slaves)
    deploy_presto(custom_conf, master, slaves, beaver_env)
    start_presto_service(slaves)
def update_and_run(custom_conf):
    print (colors.LIGHT_BLUE + "Replace Presto" + colors.ENDC)
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(custom_conf)
    stop_presto_service(slaves)
    update_copy_presto_conf(custom_conf, master, slaves)
    start_presto_service(slaves)

def undeploy(custom_conf):
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    undeploy_presto(slaves)

def usage():
    print("Usage: cluster/Presto.py [action] [path/to/conf] [-pat]/n")
    print("   Action option includes: deploy_and_run, undeploy /n")
    print("           deploy_and_run means remove all and redeploy a new run /n")
    print("           undeploy means remove all components based on the specified configuration /n")
    exit(1)

if __name__ == '__main__':
    args = sys.argv
    if len(args) < 2:
        usage()
    action = args[1]
    conf_path = os.path.abspath(args[2])
    if action == "deploy_and_run":
        deploy_and_run(conf_path)
    elif action == "update_and_run":
        update_and_run(conf_path)
    elif action == "undeploy":
        undeploy(conf_path)
    else:
        usage()
