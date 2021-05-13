#!/usr/bin/python

import sys
from core.impala import *
from core.hadoop import *

def deploy_and_run(custom_conf):
    print (colors.LIGHT_BLUE + "Deploy Impala" + colors.ENDC)

    default_env = { 'IMPALA_VERSION':'2.11',
    "IMPALA_HOME":"/opt/Beaver/impala",
    "IMPALA_DEPLOY_HADOOP":"FALSE",
    "IMPALA_TPCDS_VERSION":"2.3.1",
    "IMPALA_TPCDS_HOME":"/opt/Beaver/impala-tpcds-kit",
    "IMPALA_TPCDS_SCALE":"10",
    "http_proxy":"http://child-prc.intel.com:913",
    "https_proxy": "http://child-prc.intel.com:913"
    }

    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(custom_conf)

    for key in default_env:
        if beaver_env.has_key(key) == False:
            print "[warning]"+key+"is not set."
            beaver_env[key] = default_env.get(key)

    setup_env_dist(slaves, beaver_env, "impala")

    if beaver_env.get("IMPALA_DEPLOY_HADOOP") == "TRUE":
        deploy_hadoop(custom_conf, master, slaves, beaver_env)

    for node in slaves:
        print (colors.LIGHT_BLUE + "Install lsb_release on " + node.ip + colors.ENDC)
        ssh_execute(node, "yum -y install redhat-lsb-core --disableplugin=fastestmirror")

    deploy_impala(beaver_env, slaves)
    start_impala_service(slaves)

    # run_impala_tpcds(beaver_env, slaves)

def update_and_run(custom_conf):
    print (colors.LIGHT_BLUE + "Replace Impala" + colors.ENDC)
    default_env = { 'IMPALA_VERSION':'2.11',
    "IMPALA_HOME":"/opt/Beaver/impala",
    "IMPALA_DEPLOY_HADOOP":"FALSE",
    "IMPALA_TPCDS_VERSION":"2.3.1",
    "IMPALA_TPCDS_HOME":"/opt/Beaver/impala-tpcds-kit",
    "IMPALA_TPCDS_SCALE":"10",
    "http_proxy":"http://child-prc.intel.com:913",
    "https_proxy": "http://child-prc.intel.com:913"
    }

    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(custom_conf)
    stop_impala_service(slaves)
    start_impala_service(slaves)



def undeploy(custom_conf):
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    undeploy_impala(slaves)

def usage():
    print("Usage: cluster/Impala.py [action] [path/to/conf] /n")
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
    elif action == "undeploy":
        undeploy(conf_path)
    elif action == "update_and_run":
        update_and_run(conf_path)
    else:
        usage()
