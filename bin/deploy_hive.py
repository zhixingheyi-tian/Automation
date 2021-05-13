import sys
from cluster.SparkSQL import *

def usage():
    print("Usage: python bin/deploy_hive.py [action] [path/to/conf] ")
    print("action option includes: deploy, undeploy, help ")
    print("such as: python bin/deploy_hive.py deploy repo/conf_template ")
    exit(1)

if __name__ == '__main__':
    args = sys.argv
    if(len(args) < 2):
        usage()
    action = args[1]
    conf_path = args[len(args) - 1]
    cluster_file = get_cluster_file(conf_path)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(conf_path)

    if action == 'deploy':
        undeploy_hive(master)
        deploy_hive(conf_path, master, beaver_env)
    elif action == 'undeploy':
        undeploy_hive(master)
    elif action == "update":
        update_copy_hive_conf(conf_path, master, beaver_env)
    elif action == "help":
        usage()
    else:
        usage()