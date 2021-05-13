from cluster.SparkSQL import *

def usage():
    print("Usage: python bin/deploy_hadoop.py [action] [path/to/conf] ")
    print("action option includes: deploy, undeploy, start, update, stop help")
    print("such as: python bin/deploy_hadoop.py deploy|undeploy repo/conf_template")
    print("such as: python bin/deploy_hadoop.py start|stop")
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
        undeploy_hadoop(master, slaves, conf_path, beaver_env)
        deploy_hadoop(conf_path, master, slaves, beaver_env)
    elif action == 'undeploy':
        undeploy_hadoop(master, slaves, conf_path, beaver_env)
    elif action == 'start':
        start_hadoop_service(master,slaves,beaver_env)
    elif action == 'stop' :
        stop_hadoop_service(master,slaves)
    elif action == "update":
        update_copy_spark_conf(master, slaves, conf_path, beaver_env)
    elif action == "help":
        usage()
    else:
        usage()