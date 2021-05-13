from core.flink import *
from core.spark import *

FlINK_COMPONENT = "flink"

def usage():
    print("Usage: python bin/deploy_flink.py [action] [path_toconf] ")
    print("action option includes: compile, deploy, undeploy, start, stop help")
    print("compile: python bin/deploy_flink.py compile release-1.7.0/blink repo/conf_template")
    print("deploy,you should define the package you want to install in conf/env.conf : python bin/deploy_flink.py deploy repo/conf_template")
    print("undeploy : python bin/deploy_flink.py undeploy repo/conf_template")
    print("start flink on yarn: python bin/deploy_flink.py start")
    print("stop flink on yarn: python bin/deploy_flink.py start")
    exit(1)


if __name__ == '__main__':
    args = sys.argv
    if (len(args) < 2):
        usage()
    action = args[1]
    conf_path = args[len(args)-1]
    cluster_file = get_cluster_file(conf_path)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(conf_path)
    if action == "compile":
        checkout_cmd = args[2]
        flink_compile(conf_path, beaver_env, beaver_env.get("FLINK_GIT_REPO"), checkout_cmd)
    elif action == "deploy":
        deploy_flink(conf_path, master, slaves, beaver_env)
    elif action == "undeploy":
        stop_flink_service(master)
        clean_flink(slaves)
    elif action == "start":
        start_flink_service(master,beaver_env)
    elif action == "stop":
        stop_flink_service(master)
    elif action == "deploy_flink_cluster":
        deploy_hadoop(conf_path, master, slaves, beaver_env)
        deploy_flink(conf_path, master, slaves, beaver_env)
        start_hadoop_service(master, slaves, beaver_env)
        start_flink_service(master, beaver_env)
    else:
        usage()
