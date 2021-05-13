from benchmark.TPCDSonSparkSQL import *

def usage():
    print("Usage: python bin/deploy_Spark.py [action] [path/to/conf]")
    print("action option includes: compile, deploy, undeploy, help /n")
    print("such as: python bin/deploy_Spark.py compile [spark_branchname] repo/conf_tmplate")
    print("such as: python bin/deploy_Spark.py deploy repo/sparkae")
    print("such as: python bin/deploy_Spark.py undeploy repo/sparkae")
    exit(1)


if __name__ == '__main__':
    args = sys.argv
    if len(args) < 2:
        usage()
    action = args[1]
    conf_path = args[len(args) - 1]
    cluster_file = get_cluster_file(conf_path)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(conf_path)
    if action == "compile":
        checkout_cmd = args[2]
        spark_compile(conf_path, beaver_env, beaver_env.get("SPARK_GIT_REPO"), checkout_cmd)
    elif action == "deploy":
        undeploy_spark(master)
        deploy_spark(conf_path, master, slaves, beaver_env)
    elif action == "undeploy":
        undeploy_spark(master)
    elif action == "update":
        update_copy_spark_conf(master, slaves, conf_path, beaver_env)
    elif action == "help":
        usage()
    else:
        usage()
