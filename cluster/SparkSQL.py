from utils.node import *
from core.hadoop import *
from core.hive import *
from core.spark import *
from core.jdk import *
from core.pmem import *
from core.redis import *

def spark_sql_source_code_compile(custom_conf):
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(custom_conf)
    spark_compile(custom_conf, beaver_env, beaver_env.get("SPARK_GIT_REPO"))

def deploy_spark_sql(custom_conf):
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



def populate_spark_sql_conf(custom_conf):
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(custom_conf)
    update_copy_hadoop_conf(custom_conf, master, slaves, beaver_env)
    update_copy_hive_conf(custom_conf, master, beaver_env)
    update_copy_spark_conf(master, slaves, custom_conf, beaver_env)


def start_spark_sql(custom_conf):
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(custom_conf)
    start_hadoop_service(master, slaves, beaver_env)
    clean_yarn_dir(master, slaves, custom_conf, beaver_env)
    start_hive_service(master, beaver_env)
    start_spark_history_server(master, beaver_env)
    exchage_pmem_mode(custom_conf, beaver_env)
    start_plasma_service(master, slaves, beaver_env)
    start_redis_service(master, beaver_env)


def stop_spark_sql(custom_conf):
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(custom_conf)
    stop_spark_thrift_server(master, beaver_env)
    stop_spark_history_server(master)
    stop_hive_service(master)
    stop_hadoop_service(master, slaves)
    stop_plasma_service(master, slaves, beaver_env)
    stop_redis_service(master, beaver_env)


def restart_spark_sql(custom_conf):
    stop_spark_sql(custom_conf)
    start_spark_sql(custom_conf)


def undeploy_spark_sql(custom_conf, beaver_env):
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    undeploy_hadoop(master, slaves, custom_conf, beaver_env)
    undeploy_hive(master)
    undeploy_spark(master)

def usage():
    print("Usage: cluster/SparkSQL.py [action] [path/to/conf]/n")
    print("   Action option includes: compile, deploy, undeploy /n")
    print("           update_and_run means just replacing configurations and trigger a run /n")
    print("           deploy_and_run means remove all and redeploy a new run /n")
    print("           undeploy means remove all components based on the specified configuration /n")
    exit(1)

if __name__ == '__main__':
    args = sys.argv
    if len(args) < 2:
        usage()
    action = args[1]
    conf_path = os.path.abspath(args[len(args) - 1])
    if action == "compile":
        checkout_cmd = args[2]
        compile_cmd = args[3]
        spark_sql_source_code_compile(conf_path, checkout_cmd)
    elif action == "deploy":
        deploy_spark_sql(conf_path)
    elif action == "start":
        start_spark_sql(conf_path)
    elif action == "update_config":
        populate_spark_sql_conf(conf_path)
    elif action == "stop":
        stop_spark_sql(conf_path)
    else:
        usage()