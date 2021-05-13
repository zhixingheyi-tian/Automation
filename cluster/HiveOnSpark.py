import sys
from utils.node import *
from core.hive import *
from core.spark import *
from utils.config_utils import *

def copy_lib_for_spark(master, slaves, beaver_env, custom_conf,  hos):
    spark_version = beaver_env.get("SPARK_VERSION")
    output_conf = os.path.join(custom_conf, "output")
    core_site_file = os.path.join(output_conf, "hadoop/core-site.xml")
    defaultFS_value = get_config_value(core_site_file, "fs.defaultFS")
    spark_lib_dir = ""
    if spark_version[0:3] == "1.6" and hos:
        print("Detected spark version is 1.6")
        spark_lib_dir = "/lib"
    elif spark_version[0:3] == "2.0":
        print("Detected spark version is 2.0")
        spark_lib_dir = "/jars"
        start_hadoop_service(master, slaves, beaver_env)
        ssh_execute(master, "$HADOOP_HOME/bin/hadoop fs -mkdir /spark-2.0.0-bin-hadoop")
        ssh_execute(master, "$HADOOP_HOME/bin/hadoop fs -copyFromLocal $SPARK_HOME/jars/* /spark-2.0.0-bin-hadoop")
        stop_hadoop_service(master, slaves)
        ssh_execute(master,
                    "echo \"spark.yarn.jars " + defaultFS_value + "/spark-2.0.0-bin-hadoop/*\" >> $SPARK_HOME/conf/spark-defaults.conf")
    else:
        print("Couldn't detect spark version, HoS may can't run correctly.")
    ssh_execute(master, "cp -f " + beaver_env.get("SPARK_HOME") + spark_lib_dir + "/*" + " " + beaver_env.get("HIVE_HOME") + "/lib")


def link_spark_defaults(custom_conf):
    print("create a link file at the Hive path")
    beaver_env = get_merged_env(custom_conf)
    spark_defaults_link = os.path.join(beaver_env.get("HIVE_HOME"), "conf/spark-defaults.conf")
    spark_defaults_conf = os.path.join(beaver_env.get("SPARK_HOME"), "conf/spark-defaults.conf")
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    cmd = "rm -rf " + spark_defaults_link + ";ln -s " + spark_defaults_conf + " " + spark_defaults_link + ";"
    ssh_execute(master, cmd)


def deploy_hive_on_spark(custom_conf):
    # Deploy Hadoop
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(custom_conf)
    deploy_hadoop(custom_conf, master, slaves, beaver_env)

    # Deploy Spark
    deploy_spark(custom_conf, master, slaves, beaver_env)

    # Deploy Hive
    deploy_hive(custom_conf, master, beaver_env)
    copy_lib_for_spark(master, slaves, beaver_env, custom_conf, True)
    link_spark_defaults(custom_conf)

def populate_hive_on_spark_conf(custom_conf):
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(custom_conf)
    update_copy_hadoop_conf(custom_conf, master, slaves, beaver_env)
    update_copy_hive_conf(custom_conf, master, beaver_env)
    update_copy_spark_conf(master, slaves, custom_conf, beaver_env)


def start_hive_on_spark(custom_conf):
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(custom_conf)
    start_hadoop_service(master, slaves, beaver_env)
    start_spark_history_server(master, beaver_env)
    start_hive_service(master, beaver_env)


def stop_hive_on_spark(custom_conf):
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    stop_spark_history_server(master)
    stop_hive_service(master)
    stop_hadoop_service(master, slaves)


def restart_hive_on_spark(custom_conf):
    stop_hive_on_spark(custom_conf)
    start_hive_on_spark(custom_conf)


def undeploy_hive_on_spark(custom_conf, beaver_env):
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    undeploy_hadoop(master, slaves, custom_conf, beaver_env)
    undeploy_hive(master)
    undeploy_spark(master)


def unlink(master, component):
    softlink = "/opt/Beaver/" + component
    cmd = "unlink "+softlink
    ssh_execute(master,cmd)


def unlink_spark_shuffle_shuffle(master):
    unlink(master, "hadoop/share/hadoop/yarn/lib/spark-yarn-shuffle.jar")


def switch(custom_conf):
    cluster_file = get_cluster_file(custom_conf)
    beaver_env = get_merged_env(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    unlink(master,"hive")
    unlink(master,"spark")
    unlink_spark_shuffle_shuffle(master)
    # Deploy Spark
    deploy_spark(custom_conf, master, slaves, beaver_env)
    # Deploy Hive
    deploy_hive(custom_conf, master, beaver_env)
    copy_lib_for_spark(master, slaves, beaver_env, custom_conf, True)
    link_spark_defaults(custom_conf)
    # start Hive on Spark service
    start_hive_on_spark(custom_conf)


if __name__ == '__main__':
    args = sys.argv
    if len(args) < 2:
        exit(1)
    action = args[1]
    conf_path = os.path.abspath(args[2])
    if action == "switch":
        switch(conf_path)
