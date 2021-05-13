#!/usr/bin/python

from cluster.HiveOnTez import *
from core.bigbench import *

spark_Phive_component="spark-Phive"
def deploy_and_run(custom_conf, use_pat):
    print (colors.LIGHT_BLUE + "Deploy BigBench" + colors.ENDC)
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(custom_conf)
    undeploy_hive_on_tez(custom_conf, beaver_env)
    deploy_hive_on_tez(custom_conf)
    start_hive_on_tez(custom_conf)
    deploy_bigbench(custom_conf)
    if use_pat:
        run_BB_PAT(master, slaves, beaver_env, custom_conf)
    else:
        run_BB(master, beaver_env)

def deploy_bigbench(custom_conf):
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    update_tez_bb(custom_conf)
    deploy_bb(custom_conf, master, spark_Phive_component)

def update_tez_bb(custom_conf):
    bb_custom_hive_engineSettings_sql = os.path.join(custom_conf, "BB/engines/hive/conf/engineSettings.sql")
    update_tez_bb_settings_sql(bb_custom_hive_engineSettings_sql)

def update_tez_bb_settings_sql(conf_file):
    with open(conf_file, 'r') as file_read:
        total_line = file_read.read()
    origin_pattern = r'set hive.execution.engine=.*;'
    replace_pattern = 'set hive.execution.engine=tez;'
    new_total_line = re.sub(origin_pattern, replace_pattern, total_line)
    with open(conf_file, 'w') as file_write:
        file_write.write(new_total_line)

def update_and_run(custom_conf, use_pat):
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(custom_conf)
    spark_Phive_version = beaver_env.get("SPARK_PHIVE_VERSION")
    populate_hive_on_tez_conf(custom_conf)
    restart_hive_on_tez(custom_conf)
    undeploy_bb(master, spark_Phive_version, spark_Phive_component)
    deploy_bigbench(custom_conf)
    ssh_execute(master, "cp -r /opt/Beaver/hadoop/share/hadoop/yarn/lib/jersey-client-1.9.jar /opt/Beaver/spark-Phive/jars")
    ssh_execute(master, "cp -r /opt/Beaver/hadoop/share/hadoop/yarn/lib/jersey-core-1.9.jar /opt/Beaver/spark-Phive/jars")
    ssh_execute(master, "mv /opt/Beaver/spark-Phive/jars/jersey-client-2.22.2.jar /opt/Beaver/spark-Phive/jars/jersey-client-2.22.2.jar.bak")
    populate_bb_conf(master, custom_conf, beaver_env)
    if use_pat:
        run_BB_PAT(master,slaves, beaver_env, custom_conf)
    else:
        run_BB(master, beaver_env)

def undeploy(custom_conf):
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(custom_conf)
    spark_Phive_version = beaver_env.get("SPARK_PHIVE_VERSION")
    undeploy_hive_on_tez(custom_conf, beaver_env)
    undeploy_bb(master, spark_Phive_version, spark_Phive_component)

def usage():
    print("Usage: benchmark/BBonHoTez.sh [action] [path/to/conf] [-pat]/n")
    print("   Action option includes: deploy_and_run, update_and_run, undeploy, run /n")
    print("           update_and_run means just replacing configurations and trigger a run /n")
    print("           deploy_and_run means remove all and redeploy a new run /n")
    print("           undeploy means remove all components based on the specified configuration /n")
    exit(1)

if __name__ == '__main__':
    args = sys.argv
    if len(args) < 3:
        usage()
    action = args[1]
    conf_path = os.path.abspath(args[2])
    use_pat = False
    if len(args) > 3:
        if args[3] == "-pat":
            use_pat = True
    if action == "update_and_run":
        update_and_run(conf_path, use_pat)
    elif action == "deploy_and_run":
        deploy_and_run(conf_path, use_pat)
    elif action == "undeploy":
        undeploy(conf_path)
    elif action == "run":
        run_BB_direct(conf_path, use_pat)
    else:
        usage()
