#!/usr/bin/python

from cluster.HiveOnMR import *
from core.hive_tpc_ds import *

def deploy_tpc_ds(custom_conf):
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    deploy_hive_tpc_ds(custom_conf, master)

def deploy_and_run(custom_conf, use_pat):
    print (colors.LIGHT_BLUE + "Deploy TPC-DS" + colors.ENDC)
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(custom_conf)
    update_mr_tpcds(custom_conf)
    undeploy_hive_on_mr(custom_conf, beaver_env)
    deploy_hive_on_mr(custom_conf)
    start_hive_on_mr(custom_conf)
    deploy_hive_tpc_ds(custom_conf, master)
    run_hive_tpc_ds(master, custom_conf, beaver_env, slaves, use_pat)


def update_mr_tpcds(custom_conf):
    tpcds_custom_hive_enginesettings_sql = os.path.join(custom_conf, "TPC-DS/testbench.settings")
    update_mr_tpcds_settings_sql(tpcds_custom_hive_enginesettings_sql)


def update_mr_tpcds_settings_sql(conf_file):
    with open(conf_file, 'r') as file_read:
        total_line = file_read.read()
    origin_pattern = r'.*set hive.execution.engine=.*;'
    replace_pattern = 'set hive.execution.engine=mr;'
    new_total_line = re.sub(origin_pattern, replace_pattern, total_line)
    with open(conf_file, 'w') as file_write:
        file_write.write(new_total_line)


def update_and_run(custom_conf):
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(custom_conf)
    populate_hive_on_mr_conf(custom_conf)
    restart_hive_on_mr(custom_conf)
    populate_hive_tpc_ds_conf(master, custom_conf, beaver_env)
    run_hive_tpc_ds(master, custom_conf, beaver_env)

def undeploy(custom_conf):
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(custom_conf)
    undeploy_hive_on_mr(custom_conf, beaver_env)
    undeploy_hive_tpc_ds(master)

def usage():
    print("Usage: benchmark/TPCDSonHoMR.sh [action] [path/to/conf]/n")
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
        update_and_run(conf_path)
    elif action == "deploy_and_run":
        deploy_and_run(conf_path, use_pat)
    elif action == "undeploy":
        undeploy(conf_path)
    elif action == "run":
        run_tpcds_direct(conf_path)
    else:
        usage()
