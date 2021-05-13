import sys
from utils.node import *
from core.hive import *
from core.spark import *
from utils.config_utils import *
from core.spark_sql_perf import *
from core.tpcds_kit import *
from core.oap import *
from core.pat import *



def usage():
    print("Usage: python ./cluster/BBonHoS.py [action] [command] [-pat]/n")
    print("   Action option includes: spark, hadoop, autorun_all /n")
    #print("           update_and_run means just replacing configurations and trigger a run /n")
    #print("           deploy_and_run means remove all and redeploy a new run /n")
    #print("           undeploy means remove all components based on the specified configuration /n")
    exit(1)

def enable_ssh_authentication(custom_conf):
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(custom_conf)
    quick_clean_ssh_keys(slaves)
    setup_ssh_keys(master, slaves)

'''
def deploy_compiled_spark(custom_conf):
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(custom_conf)
    deploy_compiled_spark_internal(custom_conf, master, slaves, beaver_env)
'''

def spark_precompile(custom_conf):
    beaver_env = get_merged_env(custom_conf)
    spark_precompile_pkg_download(beaver_env.get("SPARK_SOURCE_PATH"))
    spark_precompile_set_proxy(beaver_env.get("SPARK_SOURCE_PATH"))

def deploy_spark_with_hive(custom_conf):
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(custom_conf)
    #deploy_spark_oap_style(custom_conf, master, slaves, beaver_env)
    deploy_hive(custom_conf, master, beaver_env)

def deploy_TPCDS(custom_conf):
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(custom_conf)
    deploy_tpcds_kit(custom_conf, master, slaves)
    deploy_spark_sql_perf(custom_conf, master)

def run_tpcds(custom_conf):
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(custom_conf)
    run_spark_sql_perf(master, custom_conf, beaver_env)

def init_all(custom_conf):
    subprocess.check_call("mkdir -p " + os.path.join(util.project_path, "package/tmp/"), shell=True)
    subprocess.check_call("mkdir -p " + os.path.join(util.project_path, "package/build/"), shell=True)
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(custom_conf)
    #enable_ssh_authentication(custom_conf)
    spark_compile_prepare(beaver_env)
    oap_compile_prepare(beaver_env)

def deploy_all(custom_conf):
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(custom_conf)
    deploy_hadoop(custom_conf, master, slaves, beaver_env)
    deploy_spark_with_hive(custom_conf)
    deploy_oap([master], conf_path, beaver_env)
    start_all(custom_conf)
    deploy_tpcds_kit(conf_path, master, slaves)
    deploy_spark_sql_perf(conf_path, master)
    deploy_pat(conf_path, master)

def update_and_run(custom_conf):
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(custom_conf)
    cmd = "pull origin master"
    oap_git_run(beaver_env, cmd)
    oap_compile(beaver_env)
    oap_jar_dist([master], beaver_env)
    run_tpcds_script(master, beaver_env)

def build_and_deploy_all(custom_conf):
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(custom_conf)
    spark_compile(conf_path, beaver_env)
    deploy_all(custom_conf)

def undeploy_all(custom_conf, beaver_env):
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(custom_conf)
    undeploy_hadoop(master, slaves, custom_conf, beaver_env)
    undeploy_hive(master)
    #clean_oap_spark(master, beaver_env)
    undeploy_tpcds_kit(slaves)
    undeploy_spark_sql_perf(master)
    clean_oap(master, beaver_env)
    clean_pat(master)

def start_all(custom_conf):
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(custom_conf)
    start_hadoop_service(master, slaves, beaver_env)
    start_hive_service(master, beaver_env)
    start_spark_history_server(master, beaver_env)


def stop_all(custom_conf):
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    stop_spark_history_server(master)
    stop_hive_service(master)
    stop_hadoop_service(master, slaves)

if __name__ == '__main__':
    action_list = [];
    action_list.append("hadoop")
    action_list.append("spark")
    action_list.append("hive")
    action_list.append("tpcds")
    action_list.append("oap")
    action_list.append("oapperf")
    action_list.append("sshnopass")
    action_list.append("deployall")
    action_list.append("startall")
    action_list.append("stopall")
    action_list.append("undeployall")
    action_list.append("installmysql")


    args = sys.argv
    if len(args) < 2:
        usage()
    #action = args[1]
    conf_path = os.path.join(project_path, 'repo/sparkoap')
    cluster_file = get_cluster_file(conf_path)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(conf_path)

    use_pat = False
    if "--conf_path" in args:
        if os.path.exists(args[args.index("--conf_path") + 1]):
            conf_path = args[args.index("--conf_path") + 1]
        else:
            print "Invalid directory"
            raise IOError
        args.remove("--conf_path")
        args.remove(args[args.index("--conf_path") + 1])

    if "hadoop" in args:
        cmd_list = []
        for cmd_c in range(args.index("hadoop") + 1, len(args)):
            if args[cmd_c] in action_list:
                break
            else:
                cmd_list.append(args[cmd_c])
        if "--deploy" in cmd_list:
            deploy_hadoop(conf_path, master, slaves, beaver_env)
        if "--updateconfigfiles" in cmd_list:
            update_copy_hadoop_conf(conf_path, master, slaves, beaver_env)
        if "--start" in cmd_list:
            start_hadoop_service(master, slaves, beaver_env)
        if "--stop" in cmd_list:
            stop_hadoop_service(master, slaves)
        if "--undeploy" in cmd_list:
            undeploy_hadoop(master, slaves, conf_path, beaver_env)

    if "spark" in args:
        cmd_list = []
        for cmd_c in range(args.index("spark") + 1, len(args)):
            if args[cmd_c] in action_list:
                break
            else:
                cmd_list.append(args[cmd_c])
        if '--init' in cmd_list:
            spark_init(beaver_env)
        if '--precompile' in cmd_list:
            spark_precompile(conf_path)
        if '--git' in cmd_list:
            git_cmd = ""
            for git_c in range(cmd_list.index("--git") + 1, len(cmd_list)):
                git_cmd = git_cmd + " " + cmd_list[git_c]
            spark_git_run(beaver_env, git_cmd)
        '''
        if '--compile' in cmd_list:
            spark_compile(conf_path, beaver_env)
        '''
        if '--deploy' in cmd_list:
            #deploy_spark_oap_style(conf_path, master, slaves, beaver_env)\
            deploy_spark(conf_path, master, slaves, beaver_env)
        if '--updateconfigfiles' in cmd_list:
            update_copy_spark_conf(master, slaves, conf_path, beaver_env)
            #update_copy_spark_conf_oap(master, slaves, conf_path, beaver_env)
        if '--undeploy' in cmd_list:
            undeploy_spark(master)

    if "hive" in args:
        cmd_list = []
        for cmd_c in range(args.index("hive") + 1, len(args)):
            if args[cmd_c] in action_list:
                break
            else:
                cmd_list.append(args[cmd_c])
        if "--deploy" in cmd_list:
            deploy_hive(conf_path, master, beaver_env)
        if "--start" in cmd_list:
            hive_start_metastore(master, beaver_env.get("HIVE_HOME"))
        if "--stop" in cmd_list:
            hive_stop_metastore(master)
        if "--undeploy" in cmd_list:
            undeploy_hive(master)

    if "oap" in args:
        cmd_list = []
        for cmd_c in range(args.index("oap") + 1, len(args)):
            if args[cmd_c] in action_list:
                break
            else:
                cmd_list.append(args[cmd_c])
        if "--init" in cmd_list:
            oap_init(beaver_env)
        if '--git' in cmd_list:
            git_cmd = ""
            for git_c in range(cmd_list.index("--git") + 1, len(cmd_list)):
                git_cmd = git_cmd + " " + cmd_list[git_c]
            oap_git_run(beaver_env, git_cmd)
        if "--compile" in cmd_list:
            oap_compile(beaver_env)
        if "--deploy" in cmd_list:
            deploy_oap([master], beaver_env)
        if "--deploy_oapperf" in cmd_list:
            deploy_oap_internal(conf_path, master, slaves, beaver_env)
        if "--undeploy" in cmd_list:
            clean_oap(master, beaver_env)

    if "oapperf" in args:
        cmd_list = []
        for cmd_c in range(args.index("oapperf") + 1, len(args)):
            if args[cmd_c] in action_list:
                break
            else:
                cmd_list.append(args[cmd_c])
        if "--init" in cmd_list:
            oap_perf_init(beaver_env)
        if '--git' in cmd_list:
            git_cmd = ""
            for git_c in range(cmd_list.index("--git") + 1, len(cmd_list)):
                git_cmd = git_cmd + " " + cmd_list[git_c]
            oap_perf_git_run(beaver_env, git_cmd)
        if "--compile" in cmd_list:
            oap_perf_compile(beaver_env)
        if "--deploy" in cmd_list:
            deploy_oap_perf(conf_path, master, slaves, beaver_env)
        if "--deploy_oapperf" in cmd_list:
            deploy_oap_perf(conf_path, master, slaves, beaver_env)
        if "--undeploy" in cmd_list:
            clean_oap_perf(master, beaver_env)

    if "tpcds" in args:
        cmd_list = []
        for cmd_c in range(args.index("tpcds") + 1, len(args)):
            if args[cmd_c] in action_list:
                break
            else:
                cmd_list.append(args[cmd_c])
        if "--deploy" in cmd_list:
            deploy_TPCDS(conf_path)
            copy_tpcds_test_script(master, conf_path, beaver_env)
        if "--deploy_script" in cmd_list:
            copy_tpcds_test_script(master,conf_path,  beaver_env)
        if "--run_script" in cmd_list:
            run_tpcds_script(master, beaver_env)
        if "--run" in cmd_list:
            run_spark_sql_perf(master, conf_path, beaver_env)
        if "--undeploy" in cmd_list:
            undeploy_tpcds_kit(slaves)
            undeploy_spark_sql_perf(master)
        if "--download" in cmd_list:
            tpcds_result_copy(master)

    if "pat" in args:
        cmd_list = []
        for cmd_c in range(args.index("pat") + 1, len(args)):
            if args[cmd_c] in action_list:
                break
            else:
                cmd_list.append(args[cmd_c])
        if "--deploy" in cmd_list:
            deploy_pat(conf_path, master)
        if "--updateconfig" in cmd_list:
            pat_config_dist(master, slaves, conf_path,  beaver_env)
        if "--undeploy" in cmd_list:
            clean_pat(master)

    if "test" in args:
        #oap_perf_set_build_script(beaver_env)
        #copy_oap_perf_test_script(master, beaver_env)
        #oap_perf_jar_dist([master], beaver_env)
        #copy_oap_perf_conf(master,conf_path,beaver_env)
        #oap_perf_link_tpcds_tools(master, beaver_env)
        #oap_perf_run_once(master, beaver_env)
        populate_pat_conf(conf_path)

    if "init_all" in args:
        init_all(conf_path)

    if "sshnopass" in args:
        enable_ssh_authentication(conf_path)

    if "build_and_deploy_all" in args:
        build_and_deploy_all(conf_path)

    if "deploy_all" in args:
        deploy_all(conf_path)

    if "start_all" in args:
        start_all(conf_path)

    if "update_and_run" in args:
        update_and_run(conf_path)

    if "stop_all" in args:
        stop_all(conf_path)

    if "undeploy_all" in args:
        undeploy_all(conf_path, beaver_env)

    if "installmysql" in args:
        #remove_mysql(master)
        deploy_mysql_without_localftp(master, conf_path)

    if "installmaven" in args:
        deploy_local_maven(beaver_env)

    #else:
        #usage()
