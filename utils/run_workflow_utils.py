#!/usr/bin/python

from core.oap_perf import *
from core.hibench import *
from utils.workflow import *
from utils.analyze import *
from utils.util import *
from cluster.SparkSQL import *
import argparse
import time
import benchmark.TPCDSonSparkSQL as TPCDS
import benchmark.OAPPerfonSparkSQLwithOAP  as OAPPerf
import benchmark.TPCHonSparkSQL as TPCH
import benchmark.HBonSparkSQL as HiBench


def verfiry_baseline_comp(conf):
    beaver_env = get_merged_env(conf)
    if not beaver_env.get("BASELINE_COMP") is None and beaver_env.get("BASELINE_COMP").lower() == "true":
        return True
    else:
        return False

def verfiry_baseline_rerun(conf):
    beaver_env = get_merged_env(conf)
    if not beaver_env.get("BASELINE_RERUN") is None and beaver_env.get("BASELINE_RERUN").lower() == "true":
        return True
    else:
        return False


def get_baseline_conf(target_conf, testing_conf_list):
    for conf in testing_conf_list:
        if target_conf+"_Baseline" == conf:
            return conf


def send_mail_workflow_result(workflow, conf):
    beaver_env = get_merged_env(conf)
    result_reciever = beaver_env.get("OAP_EMAIL_RECEIVER").strip().split(",")
    subject = "OAP Nightly Report: " + os.path.basename(workflow)
    output_workflow = os.path.join(workflow, "output/output_workflow")
    result_html = os.path.join(output_workflow, "summary.html")
    sendmail(subject, result_html, result_reciever)
    if verfiry_baseline_comp(conf):
        subject = "OAP Nightly Report: " + os.path.basename(workflow) + " (Gold-deck)"
        result_html = os.path.join(output_workflow, "baseline-summary.html")
        sendmail(subject, result_html, result_reciever)


def send_mail_workflow_conf_result(conf, workflow, result_dir, isSuccess):
    result_reciever = "hao.jin@intel.com"
    if isSuccess:
        subject = "OAP Nightly Report: " + os.path.basename(workflow)
        result_html = os.path.join(result_dir, "result.html")
        sendmail(subject, result_html, [result_reciever])
    else:
        os.system("echo -e 'Hello guys," + os.path.basename(workflow) + ": " + os.path.basename(conf) +
                  " running failed, please check your test!' | mail -s \"" +
                  os.path.basename(workflow) + ":" + os.path.basename(conf) + "\" " + result_reciever)

def add_workflow_wrong_message(conf, resDir, workflow, workflow_result_dict, workflow_start_date):
    res = {}
    res["component"] = os.path.basename(os.path.dirname(conf))
    res["configuration"] = os.path.basename(os.path.dirname(resDir))
    res["status"] = "FAILED"
    res["failed_cases"] = "X"
    res["succeeded_cases"] = "X"
    res["this_total_time"] = "X"
    res["pre_total_time"] = "X"
    res["degradation_ratio"] = "X"
    res["degradation"] = "X"
    res["details"] = get_result_history_url(workflow, workflow_start_date, resDir)
    workflow_result_dict[os.path.basename(conf)] = res


def run_conf(conf, plugins, workflow, workflow_result_dict, workflow_start_date, baseline_result_dict):
    print (colors.LIGHT_BLUE + "\tRunning this configuration: " + conf + "..." + colors.ENDC)
    beaver_env = get_merged_env(conf)
    workload = get_default_workload(conf)
    iteration = beaver_env.get("DEFAULT_ITERATION")
    if workload == "tpcds":
        TPCDS.update(conf, plugins)
        status = TPCDS.run_query(conf, iteration)
        if status == 0:
            result_dir = tpc_data_collect(conf, workflow, "tpcds", workflow_result_dict, workflow_start_date, True, baseline_result_dict)
            send_mail_workflow_conf_result(conf, workflow, result_dir, True)
        else:
            tpc_data_collect(conf, workflow, "tpcds", workflow_result_dict, workflow_start_date, False, baseline_result_dict)
            send_mail_workflow_conf_result(conf, workflow, "", False)
    elif workload == "tpch":
        TPCH.update(conf, plugins)
        status = TPCH.run_query(conf, iteration)
        if status == 0:
            result_dir = tpc_data_collect(conf, workflow, "tpch", workflow_result_dict, workflow_start_date, True, baseline_result_dict)
            send_mail_workflow_conf_result(conf, workflow, result_dir, True)
        else:
            tpc_data_collect(conf, workflow, "tpch", workflow_result_dict, workflow_start_date, False, baseline_result_dict)
            send_mail_workflow_conf_result(conf, workflow, "", False)
    elif workload == "hibench":
        hibench_workload = get_default_hibench_workload(conf)
        if hibench_workload is None:
            print "Please define HiBench_workload  you want to run !"
            return
        HiBench.update(conf, plugins)
        status = HiBench.run_workload(conf,  hibench_workload)
        if hibench_workload == "micro/terasort":
            release_hdfs_space(conf, "/HiBench/Terasort/Output")
        if status == 0:
            result_dir = hibench_data_collect(conf, workflow, workflow_result_dict, workflow_start_date, True, baseline_result_dict)
            send_mail_workflow_conf_result(conf, workflow, result_dir, True)
        else:
            hibench_data_collect(conf, workflow, workflow_result_dict, workflow_start_date, False, baseline_result_dict)
            send_mail_workflow_conf_result(conf, workflow, "", False)
    elif workload == "oapperf":
        OAPPerf.update(conf, plugins)
        status = OAPPerf.run_query(conf)
        if status == 0:
            result_dir = oap_perf_data_collect(conf, workflow, workflow_result_dict, workflow_start_date, True, baseline_result_dict)
            send_mail_workflow_conf_result(conf, workflow, result_dir, True)
        else:
            oap_perf_data_collect(conf, workflow, workflow_result_dict, workflow_start_date, False, baseline_result_dict)
            send_mail_workflow_conf_result(conf, workflow, "", False)

def run_dataGen_conf(conf):
    print (colors.LIGHT_BLUE + "\tRunning this dataGen configuration: " + conf + "..." + colors.ENDC)
    workload = get_default_workload(conf)
    print (colors.LIGHT_BLUE + "\tCheck wheathe to generate " + workload + " data..." + colors.ENDC)
    if workload == "tpcds":
        TPCDS.update(conf, "")
        TPCDS.gen_data(conf)
    elif workload == "tpch":
        TPCH.update(conf, "")
        TPCH.gen_data(conf)
    elif workload == "hibench":
        hibench_workload = get_default_hibench_workload(conf)
        if hibench_workload is None:
            print "Please define HIBENCH_WORKLOAD  you want to run !"
            exit(1)
        HiBench.update(conf, "")
        HiBench.gen_data(conf,  hibench_workload)
    elif workload == "oapperf":
        OAPPerf.update(conf, "")
        OAPPerf.gen_data(conf)

def get_default_workload(conf):
    beaver_env = get_merged_env(conf)
    return beaver_env.get("DEFAULT_WORKLOAD").lower()

def get_default_hibench_workload(conf):
    beaver_env = get_merged_env(conf)
    return beaver_env.get("HIBENCH_WORKLOAD").lower()

def update_package(conf, plugins):
    print (colors.LIGHT_BLUE + "\tStart to update package for workflow and use  " + conf +  " as conf..." + colors.ENDC)
    cluster_file = get_cluster_file(conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(conf)
    deploy_spark(conf, master, slaves, beaver_env)
    update_copy_hibench_conf(master, conf, beaver_env)
    for plugin in plugins:
        if plugin == "oap":
            # Deploy OAP
            oap_compile(beaver_env, master)
        if plugin == "conda_oap":
            # Deploy Conda OAP
            deploy_conda_oap_internal(slaves, beaver_env)

def get_result_directory(conf):
    conf_name = os.path.basename(conf)
    finish_time = time.strftime("%Y-%m-%d_%H:%M:%S", time.localtime())
    commit_id = subprocess.check_output("cd " + oap_source_code_path + " && git rev-parse HEAD", shell=True).strip( '\r\n')
    return  os.path.join(conf, conf_name + "_" + finish_time + "_" + commit_id)

def oap_perf_data_collect(conf, workflow, workflow_result_dict, workflow_start_date, isSuccess, baseline_result_dict):
    beaver_env = get_merged_env(conf)
    oap_perf_home = os.path.join(beaver_env.get("OAP_HOME"), "oap-perf/" + beaver_env.get("SPARK_VERSION"))
    resDir = get_result_directory(conf)
    os.system("mkdir -p " + resDir)
    os.system("cp -r " + oap_perf_home + "/oap_perf_suite_result " + resDir)
    if isSuccess:
        process_oap_perf_result(conf, resDir, workflow, workflow_result_dict, workflow_start_date)
        if verfiry_baseline_comp(conf) and not conf.endswith("_Baseline"):
            process_oap_perf_result(conf, resDir, workflow, baseline_result_dict, workflow_start_date, True)
    else:
        add_workflow_wrong_message(conf, resDir, workflow, workflow_result_dict, workflow_start_date)
        if not conf.endswith("_Baseline"):
            add_workflow_wrong_message(conf, resDir, workflow, baseline_result_dict, workflow_start_date)
    collect_emon_data(beaver_env, resDir)
    scp_result_to_history_server(conf, resDir, workflow, workflow_start_date)
    return resDir

def hibench_data_collect(conf, workflow, workflow_result_dict, workflow_start_date, isSuccess, baseline_result_dict):
    beaver_env = get_merged_env(conf)
    hibench_home = beaver_env.get("HIBENCH_HOME")
    resDir = get_result_directory(conf)
    os.system("mkdir -p " + resDir)
    os.system("cp -r " + hibench_home + "/report " + resDir)
    if isSuccess:
        process_hibench_result(conf, resDir, workflow, workflow_result_dict, workflow_start_date)
        if verfiry_baseline_comp(conf)  and not conf.endswith("_Baseline"):
            process_hibench_result(conf, resDir, workflow, baseline_result_dict, workflow_start_date, True)
    else:
        add_workflow_wrong_message(conf, resDir, workflow, workflow_result_dict, workflow_start_date)
        if not conf.endswith("_Baseline"):
            add_workflow_wrong_message(conf, resDir, workflow, baseline_result_dict, workflow_start_date)
    collect_emon_data(beaver_env, resDir)
    scp_result_to_history_server(conf, resDir, workflow, workflow_start_date)
    return resDir

def tpc_data_collect(conf, workflow, tpc_workload, workflow_result_dict, workflow_start_date, isSuccess, baseline_result_dict):
    beaver_env = get_merged_env(conf)
    spark_version = beaver_env.get("SPARK_VERSION").strip(" ").strip("\n")
    spark_sql_perf_home = beaver_env.get("SPARK_SQL_PERF_HOME")
    resDir = get_result_directory(conf)
    mkdirs(resDir)
    os.system("cp -r " +  spark_sql_perf_home + "/" + tpc_workload + "_script/" + spark_version + "/" + tpc_workload + "/logs " + resDir)
    if isSuccess:
        process_tpc_result(conf, resDir, workflow, workflow_result_dict, workflow_start_date)
        if verfiry_baseline_comp(conf) and not conf.endswith("_Baseline"):
            process_tpc_result(conf, resDir, workflow, baseline_result_dict, workflow_start_date, True)
    else:
        add_workflow_wrong_message(conf, resDir, workflow, workflow_result_dict, workflow_start_date)
        if not conf.endswith("_Baseline"):
            add_workflow_wrong_message(conf, resDir, workflow, baseline_result_dict, workflow_start_date)
    collect_emon_data(beaver_env, resDir)
    scp_result_to_history_server(conf, resDir, workflow, workflow_start_date)
    return resDir

def collect_emon_data(beaver_env, resDir):
    if beaver_env.get("EMON_ENABLE").lower() == "true":
        print(colors.LIGHT_GREEN + "Copy emon data to  repo folder" + colors.ENDC)
        emon_store_location = os.path.join(resDir, "emon_data")
        mkdirs(emon_store_location)
        os.system("cp -r " + os.path.join(beaver_env.get("EMON_HOME"), "data") + " " + emon_store_location)
    else:
        print(colors.RED + "Not enable emon tools" + colors.ENDC)

def get_conf_root(conf):
    dir_name = os.path.basename(conf)
    pre_dir = conf
    while dir_name != "output_workflow":
        pre_dir = os.path.abspath(os.path.dirname(pre_dir) + os.path.sep + ".")
        dir_name = os.path.basename(pre_dir)
    return pre_dir


def get_testing_conf_list(root_path):
    testing_conf_list = []
    data_gen_conf_list = []
    get_conf_list(root_path, testing_conf_list, data_gen_conf_list)
    return testing_conf_list


def get_corresponding_baseline_path(conf):
    corresponding_conf_path = conf
    if verfiry_baseline_comp(conf):
        testing_conf_list = get_testing_conf_list(get_conf_root(conf))
        baseline_conf = get_baseline_conf(conf, testing_conf_list)
        corresponding_conf_path = baseline_conf
    return corresponding_conf_path


def process_hibench_result(conf, resDir, workflow, workflow_result_dict, workflow_start_date, comp_flag=False):
    origin_conf = conf
    html_name = "result.html"
    if comp_flag:
        conf = get_corresponding_baseline_path(conf)
        html_name = "baseline_result.html"
    last_path = ""
    last_result_metadata = os.path.join(conf, "last_test_info")
    if os.path.exists(last_result_metadata):
        last_path = return_last_result_metadata(last_result_metadata)
    if os.path.exists(os.path.join(last_path, "report/hibench.report")):
        analyze_hibench_result(last_path, resDir, os.path.join(resDir, html_name), workflow, workflow_start_date)
        get_hibench_summary_result(last_path, resDir, workflow, workflow_result_dict, workflow_start_date, html_name)
    else:
        analyze_hibench_result(resDir, resDir, os.path.join(resDir, html_name), workflow, workflow_start_date)
        get_hibench_summary_result(resDir, resDir, workflow, workflow_result_dict, workflow_start_date, html_name)


def process_oap_perf_result(conf, resDir, workflow, workflow_result_dict, workflow_start_date, comp_flag=False):
    origin_conf = conf
    html_name = "result.html"
    if comp_flag:
        conf = get_corresponding_baseline_path(conf)
        html_name = "baseline_result.html"
    last_path = ""
    last_result_metadata = os.path.join(conf, "last_test_info")
    if os.path.exists(last_result_metadata):
        last_path = return_last_result_metadata(last_result_metadata)
    if os.path.exists(os.path.join(last_path, "oap_perf_suite_result/testres")):
        analyze_oap_perf_result(last_path, resDir, os.path.join(resDir, html_name), workflow, workflow_start_date)
        get_oap_perf_summary_result(last_path, resDir, workflow, workflow_result_dict, workflow_start_date, html_name)
    else:
        analyze_oap_perf_result(resDir, resDir, os.path.join(resDir, html_name), workflow, workflow_start_date)
        get_oap_perf_summary_result(resDir, resDir, workflow, workflow_result_dict, workflow_start_date, html_name)


def process_tpc_result(conf, resDir, workflow, workflow_result_dict, workflow_start_date, comp_flag=False):
    origin_conf = conf
    html_name = "result.html"
    if comp_flag:
        conf = get_corresponding_baseline_path(conf)
        html_name = "baseline_result.html"
    last_path = ""
    last_result_metadata = os.path.join(conf, "last_test_info")
    if os.path.exists(last_result_metadata):
        last_path = return_last_result_metadata(last_result_metadata)
    if os.path.exists(os.path.join(last_path, "logs/final_result.csv")):
        # // gene html
        analyze_tpc_result(last_path, resDir, os.path.join(resDir, html_name), workflow, workflow_start_date)
        # gen workflow_result_dict
        get_tpc_summary_result(last_path, resDir, workflow, workflow_result_dict, workflow_start_date, html_name)
    else:
        analyze_tpc_result(resDir, resDir, os.path.join(resDir, html_name), workflow, workflow_start_date)
        get_tpc_summary_result(resDir, resDir, workflow, workflow_result_dict, workflow_start_date, html_name)



def scp_result_to_history_server(conf, resDir, workflow, workflow_start_date):
    cluster_file = get_cluster_file(conf)
    slaves = get_slaves(cluster_file)
    beaver_env = get_merged_env(conf)
    os.system("cp -r " + os.path.join(conf, "output") + " " + os.path.join(resDir, "conf"))
    hadoop_log_dir = os.path.join(resDir, "hadoop_log")
    mkdirs(hadoop_log_dir)
    for node in slaves:
        if node.role == "master":
            thrift_server_log_dir = os.path.join(resDir, "thrift_server_log")
            mkdirs(thrift_server_log_dir)
            os.system("echo 'y' | scp -r root@" + node.ip + ":" + beaver_env.get(
                "SPARK_HOME") + "/logs/*-org.apache.spark.sql.hive.thriftserver.HiveThriftServer2*.out " + thrift_server_log_dir)
            continue
        node_hadoop_log_dir = os.path.join(hadoop_log_dir, node.hostname)
        mkdirs(node_hadoop_log_dir)
        ssh_execute(node, "cd " + beaver_env.get("HADOOP_HOME") + " && scp -r " +
                    "logs/userlogs/$(ls logs/userlogs/ | awk 'END {print}') " + "root@" + node.ip + ":" + node_hadoop_log_dir)
        os.system("chmod -R 755 " + node_hadoop_log_dir)
        # keep only beaver_env.get("YARN_USERLOGS_RESERVERD_NUM") history logs.
        ssh_execute(node, "cd " + beaver_env.get("HADOOP_HOME") + "/logs/userlogs/ " +
                    "&& ls -t |awk -F '-' '{if(NR>" + beaver_env.get("YARN_USERLOGS_RESERVERD_NUM") + "){print $1}}' |xargs rm -rf {}")
    history_store_server = get_history_server()
    history_store_server_path = "/home/OAP_RELEASE_RESULT/" + os.path.basename(workflow) + "/" + workflow_start_date
    ssh_execute(history_store_server, "mkdir -p " + history_store_server_path)
    os.system("echo 'y' | scp -rp " + resDir + " root@" + history_store_server.ip + ":" + history_store_server_path)
    # when the date is not equal to workflow start date
    conf_finish_date = os.path.basename(resDir).split("_")[-3]
    if conf_finish_date != workflow_start_date:
        history_store_server_path = "/home/OAP_RELEASE_RESULT/" + os.path.basename(workflow) + "/" + conf_finish_date
        ssh_execute(history_store_server, "mkdir -p " + history_store_server_path)
        os.system("echo 'y' | scp -r " + resDir + " root@" + history_store_server.ip + ":" + history_store_server_path)


def return_last_result_metadata(path):
    with open(path, "r") as f:
        last_path = f.readlines()[0].strip(" ").strip("\n")
        f.close
    return last_path

def baseline_conf_list_filter(testing_conf_list, baseline_conf_list):
    remain_testing_conf_list=[]
    for conf in testing_conf_list:
        if conf.endswith("_Baseline"):
            baseline_conf_list.append(conf)
        else:
            remain_testing_conf_list.append(conf)
    return remain_testing_conf_list

def throughput_test_conf_list_filter(testing_conf_list, throughput_test_list):
    remain_testing_conf_list = []
    for conf in testing_conf_list:
        if verfiry_throughput_test_conf(conf):
            throughput_test_list.append(conf)
        else:
            remain_testing_conf_list.append(conf)
    return remain_testing_conf_list

def release_hdfs_space(conf, path):
    cluster_file = get_cluster_file(conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(conf)
    start_spark_sql(conf)
    ssh_execute(master, beaver_env.get("HADOOP_HOME") + "/bin/hdfs dfs -rm -R " + path)

def run_workflow(workflow, plugins):
    update_workflow(workflow)
    output_workflow = os.path.join(workflow, "output/output_workflow")
    baseline_comp = verfiry_baseline_comp(os.path.join(workflow, "common"))
    testing_conf_list = []
    dataGen_conf_list = []
    baseline_throughtput_test_conf_list = []
    testing_throughtput_test_conf_list = []
    baseline_conf_list = []
    workflow_result_dict = {}
    baseline_result_dict = {}
    workflow_start_date = time.strftime("%Y-%m-%d", time.localtime())

    get_conf_list(output_workflow, testing_conf_list, dataGen_conf_list)
    if len(testing_conf_list) == 0 or len(dataGen_conf_list) == 0:
        print "Please define the conf you want to test in [your_workflow}/.base"
        exit(1)

    update_package(testing_conf_list[0], plugins)

    testing_conf_list = baseline_conf_list_filter(testing_conf_list, baseline_conf_list)
    baseline_conf_list = throughput_test_conf_list_filter(baseline_conf_list, baseline_throughtput_test_conf_list)
    testing_conf_list = throughput_test_conf_list_filter(testing_conf_list, testing_throughtput_test_conf_list)

    for conf in dataGen_conf_list:
        run_dataGen_conf(conf)

    if baseline_comp:
        for conf in baseline_conf_list:
            run_conf(conf, "", workflow, workflow_result_dict, workflow_start_date, baseline_result_dict)

    for conf in testing_conf_list:
        try:
            run_conf(conf, plugins, workflow, workflow_result_dict, workflow_start_date, baseline_result_dict)
        except Exception as e:
            print (e)

    if baseline_comp:
        if len(baseline_throughtput_test_conf_list) != 0:
            release_hdfs_space(baseline_throughtput_test_conf_list[0], "/HiBench")
        for conf in baseline_throughtput_test_conf_list:
            run_conf(conf, "", workflow, workflow_result_dict, workflow_start_date, baseline_result_dict)

    if len(testing_throughtput_test_conf_list) != 0:
        release_hdfs_space(testing_throughtput_test_conf_list[0], "/HiBench")
    for conf in testing_throughtput_test_conf_list:
        run_conf(conf, plugins, workflow, workflow_result_dict, workflow_start_date, baseline_result_dict)

    analyze_workflow_result(workflow, workflow_result_dict, workflow_start_date,  os.path.join(output_workflow, "summary.html"))
    if baseline_comp:
        analyze_workflow_result(workflow, baseline_result_dict, workflow_start_date,  os.path.join(output_workflow, "baseline-summary.html"))
    if workflow_result_dict:
        send_mail_workflow_result(workflow, dataGen_conf_list[0])

if __name__ == '__main__':
    # parser = argparse.ArgumentParser(description='manual to this script')
    # parser.add_argument('--workflow', type=str, default=None)
    # parser.add_argument('--plugins', type=str, default=None)
    # args = parser.parse_args()
    #
    # workflow = os.path.abspath(args.workflow)
    # update_workflow(workflow)
    # output_workflow=os.path.join(workflow,"output/output_workflow")
    # if not args.plugins is None:
    #     plugins = args.plugins.split(",")
    # testing_conf_list = []
    # dataGen_conf_list = []
    #
    # get_conf_list(output_workflow, testing_conf_list, dataGen_conf_list)
    # if len(testing_conf_list) == 0 or len(dataGen_conf_list) == 0:
    #     print "Please define the conf you want to test in [your_workflow}/.base"
    #     exit(1)
    #
    # update_package(testing_conf_list[0], plugins)
    #
    # for conf in dataGen_conf_list:
    #     run_dataGen_conf(conf)

    # for conf in testing_conf_list:
    #     run_conf(conf, plugins, workflow, )

    # send_mail_workflow_conf_result("/HOME/hadoop", "HOME", "/home", False)

    workflow="/home/jh/Beaver/repo/workflows/oap_release_pmem_cluster_2_gold"
    # workflow_result_dict={}
    # workflow_start_date = "2020-11-17"
    # conf1 = "TPCDS_3TB_parquet_DCPMM_Plasma_ColumnVector"
    # conf2 = "KMEANS_250GB_INTEL_MLLIB"
    # conf3 = "TPCH_1.5TB_parquet_NATIVE_SQL_ENGINE"
    # workflow_result_dict[conf1] = ["total queries: 9; success: 8, regression: q26",
    #                                "https://10.239.47.195/oap_release_pmem_cluster_1_gold/" + workflow_start_date + "/" +conf1]
    # workflow_result_dict[conf2] = ["total queries: 9; success: 9, regression: q26",
    #                                "https://10.239.47.195/oap_release_pmem_cluster_1_gold/" + workflow_start_date + "/" + conf2]
    # workflow_result_dict[conf3] = ["total queries: 9; success: 9, regression: q26",
    #                                "https://10.239.47.195/oap_release_pmem_cluster_1_gold/" + workflow_start_date + "/" + conf3]
    # output_workflow=os.path.join(workflow, "output/output_workflow")
    #
    # analyze_workflow_result(workflow, workflow_result_dict, workflow_start_date,
    #                         os.path.join(output_workflow, "summary.html"))
    #
    # command = "mail -s \"$(echo -e \"" + os.path.basename(
    #     workflow) + " Summary: " + "\nContent-Type: text/html; charset=utf-8\")\""
    # result_html = os.path.join(output_workflow, "summary.html")
    # os.system(command + " " + "hao.jin@intel.com" + " < " + result_html)
    run_workflow(workflow, "oap")
