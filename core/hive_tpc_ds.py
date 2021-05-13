#!/usr/bin/python

from core.maven import *
from core.pat import *
from utils.node import *

TPC_DS_COMPONENT = "TPC-DS"


def deploy_hive_tpc_ds(custom_conf, master):
    print("Deploy Hive TPC_DS")
    clean_hive_tpc_ds(master)
    beaver_env = get_merged_env(custom_conf)
    deploy_maven(master, beaver_env)
    copy_packages([master], TPC_DS_COMPONENT, beaver_env.get("TPC_DS_VERSION"))
    deploy_pat(custom_conf, master)
    update_copy_tpc_ds_conf(master, custom_conf, beaver_env)


def undeploy_hive_tpc_ds(master):
    print("Undeploy Hive TPC_DS")
    clean_hive_tpc_ds(master)


def clean_hive_tpc_ds(master):
    ssh_execute(master, "rm -rf /opt/Beaver/TPC-DS*")


def populate_hive_tpc_ds_conf(master, custom_conf, beaver_env):
    print("replace Hive TPC_DS configurations")
    update_copy_tpc_ds_conf(master, custom_conf, beaver_env)


def update_copy_tpc_ds_conf(master, custom_conf, beaver_env):
    output_conf = os.path.join(custom_conf, "output")
    ds_output_conf = copy_conf_tree(custom_conf, TPC_DS_COMPONENT, output_conf)
    
    ds_tar_file_name = "ds.conf.tar"
    ds_tar_file = os.path.join(output_conf, ds_tar_file_name)
    os.system("cd " + ds_output_conf + ";" + "tar cf " + ds_tar_file + " *")
    ds_home = beaver_env.get("TPC_DS_HOME")
    remote_tar_file = os.path.join(ds_home, ds_tar_file_name)
    ssh_copy(master, ds_tar_file, remote_tar_file)
    ssh_execute(master, "tar xf " + remote_tar_file + " -C " + ds_home)

    ssh_execute(master, "mkdir ~/.m2;\cp -f " + ds_home + "/settings.xml.bak ~/.m2/settings.xml" )
    populate_pat_conf(custom_conf)


'''
def update_copy_tpc_ds_conf(master, custom_conf, beaver_env):
    output_conf = os.path.join(custom_conf, "output")
    tpc_ds_output_conf = copy_conf_tree(custom_conf, TPC_DS_COMPONENT, output_conf)
    
    tpc_ds_tar_file_name = "tpc_ds.conf.tar"
    tpc_ds_tar_file = os.path.join(output_conf, tpc_ds_tar_file_name)
    os.system("cd " + tpc_ds_output_conf + ";"+ "tar cf " + tpc_ds_tar_file + " *")
    tpc_ds_home = beaver_env.get("TPC_DS_HOME")
    remote_tar_file = os.path.join(tpc_ds_home, tpc_ds_tar_file_name)
    ssh_copy(master, tpc_ds_tar_file, remote_tar_file)
    ssh_execute(master, "tar xf " + remote_tar_file + " -C " + tpc_ds_home)
'''


def build_tpc_ds(master, tpc_ds_home):
    print("+++++++++++++++++++++++++++++")
    print("Install gcc. Downloads, compiles and packages the TPC-DS data generator.")
    print("+++++++++++++++++++++++++++++")
    cmd = "yum -y install gcc make flex bison byacc unzip;"
    cmd += "yum -y install patch;"
    cmd += "cd " + tpc_ds_home + ";bash -x ./tpcds-build.sh;"
    ssh_execute(master, cmd)


def generate_tpc_ds_data(master, tpc_ds_home, scale, data_format):
    print("+++++++++++++++++++++++++++++")
    print("Generate tpc-ds data and load data")
    print("+++++++++++++++++++++++++++++")
    cmd = "cd " + tpc_ds_home + ";FORMAT=" + data_format + " ./tpcds-setup.sh " + scale + ";"
    ssh_execute(master, cmd)

def run_tpcds_direct(custom_conf):
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(custom_conf)
    run_hive_tpc_ds(master, custom_conf, beaver_env, False)

def run_hive_tpc_ds(master, custom_conf, beaver_env, slaves, use_pat):
    pat_home = beaver_env.get("PAT_HOME")
    run_datanode_on_master = beaver_env.get("RUN_DATANODE_ON_MASTER")
    output_conf = os.path.join(custom_conf, "output/")
    pat_output_path = os.path.join(output_conf, PAT_COMPONENT)
    pat_config_xml_conf = os.path.join(pat_output_path, "PAT-post-processing/config.xml")
    tpc_ds_home = beaver_env.get("TPC_DS_HOME")
    tpc_ds_config_file = os.path.join(custom_conf, "TPC-DS/config")
    config_dict = get_configs_from_properties(tpc_ds_config_file)
    scale = config_dict.get("scale")
    queries = config_dict.get("queries")
    build_flg = config_dict.get("build")
    generate_flg = config_dict.get("generate")
    data_format = config_dict.get("format")
    if not queries:
        queries = ''
    if build_flg == "yes":
        build_tpc_ds(master, tpc_ds_home)
    if generate_flg == "yes":
        if data_format == "":
            print(colors.LIGHT_RED + "Please set the format in <custom_conf>/TPC-DS/config file" + colors.ENDC)
            return
        if int(scale) < 2:
            print(colors.LIGHT_RED + "The scale in <custom_conf>/TPC-DS/config file must greater than 1" + colors.ENDC)
            return
        generate_tpc_ds_data(master, tpc_ds_home, scale, data_format)
    tpc_ds_result = os.path.join(beaver_env.get("TPC_DS_RES_DIR"), str(time.strftime("%Y-%m-%d-%H-%M-%S",
                                                                     time.localtime())))
    if use_pat:
        queries_arr = queries.split(",")
        for queryId in queries_arr:
            print (colors.LIGHT_BLUE + "Run TPC-DS with PAT..." + colors.ENDC)
            tree = ET.parse(pat_config_xml_conf)
            root = tree.getroot()
            all_nodes_content = "ALL_NODES: "
            if len(slaves) == 1:
                for node in slaves:
                    all_nodes_content += node.hostname + ":22 "
            else:
                if run_datanode_on_master == "TRUE":
                    for node in slaves:
                        all_nodes_content += node.hostname + ":22 "
                else:
                    for node in slaves:
                        if node.hostname == "master":
                            all_nodes_content += node.hostname + ":22 "
            cmd = "cp -r " + pat_home + "/PAT-collecting-data/config.template " + pat_home + "/PAT-collecting-data/config;"
            cmd += "sed -i 's/ALL_NODES/#ALL_NODES/g' " + pat_home + "/PAT-collecting-data/config;"
            cmd += "echo " + all_nodes_content + " >> " + pat_home + "/PAT-collecting-data/config;"
            ssh_execute(master, cmd)
            cmd = "sed -i 's/CMD_PATH/#CMD_PATH/g' " + pat_home + "/PAT-collecting-data/config;"
            ssh_execute(master, cmd)
            print (colors.LIGHT_BLUE + "Running Benchmark with PAT: " + queryId + colors.ENDC)
            # write tpcds_with_pat to shell file
            tpcds_with_pat_shell_file = "tpcds_pat.sh"
            cmd = "echo cd /opt/Beaver/TPC-DS \\; perl runSuite.pl tpcds \\" + scale + " \\" + queryId + "> " + os.path.join(pat_home,
                                tpcds_with_pat_shell_file)
            ssh_execute(master, cmd)
            cmd = "echo CMD_PATH: sh " + os.path.join(pat_home, tpcds_with_pat_shell_file) + " >> " + pat_home + "/PAT-collecting-data/config;"
            cmd += "cd " + pat_home + "/PAT-collecting-data;./pat run " + queryId
            ssh_execute(master, cmd)

            print (colors.LIGHT_BLUE + "Generating PAT report: " + queryId + colors.ENDC)
            root.find("source").text = pat_home + "/PAT-collecting-data/results/" + queryId + "/instruments"
            tree.write(pat_config_xml_conf)
            ssh_copy(master, pat_config_xml_conf, pat_home + "/PAT-post-processing/config.xml")
            cmd = "cd " + pat_home + "/PAT-post-processing;./pat-post-process.py;"
            ssh_execute(master, cmd)
            copy_res_hive_tpc_ds(master, beaver_env, tpc_ds_result)
    else:
        print (colors.LIGHT_BLUE + "Run TPC-DS..." + colors.ENDC)
        # cmd = "mkdir -p " + tpc_ds_result + ";cd " + tpc_ds_home + ";perl runSuite.pl tpcds " + scale + " >> " + tpc_ds_result + "/result.log;" + "\cp -rf " +tpc_ds_result + "/result.log" + " /opt/Beaver/;"
        cmd = "mkdir -p " + tpc_ds_result + ";cd " + tpc_ds_home + ";perl runSuite.pl tpcds " + scale +" " + queries + " >> " + tpc_ds_result + "/result.log;" + "\cp -rf " +tpc_ds_result + "/result.log" + " /opt/Beaver/;"
        ssh_execute(master, cmd)
        copy_res_hive_tpc_ds(master, beaver_env, tpc_ds_result)


def generate_tpc_ds_data_onhive(master, custom_conf, beaver_env):
    tpc_ds_home = beaver_env.get("TPC_DS_HOME")
    tpc_ds_config_file = os.path.join(custom_conf, "TPC-DS/config")
    config_dict = get_configs_from_properties(tpc_ds_config_file)
    scale = config_dict.get("scale")
    build_flg = config_dict.get("build")
    generate_flg = config_dict.get("generate")
    data_format = config_dict.get("format")
    if build_flg == "yes":
        build_tpc_ds(master, tpc_ds_home)
    if generate_flg == "yes":
        if data_format == "":
            print(colors.LIGHT_RED + "Please set the format in <custom_conf>/TPC-DS/config file" + colors.ENDC)
            return
        if int(scale) < 2:
            print(colors.LIGHT_RED + "The scale in <custom_conf>/TPC-DS/config file must greater than 1" + colors.ENDC)
            return
        generate_tpc_ds_data(master, tpc_ds_home, scale, data_format)

def copy_res_hive_tpc_ds(master, beaver_env, res_dir):
    print("Collect Hive TPC_DS benchmark result")
    tpc_ds_home = beaver_env.get("TPC_DS_HOME")
    log_dir = os.path.join(tpc_ds_home, "sample-queries-tpcds")
    ssh_execute(master, "mkdir -p " + res_dir + ";cp -r " + log_dir + "/*.log" + " " + res_dir)
