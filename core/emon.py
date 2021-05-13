from core.spark import *
from utils.node import *
from core.sbt import *
from core.jdk import *
from core.oap import *

EMON_COMPONENT="emon"

def deploy_emon(custom_conf, slaves):
    beaver_env = get_merged_env(custom_conf)
    if beaver_env.get("EMON_ENABLE").lower() == "true":
        clean_emon(slaves)
        cluster_file = get_cluster_file(custom_conf)
        slaves = get_slaves(cluster_file)
        emon_version = beaver_env.get("EMON_VERSION")
        copy_emon(emon_version, slaves)
        compile_emon(slaves, beaver_env)
        copy_emon_test_script(slaves, custom_conf, beaver_env)


def copy_emon_test_script(slaves, custom_conf, beaver_env):
    script_folder = os.path.join(tool_path, "emon")
    dst_path = os.path.join(beaver_env.get("EMON_HOME"), "emon_scripts")
    dict = emon_test_dict(beaver_env)
    for node in slaves:
        print (colors.LIGHT_BLUE + "\tCopy emon script to " + node.hostname + "..." + colors.ENDC)
        copy_test_script_to_remote(node, script_folder, dst_path, dict)


def copy_test_script_to_remote(node, script_folder, dst_path, dict):
    output_folder = os.path.join(package_path, "tmp/script/" + os.path.basename(script_folder))
    subprocess.check_call("rm -rf " + output_folder, shell=True)
    subprocess.check_call("mkdir -p " + output_folder, shell=True)
    subprocess.check_call("cp -rf " + script_folder + "/* " + output_folder, shell=True)
    #dict = gen_test_dict(node, custom_conf, beaver_env, v.strip(), test_mode)
    output_folder_star = output_folder + "/*"
    final_config_files = glob.glob(output_folder_star)
    for file in final_config_files:
        if not os.path.isdir(file):
            replace_conf_value(file, dict)
    ssh_execute(node, "rm -rf " + dst_path)
    ssh_execute(node, "mkdir -p " + dst_path)
    dst_folder = dst_path
    recursive_remote_copy(node, output_folder, dst_folder)


def emon_test_dict(beaver_env):
    dict = {};
    dict["{%emon.home%}"] = beaver_env.get("EMON_HOME")
    for key, val in dict.items():
        if val == None:
            dict[key] = ""
    return dict

def copy_emon(version, slaves):
    package = "emon-" + version + ".tar.gz"
    if not os.path.isfile(os.path.join(package_path, package)):
        download_url = "http://" + download_server + "/software"
        print (colors.LIGHT_BLUE + "/tDownloading " + package + " from our repo..." + colors.ENDC)
        os.system("wget -P " + package_path + " " + download_url + "/" + package)
    else:
        print (colors.LIGHT_GREEN + "\t" + package + " has already exists in Beaver package" + colors.ENDC)
    copy_package_dist(slaves, os.path.join(package_path, package), EMON_COMPONENT, version)

def compile_emon(slaves, beaver_env):
    for node in slaves:
        ssh_execute(node, " yum -y install kernel-devel-$(uname -r); yum -y install  elfutils-libelf-devel")
        ssh_execute(node, "cd " + os.path.join(beaver_env.get("EMON_HOME"), "sepdk/src" )+ " && ./build-driver -ni")

def clean_emon(slaves):
    for node in slaves:
        ssh_execute(node, "rm -rf  /opt/Beaver/emon*")

def run_emon(slaves, beaver_env):
    if beaver_env.get("EMON_ENABLE").lower() == "true":
        for node in slaves:
            if node.role == "master":
                continue
            print (colors.LIGHT_BLUE + "Running emon on  " + node.hostname + "..." + colors.ENDC)
            ssh_execute(node, "cd " + os.path.join(beaver_env.get("EMON_HOME"), "emon_scripts") + "; sh emon.sh start")

def stop_emon(slaves, beaver_env):
    if beaver_env.get("EMON_ENABLE").lower() == "true":
        master = get_master_node(slaves)
        data_location = os.path.join(beaver_env.get("EMON_HOME"), "data")
        ssh_execute(master, "rm -rf  " + data_location)
        ssh_execute(master, "mkdir -p " + data_location)
        for node in slaves:
            if node.role == "master":
                continue
            print (colors.LIGHT_BLUE + "/Stopping emon on  " + node.hostname + "..." + colors.ENDC)
            ssh_execute(node, "cd " + os.path.join(beaver_env.get("EMON_HOME"), "emon_scripts") + "; sh emon.sh stop")
        for node in slaves:
            if node.role == "master":
                continue
            print (colors.LIGHT_BLUE + "Scp emon date from " + node.hostname + " to master..." + colors.ENDC)
            ssh_execute(master, "scp -r root@" + node.ip + ":" + os.path.join(beaver_env.get("EMON_HOME"),
                                                                              "emon_data/*") + " " + data_location)


if __name__ == '__main__':
    custom_conf = "/home/jh/Beaver/repo/workflows/oap_release_cluster_1_function/output/output_workflow/oap-cache/DailyTest_200GB_DCPMM_Guava"
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(custom_conf)
    deploy_emon(custom_conf, slaves)
    run_emon(slaves, beaver_env)
    stop_emon(slaves, beaver_env)