import glob
import re
import time
import os
from utils.config_utils import *
from utils.colors import *
from utils.ssh import *
import shutil
from datetime import datetime
from inspect import currentframe, getframeinfo
import smtplib
from email.mime.text import MIMEText
from email.header import Header
import socket

download_server = os.environ.get("PACKAGE_SERVER", "10.239.44.95")
current_path = os.path.dirname(os.path.abspath(__file__))
project_path = os.path.dirname(current_path)
package_path = os.path.join(project_path, "package")
build_path = os.path.join(package_path, "build")
config_path = os.path.join(project_path, "conf")
runtime_path = os.path.join(project_path, "runtime")
local_repo_path = os.path.join(project_path, "repo")
tool_path = os.path.join(project_path, "tools")
flink_source_code_path = os.path.join(package_path, "source_code/flink")
spark_source_code_path = os.path.join(package_path, "source_code/spark")
oap_source_code_path = os.path.join(package_path, "source_code/oap")
oapperf_source_code_path = os.path.join(package_path, "source_code/oap-perf")

# Execute command on slave nodes
def execute_command_dist(slaves, command):
    print ("Execute commands over slaves")
    for node in slaves:
        ssh_execute(node, command)

def repeat_execute_command_dist(slaves, command, retry_times = 5):
    exit_status = 1
    if len(slaves) == 0:
        print ("Execute commands on local")
        while (exit_status != 0 and retry_times > 0):
            try:
                subprocess.check_call(command, shell=True)
                exit_status = 0
            except:
                retry_times -= 1
        if (exit_status != 0 and retry_times == 0):
            print (colors.LIGHT_RED + "Fail to execute commands : " + command + colors.ENDC)
    else:
        print ("Execute commands over slaves")
        for node in slaves:
            while (exit_status != 0 and retry_times > 0):
                exit_status = ssh_execute(node, command)
                retry_times -= 1
        if (exit_status != 0 and retry_times == 0):
            print (colors.LIGHT_RED + "Fail to execute commands over slaves: " + command + colors.ENDC)
    return exit_status

# Get all the defined properties from a property file
def get_properties(filename):
    properties = {}
    if not os.path.isfile(filename):
        return properties
    with open(filename) as f:
        for line in f:
            if line.startswith('#') or not line.split():
                continue
            key, value = line.partition("=")[::2]
            properties[key.strip()] = value.strip()
    return properties


def read_bigBench_workloads(bigBench_properties, bb_home):
    bigBench_command = []
    workload_value = ""
    power_test_value = ""
    for key in bigBench_properties:
        if key == "workload":
            workload_value = bigBench_properties[key]
        elif key == "power_test_0":
            power_test_value = bigBench_properties[key]
    workload_value_arr = workload_value.split(",")
    for workload_key in workload_value_arr:
        if workload_key == "CLEAN_ALL":
            bigBench_command.append(workload_key)
            bigBench_command.append(bb_home + "/bin/bigBench cleanAll -U")
        elif workload_key == "CLEAN_DATA":
            bigBench_command.append(workload_key)
            bigBench_command.append(bb_home + "/bin/bigBench cleanData -U")
        elif workload_key == "DATA_GENERATION":
            bigBench_command.append(workload_key)
            bigBench_command.append(bb_home + "/bin/bigBench dataGen -U")
        elif workload_key == "LOAD_TEST":
            bigBench_command.append(workload_key)
            bigBench_command.append(bb_home + "/bin/bigBench populateMetastore -U \&\& " + bb_home + "/bin/bigBench refreshMetastore -U")
        elif workload_key == "POWER_TEST":
            queryNumber = read_bigBench_power_test_query_list(power_test_value)
            for query in queryNumber:
                bigBench_command.append(workload_key + "_" + str(query))
                bigBench_command.append(bb_home + "/bin/bigBench runQuery -q "+ str(query)+" -U")
        elif workload_key == "VALIDATE_POWER_TEST":
            bigBench_command.append(workload_key)
            bigBench_command.append(" ")
        else:
            print ("UNKNOWN WORKLOAD:" + workload_key)
    return bigBench_command


def read_bigBench_power_test_query_list(power_test_value):
    tempQuerys = []
    power_test_value_arr = power_test_value.split(",")
    for tempQuery in power_test_value_arr:
        if "-" in tempQuery:
            subquery = tempQuery.split("-")
            if len(subquery) == 2:
                for i in range(int(subquery[0]), int(subquery[1]) + 1):
                    tempQuerys.append(i)
            else:
                print ("Format error")
        else:
            tempQuerys.append(tempQuery)
    return tempQuerys


# Set environment variables
def setup_env_dist(slaves, envs, component):
        print (colors.LIGHT_BLUE+ "Set environment variables over all nodes" + colors.ENDC)
        # list = ["HADOOP_HOME", "JAVA_HOME", "HIVE_HOME", "BB_HOME"]
        rcfile = "/opt/Beaver/" + component + "rc"
        for node in slaves:
            cmd = ""
            cmd += "rm -f " + rcfile + "; "
            # with open(rcfile, 'w') as f:
            #     for key, value in envs.iteritems():
            #         if key in list:
            #             line = key + "=" + value
            #             f.write(line + "\n")
            # for python 2.7
            #for key, value in envs.iteritems():
            for key, value in envs.items():
                if key=="SPARK_VERSION":
                    continue
                if key=="SPARK_COMPILED_SETTING":
                    continue
                if key=="OAP_COMPILED_SETTING":
                    continue
                if key=="FLINK_COMPILED_SETTING":
                    continue
                if len(value.split(" ")) > 1:
                    continue
                cmd += "echo \"export " + key + "=" + value + "\">> /opt/Beaver/" + component + "rc;"
            if detect_rcfile(node, component):
                cmd += "echo \"" + "if [ -f " + rcfile + " ]; then\n" \
                        + "   . " + rcfile + "\n" \
                        + "fi\"" + " >> ~/.bashrc;"
            ssh_execute_withReturn(node, cmd)

# Detect whether the "~/.bashrc" file of this node has been written in this statement: "./opt/Beaver/<component>rc"
def detect_rcfile(node, component):
    if not os.path.exists(package_path):
        os.makedirs(package_path)
    remote_path = "/" + node.username + "/.bashrc"
    ssh_download(node, remote_path, os.path.join(package_path, "bashrc"))
    bashfile = os.path.join(package_path, "bashrc")
    strLine = ". /opt/Beaver/" + component + "rc"
    with open(bashfile) as f:
        flag = True
        for line in f:
            str = re.findall(strLine, line)
            if len(str) > 0:
                flag = False
    os.remove(bashfile)
    return flag

# Set environment variables
def setup_local_env(master_hostname, envs, component):
    local_hostname = socket.gethostname()
    # set up ssh key for local pc to master pc if local pc is not master pc
    if local_hostname != master_hostname:
        print (colors.LIGHT_BLUE+ "Set environment variables over local" + colors.ENDC)
        dict = {}
        dict["sbt"] = "SBT_HOME"
        dict["maven"] = "MAVEN_HOME"
        dict["java"] = "JAVA_HOME"
        rcfile = "/opt/Beaver/" + component + "rc"
        cmd = ""
        cmd += "rm -f " + rcfile + "; "
        for key, value in envs.items():
            if key == dict[component]:
                cmd += "echo \"export " + key + "=" + value + "\">> /opt/Beaver/" + component + "rc;"
        if detect_local_rcfile(component):
            cmd += "echo \"" + "if [ -f " + rcfile + " ]; then\n" \
                    + "   . " + rcfile + "\n" \
                    + "fi\"" + " >> ~/.bashrc;"
        subprocess.check_call(cmd, shell=True)

# Detect whether the "~/.bashrc" file of this node has been written in this statement: "./opt/Beaver/<component>rc"
def detect_local_rcfile(component):
    if not os.path.exists(package_path):
        os.makedirs(package_path)
    bashfile = "/root/.bashrc"
    strLine = ". /opt/Beaver/" + component + "rc"
    with open(bashfile) as f:
        flag = True
        for line in f:
            str = re.findall(strLine, line)
            if len(str) > 0:
                flag = False
    return flag

# # Write IP address of all nodes to "/etc/hosts" file
def update_etc_hosts(slaves):
    print (colors.LIGHT_BLUE + "Update /etc/hosts file on slaves:" + colors.ENDC)
    str_hosts = "127.0.0.1 localhost\n"
    for node in slaves:
        str_hosts += node.ip + "  " + node.hostname + "\n"

    for node in slaves:
        print (colors.LIGHT_BLUE + "Update /etc/hosts file on " + node.hostname + "(" + node.ip + ")" + colors.ENDC)
        ssh_execute(node, "echo \"" + str_hosts + "\">/etc/hosts;")

# Add binary files of different components into PATH
def set_path(component, slaves, path):
    print (colors.LIGHT_BLUE+ "Add binary files of " + component + " into PATH env" + colors.ENDC)
    for node in slaves:
        cmd = "echo \"export PATH=" + path + "/bin:" + path + "/sbin:\$PATH\" >> /opt/Beaver/" + component + "rc"
        ssh_execute(node, cmd)
        ssh_execute(node, "source ~/.bashrc")

# Copy and unpack a package to slave nodes
def copy_package_dist(slaves, file, component, version):
    for node in slaves:
        print (colors.LIGHT_BLUE + "\tCopy " + component + " to " + node.hostname + "..." + colors.ENDC)
        ssh_execute(node, "mkdir -p /opt/Beaver/")
        ssh_copy(node, file, "/opt/Beaver/" + os.path.basename(file))
        print (colors.LIGHT_BLUE + "\tUnzip " + component + " on " + node.hostname + "..." + colors.ENDC)
        softlink = "/opt/Beaver/" + component
        package = "/opt/Beaver/" + os.path.basename(file)
        component_home = "/opt/Beaver/" + component + "-" + version
        cmd = "rm -rf " + softlink + ";"
        cmd += "rm -rf " + component_home + ";"
        cmd += "mkdir -p " + component_home + ";"\
            + "tar zxf " + package + " -C " + component_home + " --strip-components=1 > /dev/null"
        ssh_execute(node, cmd)
        cmd = "ln -s " + component_home + " " + softlink + ";"\
            + "rm -rf " + package
        ssh_execute(node, cmd)

# Add binary files of different components into PATH
def set_local_path(master_hostname, component, path):
    local_hostname = socket.gethostname()
    # set up ssh key for local pc to master pc if local pc is not master pc
    if local_hostname != master_hostname:
        print (colors.LIGHT_BLUE+ "Add binary files of " + component + " into PATH env" + colors.ENDC)
        cmd = "echo \"export PATH=" + path + "/bin:" + path + "/sbin:\$PATH\" >> /opt/Beaver/" + component + "rc"
        subprocess.check_call(cmd, shell=True)
        subprocess.check_call("source ~/.bashrc", shell=True)

# Copy and unpack a package to slave nodes
def copy_package_to_local(master_hostname, component, version):
    local_hostname = socket.gethostname()
    # set up ssh key for local pc to master pc if local pc is not master pc
    if local_hostname != master_hostname:
        print (colors.LIGHT_BLUE + "Distribute " + "tar.gz file" + " for " + component + colors.ENDC)
        download_url = "http://" + download_server + "/" + component
        package = component + "-" + version + ".tar.gz"
        if not os.path.isfile(os.path.join(package_path, package)):
            print (colors.LIGHT_BLUE + "\tDownloading " + package + " from our repo..." + colors.ENDC)
            os.system("wget -P " + package_path + " " + download_url + "/" + package)
        else:
            print (colors.LIGHT_GREEN + "\t" + package + " has already exists in Beaver package" + colors.ENDC)
        file = os.path.join(package_path, package)
        print (colors.LIGHT_BLUE + "\tCopy " + component + " to local..." + colors.ENDC)
        subprocess.check_call("mkdir -p /opt/Beaver/", shell=True)
        subprocess.check_call("cp "+ file + " /opt/Beaver/" + os.path.basename(file), shell=True)
        print (colors.LIGHT_BLUE + "\tUnzip " + component + " on local..." + colors.ENDC)
        softlink = "/opt/Beaver/" + component
        package = "/opt/Beaver/" + os.path.basename(file)
        component_home = "/opt/Beaver/" + component + "-" + version
        cmd = "rm -rf " + softlink + ";"
        cmd += "rm -rf " + component_home + ";"
        cmd += "mkdir -p " + component_home + ";"\
            + "tar zxf " + package + " -C " + component_home + " --strip-components=1 > /dev/null"
        subprocess.check_call(cmd, shell=True)
        cmd = "ln -s " + component_home + " " + softlink + ";"\
            + "rm -rf " + package
        subprocess.check_call(cmd, shell=True)


# Copy component package to slave nodes
def copy_packages(nodes, component, version):
    print (colors.LIGHT_BLUE + "Distribute " + "tar.gz file" + " for " + component + colors.ENDC)
    download_url = "http://" + download_server + "/" + component
    package = component + "-" + version + ".tar.gz"
    if not os.path.isfile(os.path.join(package_path, package)):
        print (colors.LIGHT_BLUE + "\tDownloading " + package + " from our repo..." + colors.ENDC)
        os.system("wget -P " + package_path + " " + download_url + "/" + package)
    else:
        print (colors.LIGHT_GREEN + "\t" + package + " has already exists in Beaver package" + colors.ENDC)
    copy_package_dist(nodes, os.path.join(package_path, package), component, version)

# Copy "spark-<version>-yarn-shuffle.jar" to all of Hadoop nodes
def copy_spark_shuffle(slaves, spark_version, hadoop_home):
    package = "spark-" + spark_version + "-yarn-shuffle.jar"
    if not os.path.isfile(os.path.join(package_path, package)):
        download_url = "http://" + download_server + "/software"
        print (colors.LIGHT_BLUE + "Downloading " + package + " from our repo..." + colors.ENDC)
        os.system("wget -P " + package_path + " " + download_url + "/" + package)
    else:
        print (colors.LIGHT_GREEN + "\t" + package + " has already exists in Beaver package" + colors.ENDC)
    des_path = os.path.join(hadoop_home, "share/hadoop/yarn/lib")
    cmd = "rm -rf " + des_path + "/spark-*-yarn-shuffle.jar"
    des = os.path.join(des_path, package)
    for node in slaves:
        print (colors.LIGHT_BLUE + "\tCopy spark-shuffle jar to " + node.hostname + "..." + colors.ENDC)
        ssh_execute(node, cmd)
        ssh_copy(node, os.path.join(package_path, package), des)

# Generate final configuration file and copy this files to destination node
def copy_configurations(nodes, config_path, component, home_path):
    print (colors.LIGHT_BLUE + "Distribute configuration files for " + component + ":" + colors.ENDC)
    print (colors.LIGHT_BLUE + "\tGenerate final configuration files of " + component + colors.ENDC)
    path = config_path + "/*"
    final_config_files = glob.glob(path)
    #print (final_config_files)
    copy_final_configs(nodes, final_config_files, component, home_path)

#Copy final configuration files to destination
def copy_final_configs(nodes, config_files, component, home_path):
    print (colors.LIGHT_BLUE + "\tCopy configuration files of " + component + " to all nodes" + colors.ENDC)
    if component == "hadoop":
        conf_link = os.path.join(home_path, "etc/hadoop")
        conf_path = os.path.join(home_path, "etc/") + str(time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime())) + "/"
    if component == "hive":
        conf_link = os.path.join(home_path, "conf")
        conf_path = home_path + "/config/" + str(time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime())) + "/"
    if component == "spark":
        conf_link = os.path.join(home_path, "conf")
        conf_path = home_path + "/config/" + str(time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime())) + "/"
    if component == "flink":
        conf_link = os.path.join(home_path, "conf")
        conf_path = home_path + "/config/" + str(time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime())) + "/"
    for node in nodes:
        if component == "BB" or component == "hibench":
            break
        ssh_execute(node, "mkdir -p " + conf_path)
        ssh_execute(node, "cp -r " + conf_link + "/*" + " " + conf_path)
        for file in config_files:
            ssh_copy(node, file, conf_path + os.path.basename(file))
        ssh_execute(node, "rm -rf " + conf_link)
        ssh_execute(node, "ln -s " + conf_path + " " + conf_link)
    if component == "hibench":
        conf_link = os.path.join(home_path, "conf")
        conf_path = os.path.join(home_path, "config/") + str(time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime())) + "/"
        for node in nodes:
            ssh_execute(node, "mkdir -p " + conf_path)
            ssh_execute(node, "cp -r " + conf_link + "/*" + " " + conf_path)
            for file in config_files:
                if os.path.basename(file).strip("'\r\n'") in ["hadoop.conf", "spark.conf", "hibench.conf", "flink.conf", "gearpump.conf", "storm.conf"]:
                    ssh_copy(node, file, conf_path + os.path.basename(file))
                else:
                    cmd = "find " + conf_path + " -name " + os.path.basename(file).strip("'\r\n'")
                    stdout = ssh_execute_withReturn(node, cmd)
                    file_path = stdout[0]
                    ssh_copy(node, file, file_path)
                ssh_execute(node, "rm -rf " + conf_link)
                ssh_execute(node, "ln -s " + conf_path + " " + conf_link)
    if component == "BB":
        conf_link = os.path.join(home_path, "conf")
        conf_path = os.path.join(home_path, "config/") + str(time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime())) + "/"
        for node in nodes:
            ssh_execute(node, "mkdir -p " + conf_path)
            ssh_execute(node, "cp -r " + conf_link + "/*" + " " + conf_path)
            for file in config_files:
                if os.path.basename(file) == "engineSettings.sql":
                    conf_link_tmp = os.path.join(home_path, "engines/hive/conf")
                    # conf_path_tmp = os.path.join(bb_home, "engines/hive/config/") + str(
                    #     time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime())) + "/"
                    # ssh_execute(node, "mkdir -p " + conf_path)
                    ssh_execute(node, "cp -r " + conf_link_tmp + "/*" + " " + conf_path)
                    ssh_copy(node, file, conf_path + os.path.basename(file))
                    ssh_execute(node, "rm -rf " + conf_link_tmp)
                    ssh_execute(node, "ln -s " + conf_path+ " " + conf_link_tmp)
                    continue
                ssh_copy(node, file, conf_path + os.path.basename(file))
            ssh_execute(node, "rm -rf " + conf_link)
            ssh_execute(node, "ln -s " + conf_path + " " + conf_link)

def copy_spark_test_script_to_remote(node, script_folder, dst_path, beaver_env, dict):
    spark_version = beaver_env.get("SPARK_VERSION")
    output_folder = os.path.join(package_path, "tmp/script/" + os.path.basename(script_folder)  + "/" + spark_version.strip())
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

    dst_folder = os.path.join(dst_path, spark_version.strip())
    output_folder = os.path.join(package_path, "tmp/script/" + os.path.basename(script_folder) + "/" + spark_version.strip())
    recursive_remote_copy(node, output_folder, dst_folder)

def copy_pmdk_to_local(dst_path):
    download_url = "http://" + download_server + "/pmdk.tar.gz"
    os.system("wget -O " + dst_path + "/pmdk.tar.gz " + download_url)
    os.system("cd "+ dst_path +"&&tar -zxvf pmdk.tar.gz" )

def recursive_remote_copy(node, ori_path, dst_path):
    ori_path_star = ori_path + "/*"
    files = glob.glob(ori_path_star)
    ssh_execute(node, "mkdir -p " + dst_path)
    for file in files:
        if os.path.isdir(file):
            recursive_remote_copy(node, file, os.path.join(dst_path, os.path.relpath(file, ori_path)))
        else:
            ssh_copy(node, file, os.path.join(dst_path, os.path.basename(file)))

def check_env(component, version):
    cmd = "ls /opt/Beaver | grep -x " + component + "-" + version
    installed = ""
    for line in os.popen(cmd).readlines():
        installed += line.strip()
    if installed != component + "-" + version:
        return False
    else:
        print (colors.LIGHT_BLUE + component + " has exists, we do not have to deploy it." + colors.ENDC)
        return True

def stop_firewall(slaves):
    print (colors.LIGHT_BLUE + "Stop firewall service" + colors.ENDC)
    for node in slaves:
        ssh_execute(node, "systemctl stop firewalld")

def spark_get_build_version():
    spark_ET = ET
    spark_pom_tree = spark_ET.parse(os.path.join(spark_source_code_path, 'pom.xml'))
    spark_pom_root = spark_pom_tree.getroot()
    spark_version = spark_pom_root.find('{http://maven.apache.org/POM/4.0.0}version').text
    return spark_version

def spark_get_build_maven_version():
    spark_ET = ET
    spark_pom_tree = spark_ET.parse(os.path.join(spark_source_code_path, 'pom.xml'))
    spark_pom_root = spark_pom_tree.getroot()
    maven_version = spark_pom_root.find('{http://maven.apache.org/POM/4.0.0}properties').find('{http://maven.apache.org/POM/4.0.0}maven.version').text
    return maven_version

def spark_get_build_scala_version():
    spark_ET = ET
    spark_pom_tree = spark_ET.parse(os.path.join(spark_source_code_path, 'pom.xml'))
    spark_pom_root = spark_pom_tree.getroot()
    scala_version = spark_pom_root.find('{http://maven.apache.org/POM/4.0.0}properties').find('{http://maven.apache.org/POM/4.0.0}scala.version').text
    return scala_version

def get_time_now():
    return datetime.now().strftime("%H:%M:%S")

def get_filename():
    return getframeinfo(currentframe()).filename

def get_linenumber():
    return currentframe().f_back.f_lineno

def get_functionname():
    return currentframe().f_back.f_code.co_name

def print_debuginfo():
    print(colors.LIGHT_CYAN + "[" + datetime.now().strftime("%H:%M:%S") + "]:" \
            + currentframe().f_back.f_code.co_name + "()..." + "   : "\
            + getframeinfo(currentframe().f_back).filename + ":"\
            + str(currentframe().f_back.f_lineno) + colors.ENDC)


def get_files_by_suffix(path, suffix):
    return [os.path.join(root, file) for root, dirs, files in os.walk(path) for file in files if file.endswith(suffix)]


def scp_path_to_history_server(root_path, target_path):
    history_store_server = get_history_server()
    history_store_server_path = "/home/OAP_RELEASE_RESULT/" + target_path
    ssh_execute(history_store_server, "mkdir -p " + history_store_server_path)
    os.system("echo 'y' | scp -r " + os.path.join(root_path,
                                                  target_path + "/*") + " root@" + history_store_server.ip + ":" + history_store_server_path)


def sendmail(subject, html_path, receivers, sender_name=""):
    sender = "root@" + socket.gethostname()
    with open(html_path, 'rb') as f:
        mail_body = f.read()
    message = MIMEText(mail_body, 'HTML', "utf-8")
    message['Subject'] = Header(subject, "utf-8")
    if sender_name:
        message['From'] = sender_name
    message['To'] = ",".join(receivers)
    try:
        smtp_obj = smtplib.SMTP('localhost')
        smtp_obj.sendmail(sender, receivers, message.as_string())
    except smtplib.SMTPException as e:
        print e

def get_conf_list(root_path, testing_conf_list, dataGen_conf_list):
    dir_or_files = os.listdir(root_path)
    for dir_file in dir_or_files:
        dir_file_path = os.path.join(root_path, dir_file)
        if os.path.isdir(dir_file_path):
            if os.path.exists(os.path.join(dir_file_path, ".base")):
                if verfiry_dataGen_conf(dir_file_path):
                    dataGen_conf_list.append(dir_file_path)
                else:
                    testing_conf_list.append(dir_file_path)
            else:
                get_conf_list(dir_file_path, testing_conf_list, dataGen_conf_list)

def verfiry_dataGen_conf(conf):
    beaver_env = get_merged_env(conf)
    if not beaver_env.get("GENERATE_DATA") is None and beaver_env.get("GENERATE_DATA").lower() == "true":
        return True
    else:
        return False

def verfiry_throughput_test_conf(conf):
    beaver_env = get_merged_env(conf)
    if not beaver_env.get("THROUGHPUT_TEST") is None and beaver_env.get("THROUGHPUT_TEST").lower() == "true":
        return True
    else:
        return False

def get_all_conf_list(root_path, testing_conf_list):
    dir_or_files = os.listdir(root_path)
    for dir_file in dir_or_files:
        dir_file_path = os.path.join(root_path, dir_file)
        if os.path.isdir(dir_file_path):
            if os.path.exists(os.path.join(dir_file_path, ".base")):
                testing_conf_list.append(dir_file_path)
            else:
                get_all_conf_list(dir_file_path, testing_conf_list)