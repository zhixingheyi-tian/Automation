#!/usr/bin/python
from utils.util import *
from utils.ssh import *
from core.jdk import *
from shutil import *

HADOOP_COMPONENT = "hadoop"

# Deploy Hadoop component
def deploy_hadoop_internal(custom_conf, master, slaves, beaver_env):
    format_clean_hadoop = beaver_env.get("format_clean_hadoop")
    if format_clean_hadoop == "TRUE":
        clean_hadoop(slaves, custom_conf)
    setup_env_dist(slaves, beaver_env, HADOOP_COMPONENT)
    set_path(HADOOP_COMPONENT, slaves, beaver_env.get("HADOOP_HOME"))
    generate_hosts(slaves)
    copy_packages(slaves, HADOOP_COMPONENT, beaver_env.get("HADOOP_VERSION"))
    update_copy_hadoop_conf(custom_conf, master, slaves, beaver_env)
    stop_firewall(slaves)

def clean_hadoop(slaves, custom_conf):
    name_dir = "/opt/Beaver/hadoop/data/nn"
    data_dir = "/opt/Beaver/hadoop/data/dn"
    hdfs_conf_file = ""

    output_hadoop_conf = update_conf(HADOOP_COMPONENT, custom_conf)
    for conf_file in [file for file in os.listdir(output_hadoop_conf) if fnmatch.fnmatch(file, 'hdfs-site.xml')]:
        hdfs_conf_file = os.path.join(output_hadoop_conf, conf_file)
        break
    if os.path.basename(hdfs_conf_file) == "hdfs-site.xml":
        name_dir = get_config_value(hdfs_conf_file, "dfs.namenode.name.dir").replace(",", " ")
        data_dir = get_config_value(hdfs_conf_file, "dfs.datanode.data.dir").replace(",", " ")

    for node in slaves:
        ssh_execute(node, "rm -rf /opt/Beaver/hadoop*")
        ssh_execute(node, "rm -rf " + name_dir)
        ssh_execute(node, "rm -rf " + data_dir)
        ssh_execute(node, "source ~/.bashrc")


def auto_hardware_config():
    print ("Get configs from hardware detect")

def display_hadoop_perf(project_path, hadoop_home):
    tools_path = os.path.join(project_path, "toos")
    print (colors.LIGHT_BLUE + "Test cluster performance, this will take a while..." + colors.ENDC)
    print (colors.LIGHT_GREEN + "Test hdfs io..." + colors.ENDC)
    cmd = ". " + os.path.join(tools_path, "dfstest.sh") + " " + hadoop_home
    os.system(cmd)
    print (colors.LIGHT_GREEN + "Test disk io..." + colors.ENDC)
    data_dir = "/opt/Beaver/hadoop/data"
    cmd = ". " + os.path.join(tools_path, "diskiotest.sh") + " " + data_dir
    os.system(cmd)

# Generate Hadoop slaves config file
def generate_slaves(slaves, conf_dir, run_datanode_on_master):
    with open(os.path.join(conf_dir, "slaves"), "w") as f:
        if len(slaves) == 1:
            for node in slaves:
                f.write(node.ip + "\n")
        else:
            if run_datanode_on_master == "TRUE":
                for node in slaves:
                    f.write(node.ip + "\n")
            else:
                for node in slaves:
                    if node.role != "master":
                        f.write(node.ip + "\n")

def generate_hosts(slaves):
    tmp_hosts = os.path.join(package_path, "tmp/hosts")
    subprocess.check_call("rm -f " + tmp_hosts, shell=True)
    subprocess.check_call("mkdir -p " + os.path.join(package_path, "tmp"), shell=True)
    print (colors.LIGHT_BLUE + "\tGenerate hosts file" + colors.ENDC)
    with open(tmp_hosts, "w+") as f:
        f.write("127.0.0.1 localhost\n")
        for node in slaves:
            f.write(node.ip + " " + node.hostname + "\n")

    for node in slaves:
        print (colors.LIGHT_BLUE + "\tCopy hosts file to " + node.hostname + "..." + colors.ENDC)
        ssh_execute(node, "find /etc/ -maxdepth 1 -name hosts|xargs -i mv {} /etc/hosts.old-" + str(time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime())))
        ssh_copy(node, tmp_hosts, "/etc/hosts")


# merge configuration file
def update_hadoop_conf(custom_conf, master, slaves, beaver_env):
    output_hadoop_conf = update_conf(HADOOP_COMPONENT, custom_conf)
    # generate slaves file
    generate_slaves(slaves, output_hadoop_conf, beaver_env.get("RUN_DATANODE_ON_MASTER"))
    # for all conf files, replace the related value, eg, replace master_hostname with real hostname
    for conf_file in [file for file in os.listdir(output_hadoop_conf) if fnmatch.fnmatch(file, '*.xml')]:
        output_conf_file = os.path.join(output_hadoop_conf, conf_file)
        dict = {'master_hostname':master.hostname}
        replace_conf_value(output_conf_file, dict)
        format_xml_file(output_conf_file)
    yarn_site_conf = os.path.join(output_hadoop_conf, "yarn-site.xml")
    if ET.parse(yarn_site_conf).getroot().find("property/[name='yarn.nodemanager.resource.memory-mb']").find("value").text == "{%yarn.nodemanager.resource.memory-mb%}":
        memory = calculate_memory(master)
        replace_name_value(yarn_site_conf, "yarn.nodemanager.resource.memory-mb", str(memory))
    if ET.parse(yarn_site_conf).getroot().find("property/[name='yarn.nodemanager.resource.cpu-vcores']").find("value").text == "{%yarn.nodemanager.resource.cpu-vcores%}":
        vcores = calculate_vcores(master)
        replace_name_value(yarn_site_conf, "yarn.nodemanager.resource.cpu-vcores", str(vcores))
    if ET.parse(yarn_site_conf).getroot().find("property/[name='yarn.scheduler.maximum-allocation-mb']").find("value").text == "{%yarn.scheduler.maximum-allocation-mb%}":
        memory = int(0.9 * calculate_memory(master))
        replace_name_value(yarn_site_conf, "yarn.scheduler.maximum-allocation-mb", str(memory))
    if (not ET.parse(yarn_site_conf).getroot().find("property/[name='yarn.scheduler.maximum-allocation-vcores']") == None) and (ET.parse(yarn_site_conf).getroot().find("property/[name='yarn.scheduler.maximum-allocation-vcores']").find("value").text == "{%yarn.scheduler.maximum-allocation-vcores%}"):
        memory = int(0.9 * calculate_vcores(master))
        replace_name_value(yarn_site_conf, "yarn.scheduler.maximum-allocation-vcores", str(memory))
    return output_hadoop_conf

def calculate_vcores(node):
    print(colors.LIGHT_BLUE + "\tCalculate vcore configurations into yarn-site.xml"+ colors.ENDC)
    cmd = "cat /proc/cpuinfo | grep \"processor\" | wc -l"
    stdout = ssh_execute_withReturn(node, cmd)
    vcores = int(stdout[0])
    print(colors.LIGHT_BLUE + "\tThe vcores number are " + str(vcores) + "." + colors.ENDC)
    return vcores

def calculate_memory(node):
    print(colors.LIGHT_BLUE + "\tCalculate memory configurations into yarn-site.xml"+ colors.ENDC)
    cmd = "cat /proc/meminfo | grep \"MemTotal\""
    stdout = ssh_execute_withReturn(node, cmd)
    for line in stdout:
        memory = int(int(line.split()[1]) / 1024 * 0.85)
    print(colors.LIGHT_BLUE + "\tThe memory is " + str(memory) + "mb." + colors.ENDC)
    return memory

def copy_hadoop_conf(default_hadoop_conf, beaver_custom_conf):
    os.system("cp -r " + default_hadoop_conf + "/hadoop " + beaver_custom_conf)

# Stop hadoop related services
def stop_hadoop_service(master, slaves):
    print (colors.LIGHT_BLUE + "Stop hadoop related services, it may take a while..." + colors.ENDC)
    process_list = ["NameNode", "DataNode", "SecondaryNameNode", "NodeManager", "ResourceManager", "WebAppProxyServer", "JobHistoryServer"]
    ssh_execute(master, "$HADOOP_HOME/sbin/stop-all.sh")
    for node in slaves:
        ssh_execute(node, "$HADOOP_HOME/sbin/mr-jobhistory-daemon.sh stop historyserver")
        stdout = ssh_execute_withReturn(node, "jps")
        process_dict = {}
        for line in stdout:
            k, v = line.partition(" ")[::2]
            process_dict[v.strip()] = k.strip()
        for process in process_list:
            if process in process_dict:
                ssh_execute(node, "kill -9 " + process_dict.get(process))
        del process_dict
    #chech if the port has been occupied
    ssh_execute(master,
                "netstat -lnp | grep 9000  | awk -F ' ' '{print $7}' | awk -F '/' '{print $1}' | xargs -r kill -9; "
                "netstat -lnp | grep 50070 | awk -F ' ' '{print $7}' | awk -F '/' '{print $1}' | xargs -r kill -9; "
                "netstat -lnp | grep 50090 | awk -F ' ' '{print $7}' | awk -F '/' '{print $1}' | xargs -r kill -9; "
                "netstat -lnp | grep 8030  | awk -F ' ' '{print $7}' | awk -F '/' '{print $1}' | xargs -r kill -9; "
                "netstat -lnp | grep 8031  | awk -F ' ' '{print $7}' | awk -F '/' '{print $1}' | xargs -r kill -9; "
                "netstat -lnp | grep 8032  | awk -F ' ' '{print $7}' | awk -F '/' '{print $1}' | xargs -r kill -9; "
                "netstat -lnp | grep 8033  | awk -F ' ' '{print $7}' | awk -F '/' '{print $1}' | xargs -r kill -9; "
                "netstat -lnp | grep 8088  | awk -F ' ' '{print $7}' | awk -F '/' '{print $1}' | xargs -r kill -9; "
                "netstat -lnp | grep 10020 | awk -F ' ' '{print $7}' | awk -F '/' '{print $1}' | xargs -r kill -9; "
                "netstat -lnp | grep 19888 | awk -F ' ' '{print $7}' | awk -F '/' '{print $1}' | xargs -r kill -9; "
                "netstat -lnp | grep 10033 | awk -F ' ' '{print $7}' | awk -F '/' '{print $1}' | xargs -r kill -9")
    for node in slaves:
        if node.role == "master":
            continue
        ssh_execute(node,
                    "netstat -lnp | grep 50010 | awk -F ' ' '{print $7}' | awk -F '/' '{print $1}' | xargs -r kill -9; "
                    "netstat -lnp | grep 50020 | awk -F ' ' '{print $7}' | awk -F '/' '{print $1}' | xargs -r kill -9; "
                    "netstat -lnp | grep 50075 | awk -F ' ' '{print $7}' | awk -F '/' '{print $1}' | xargs -r kill -9; "
                    "netstat -lnp | grep 8040  | awk -F ' ' '{print $7}' | awk -F '/' '{print $1}' | xargs -r kill -9; "
                    "netstat -lnp | grep 8042  | awk -F ' ' '{print $7}' | awk -F '/' '{print $1}' | xargs -r kill -9")


# Start Hadoop related services
def start_hadoop_service(master, slaves, beaver_env):
    hadoop_home = beaver_env.get("HADOOP_HOME")
    stop_hadoop_service(master, slaves)
    print (colors.LIGHT_BLUE + "Start hadoop related services,  it may take a while..." + colors.ENDC)
    ssh_execute(master, hadoop_home + "/sbin/start-all.sh")
    ssh_execute(master, hadoop_home + "/sbin/yarn-daemon.sh start proxyserver")
    ssh_execute(master, hadoop_home + "/sbin/mr-jobhistory-daemon.sh start historyserver")

def clean_yarn_dir(master, slaves, custom_conf, beaver_env):
    output_hadoop_conf = update_hadoop_conf(custom_conf, master, slaves, beaver_env)
    yarn_site_conf = os.path.join(output_hadoop_conf, "yarn-site.xml")
    yarn_tmp_dirs = ET.parse(yarn_site_conf).getroot().find("property/[name='yarn.nodemanager.local-dirs']").find("value").text.strip().split(",")
    for node in slaves:
        if node.role == "master":
            continue
        ssh_execute(node, "mkdir -p /tmp/empty_dir")
        for dir in yarn_tmp_dirs:
            ssh_execute(node, "cd " + dir + " && rsync -a --delete /tmp/empty_dir/ .")

def update_copy_hadoop_conf(custom_conf, master, slaves, beaver_env):
    output_hadoop_conf = update_hadoop_conf(custom_conf, master, slaves, beaver_env)
    copy_configurations(slaves, output_hadoop_conf, HADOOP_COMPONENT, beaver_env.get("HADOOP_HOME"))

def hdfs_format(master, hadoop_home):
    print (colors.LIGHT_BLUE + "format hdfs" + colors.ENDC)
    ssh_execute(master, "yes | " + hadoop_home + "/bin/hdfs namenode -format")

def deploy_hadoop(custom_conf, master, slaves, beaver_env):
    deploy_jdk(slaves, beaver_env)
    stop_hadoop_service(master, slaves)
    deploy_hadoop_internal(custom_conf, master, slaves, beaver_env)
    format_clean_hadoop = beaver_env.get("format_clean_hadoop")
    if format_clean_hadoop == "TRUE":
        hdfs_format(master, beaver_env.get("HADOOP_HOME"))

def deploy_start_hadoop(custom_conf, master, slaves, beaver_env):
    format_clean_hadoop = beaver_env.get("format_clean_hadoop")
    deploy_jdk(slaves, beaver_env)
    stop_hadoop_service(master, slaves)
    deploy_hadoop_internal(custom_conf, master, slaves, beaver_env)
    if format_clean_hadoop == "TRUE":
        hdfs_format(master, beaver_env.get("HADOOP_HOME"))
    start_hadoop_service(master, slaves, beaver_env)

def undeploy_hadoop(master, slaves, custom_conf, beaver_env):
    format_clean_hadoop = beaver_env.get("format_clean_hadoop")
    stop_hadoop_service(master, slaves)
    if format_clean_hadoop == "TRUE":
        print (colors.LIGHT_BLUE + "\nStart to remove hadoop from the nodes" + colors.ENDC)
        clean_hadoop(slaves, custom_conf)

'''
if __name__ == '__main__':
    parser = optparse.OptionParser()
    parser.add_option('--conf',
                      dest='conf_dir',
                      default="")
    parser.add_option('--action',
                      dest='action')

    options, remainder = parser.parse_args()

    custom_conf = options.conf_dir
    action = options.action

    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)

    if action == "deploy":
        deploy_hadoop(custom_conf, slaves)
    elif action == "undeploy":
        clean_hadoop(slaves)
    elif action == "start":
        stop_hadoop_service(master, slaves)
        start_hadoop_service(master)
    elif action == "stop":
        stop_hadoop_service(master, slaves)
    elif action == "format":
        hdfs_format(master)
    else:
        print ("Not support")
'''
