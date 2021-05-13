#!/usr/bin/python

import errno
from core.hadoop import *
from utils.util import *
from utils.git_cmd import *
from xml.dom import minidom
from shutil import copyfile
from utils.node import *
from core.oap import *
from core.maven import *

SPARK_COMPONENT = "spark"

def spark_compile_prepare(master, beaver_env, spark_repo):
    #if this machine is not master, then will use deploy_local_maven function to deploy maven.
    deploy_local_maven(master, beaver_env)
    deploy_maven(master, beaver_env)
    deploy_local_jdk(master, beaver_env)
    deploy_jdk([master], beaver_env)
    # Determine whether to clone source code or not
    if os.path.exists(spark_source_code_path):
        remote_spark_url = subprocess.check_output("cd " + spark_source_code_path + " && git remote -v | grep push | awk '{print $2}' ", shell=True).strip('\r\n')
        if remote_spark_url == spark_repo:
            try:
                subprocess.check_call("cd " + spark_source_code_path + " && git checkout master && git pull", shell=True)
            except subprocess.CalledProcessError, e:
                print e
                subprocess.check_call("rm -rf " + spark_source_code_path, shell=True)
                spark_git_clone(spark_repo, spark_source_code_path)
        else:
            subprocess.check_call("rm -rf " + spark_source_code_path, shell=True)
            spark_git_clone(spark_repo, spark_source_code_path)
    else:
        spark_git_clone(spark_repo, spark_source_code_path)

    spark_checkout_cmd = beaver_env.get("SPARK_BRANCH")
    if spark_checkout_cmd != "" and spark_checkout_cmd != None:
        spark_checkout(spark_checkout_cmd)
        try:
            subprocess.check_call("cd " + spark_source_code_path + " && git pull", shell=True)
        except subprocess.CalledProcessError:
            print (colors.RED + beaver_env.get("SPARK_BRANCH") + " is a tag not a branch" + colors.ENDC)

    # create build and tmp path for caching file
    build_path = os.path.join(package_path, "build")
    tmp_path = os.path.join(package_path, "tmp")
    if not os.path.exists(build_path):
        subprocess.check_call("mkdir " + build_path, shell=True)
    if not os.path.exists(tmp_path):
        subprocess.check_call("mkdir " + tmp_path, shell=True)

    spark_precompile_pkg_download(spark_source_code_path)
    spark_precompile_set_proxy(spark_source_code_path)
    #to shutdown zinc when first time compile
    try:
        nailgun_id = subprocess.check_output("source ~/.bashrc && jps | grep Nailgun", shell = True).strip('\r\n').split()[0]
        if nailgun_id:
            subprocess.check_call("kill " + nailgun_id, shell=True)
    except subprocess.CalledProcessError, e:
        print e

    #if beaver_env.get("OAP_ENABLE") == "TRUE":
    #    oap_compile_prepare(master, beaver_env)
    #    oap_perf_compile_prepare(master, beaver_env)

def deploy_spark_internal(custom_conf, master, slaves, beaver_env):
    clean_spark(master)
    #spark_verion = beaver_env.get("SPARK_VERSION")
    setup_env_dist([master], beaver_env, SPARK_COMPONENT)
    #copy_packages([master], SPARK_COMPONENT, spark_verion)
    deploy_compiled_spark_and_update_shuffle(custom_conf, master, slaves, beaver_env)
    update_copy_spark_conf(master, slaves, custom_conf, beaver_env)


def clean_spark(master):
    ssh_execute(master, "rm -rf /opt/Beaver/spark-[0-9]*[0-9]")
    ssh_execute(master, "rm -rf /opt/Beaver/spark")

# Calculate statistics of hardware information
def calculate_hardware(master):
    list = []
    cmd = "cat /proc/cpuinfo | grep \"processor\" | wc -l"
    stdout = ssh_execute_withReturn(master, cmd)
    for line in stdout:
        vcore_num = int(line)
        list.append(vcore_num)
    cmd = "cat /proc/meminfo | grep \"MemTotal\""
    stdout = ssh_execute_withReturn(master, cmd)
    for line in stdout:
        memory = int(int(line.split()[1]) / 1024 * 0.85)
        list.append(memory)
    return list

def create_related_hdfs_dir(spark_output_conf, master, slaves, beaver_env):
    spark_default_path = os.path.join(spark_output_conf, "spark-defaults.conf")
    spark_eventLog = ""
    spark_history = ""
    with open(spark_default_path) as f:
        for line in f:
            if not line.startswith('#') and line.split():
                line = line.split()
                if line[0] == "spark.eventLog.dir":
                    spark_eventLog = line[1]
                if line[0] == "spark.history.fs.logDirectory":
                    spark_history = line[1]
    if spark_eventLog is not None or spark_history is not None:
        start_hadoop_service(master, slaves, beaver_env)
        hadoop_home = beaver_env.get("HADOOP_HOME")
        print (colors.LIGHT_BLUE + "\nCreate spark eventlog HDFS directory: Waiting for safe mode" + colors.ENDC)
        ssh_execute(master, hadoop_home + "bin/hadoop dfsadmin -safemode wait")
        if spark_eventLog is not None:
            print (colors.LIGHT_BLUE+ "\nCreate spark eventlog HDFS directory" + colors.ENDC)
            ssh_execute(master, hadoop_home + "/bin/hadoop fs -mkdir -p " + spark_eventLog)
        if spark_history is not None:
            ssh_execute(master, hadoop_home + "/bin/hadoop fs -mkdir -p " + spark_history)
        stop_hadoop_service(master, slaves)

# Start Spark history server
def start_spark_history_server(master, beaver_env):
    stop_spark_history_server(master)
    print (colors.LIGHT_BLUE + "Start spark history server" + colors.ENDC)
    ssh_execute(master, beaver_env.get("SPARK_HOME") + "/sbin/start-history-server.sh")


def stop_spark_history_server(master):
    print (colors.LIGHT_BLUE + "Stop Spark history-server service..." + colors.ENDC)
    ssh_execute(master, "ps aux | grep 'HistoryServer' | grep 'org.apache.spark.deploy.history.HistoryServer' | awk '{print $2}' | xargs -r kill -9")

def stop_spark_thrift_server(master, beaver_env):
    print (colors.LIGHT_BLUE + "Stop Spark thrift-server service..." + colors.ENDC)
    ssh_execute(master, beaver_env.get("SPARK_HOME") + "/sbin/stop-thriftserver.sh")
    ssh_execute(master, "netstat -lnp | grep 10000 | awk -F ' ' '{print $7}' | awk -F '/' '{print $1}' | xargs -r kill -9")

def deploy_spark(custom_conf, master, slaves, beaver_env):
    stop_spark_service(master)
    deploy_spark_internal(custom_conf, master, slaves, beaver_env)


def undeploy_spark(master):
    stop_spark_history_server(master)
    clean_spark(master)


def stop_spark_service(master):
    stop_spark_history_server(master)


def start_spark_service(master, beaver_env):
    start_spark_history_server(master, beaver_env)


def update_copy_spark_conf(master, slaves, custom_conf, beaver_env):
    spark_output_conf = update_conf(SPARK_COMPONENT, custom_conf)
    for conf_file in [file for file in os.listdir(spark_output_conf) if file.endswith(('.conf', '.xml'))]:
        output_conf_file = os.path.join(spark_output_conf, conf_file)
        spark_version = beaver_env.get("SPARK_VERSION")
        dict = get_spark_replace_dict(master, slaves, beaver_env, spark_version)
        replace_conf_value(output_conf_file, dict)
    copy_configurations([master], spark_output_conf, SPARK_COMPONENT, beaver_env.get("SPARK_HOME"))
    # In order to run beeline with thrift server with need hive_site to save database metadata
    copy_hive_conf_to_spark(master, custom_conf, beaver_env)
    create_related_hdfs_dir(spark_output_conf, master, slaves, beaver_env)

def copy_hive_conf_to_spark(master, custom_conf, beaver_env):
    hive_conf_file = os.path.join(custom_conf, "output/hive/hive-site.xml")
    spark_conf_folder = os.path.join(beaver_env.get("SPARK_HOME"), "conf")
    if os.path.isfile(hive_conf_file):
        print(colors.LIGHT_BLUE + "Start to copy hive-site.xml to spark config folder" + colors.ENDC)
        ssh_copy(master, hive_conf_file, os.path.join(spark_conf_folder, "hive-site.xml"))

def get_spark_replace_dict(master, slaves, beaver_env, spark_version):
    print("Calculate vcore and memory configurations into spark-defaults.conf")
    hardware_config_list = calculate_hardware(master)
    node_num = len(slaves)
    total_cores = int(hardware_config_list[0]) * node_num
    total_memory = hardware_config_list[1] * node_num
    executor_cores = 4
    instances = int(total_cores / executor_cores)
    executor_memory = str(int(total_memory / instances / 1024 * 0.8))
    executor_memoryOverhead = str(int(total_memory / instances * 0.2))
    executor_memoryoffheap = str(int((total_memory/ (1024 * 1024)) * 0.18))
    dict = {'master_hostname':master.hostname,
            '{%spark.memory.offHeap.size%}': str(executor_memoryoffheap),
            '{%spark.executor.cores%}':str(executor_cores), '{%spark.executor.instances%}':str(instances),
            '{%spark.executor.memory%}':executor_memory, '{%spark.yarn.executor.memoryOverhead%}':executor_memoryOverhead}
    return dict

def spark_git_clone(git_repo, dst):
    subprocess.check_call("mkdir -p " + dst, shell=True)
    git_clone(git_repo, dst)

def spark_precompile_pkg_download(spark_source_path):
    spark_mvn_path = os.path.join(spark_source_path, "build")
    tmp_path = os.path.join(project_path, "package/tmp")
    try:
        subprocess.check_call("rm " + os.path.join(tmp_path, "zinc-0.3.15.tgz"), shell=True)
        subprocess.check_call("rm " + os.path.join(tmp_path, "scala-" + spark_get_build_scala_version() + ".tgz"), shell=True)
        subprocess.check_call("rm " + os.path.join(tmp_path, "apache-maven-" + spark_get_build_maven_version() + "-bin.tar.gz"), shell=True)
    except subprocess.CalledProcessError, e:
        print e
    #download zinc
    try:
        subprocess.check_call("wget 'http://" + download_server + "/spark/buildenv/zinc-0.3.15.tgz' -P " + tmp_path, shell=True)
    except subprocess.CalledProcessError:
        subprocess.check_call("wget 'https://downloads.typesafe.com/zinc/0.3.15/zinc-0.3.15.tgz' -P " + tmp_path, shell=True)
    subprocess.check_call("tar -C " + spark_mvn_path +" -xvf " + os.path.join(tmp_path, "zinc-0.3.15.tgz"), shell=True)
    #download scala
    try:
        subprocess.check_call("wget 'http://" + download_server + "/spark/buildenv/scala-" + spark_get_build_scala_version() + ".tgz' -P " + tmp_path, shell=True)
    except subprocess.CalledProcessError:
        subprocess.check_call("wget 'https://downloads.typesafe.com/scala/" + spark_get_build_scala_version() + "/scala-" + spark_get_build_scala_version() + ".tgz' -P " + tmp_path, shell=True)
    subprocess.check_call("tar -C " + spark_mvn_path +" -xvf " + os.path.join(tmp_path, "scala-" + spark_get_build_scala_version() + ".tgz") , shell=True)
    #download spark buildin maven
    try:
        subprocess.check_call("wget http://" + download_server + "/spark/buildenv/apache-maven-" + spark_get_build_maven_version() + "-bin.tar.gz -O " + tmp_path + "/apache-maven-" + spark_get_build_maven_version() + '-bin.tar.gz', shell=True)
    except subprocess.CalledProcessError:
        subprocess.check_call('wget "https://www.apache.org/dyn/closer.lua?action=download&filename=/maven/maven-3/' + spark_get_build_maven_version() + '/binaries/apache-maven-' + spark_get_build_maven_version() + '-bin.tar.gz" -O ' + tmp_path + '/apache-maven-' + spark_get_build_maven_version() + '-bin.tar.gz', shell=True)
    subprocess.check_call("tar -C " + spark_mvn_path +" -xvf " + os.path.join(tmp_path, "apache-maven-" + spark_get_build_maven_version() + "-bin.tar.gz"), shell=True)
    #remove tempory file
    subprocess.check_call("rm " + os.path.join(tmp_path, "zinc-0.3.15.tgz"), shell = True)
    subprocess.check_call("rm " + os.path.join(tmp_path, "scala-" + spark_get_build_scala_version() + ".tgz"), shell=True)
    subprocess.check_call("rm " + os.path.join(tmp_path, "apache-maven-" + spark_get_build_maven_version() + "-bin.tar.gz"), shell=True)

def spark_precompile_set_proxy(spark_source_path):
    is_proxy_set = False
    output = subprocess.check_output("set | grep http_proxy", shell = True)
    for element in output.split("\n"):
        if element.split("=")[0] == "http_proxy" and element.split("=")[1] != "" :
            print(colors.LIGHT_BLUE + "Find you have set the proxy" + colors.ENDC)
            is_proxy_set = True

    if is_proxy_set:
        print(colors.LIGHT_BLUE + "Setting proxy for Maven" + colors.ENDC)
        maven_config_path = os.path.join(spark_source_path, "build/apache-maven-" + spark_get_build_maven_version() + "/conf")
        if not os.path.exists(os.path.join(maven_config_path, "settings.xml.template")):
            copyfile(os.path.join(maven_config_path, "settings.xml"), os.path.join(maven_config_path, "settings.xml.template"))

        proxy_ET = ET
        proxy_ET.register_namespace("", "http://maven.apache.org/SETTINGS/1.0.0")
        proxy_ET.register_namespace("xsi", "http://www.w3.org/2001/XMLSchema-instance")
        setting_tree = proxy_ET.parse(os.path.join(maven_config_path, 'settings.xml.template'))
        maven_config_root = setting_tree.getroot()
        proxies_element = maven_config_root.find('{http://maven.apache.org/SETTINGS/1.0.0}proxies')

        proxy_element1 = proxy_ET.Element('proxy')
        proxy_element1_active = proxy_ET.SubElement(proxy_element1, 'active')
        proxy_element1_protocol = proxy_ET.SubElement(proxy_element1, 'protocol')
        proxy_element1_username = proxy_ET.SubElement(proxy_element1, 'username')
        proxy_element1_password = proxy_ET.SubElement(proxy_element1, 'password')
        proxy_element1_host = proxy_ET.SubElement(proxy_element1, 'host')
        proxy_element1_port = proxy_ET.SubElement(proxy_element1, 'port')
        proxy_element1_nonProxyHosts = proxy_ET.SubElement(proxy_element1, 'id')

        proxy_element1_active.text = 'true'
        proxy_element1_protocol.text = 'http'
        proxy_element1_host.text = 'child-prc.intel.com'
        proxy_element1_port.text = '913'
        proxies_element.append(proxy_element1)

        proxy_element2 = proxy_ET.Element('proxy')
        proxy_element2_active = proxy_ET.SubElement(proxy_element2, 'active')
        proxy_element2_protocol = proxy_ET.SubElement(proxy_element2, 'protocol')
        proxy_element2_username = proxy_ET.SubElement(proxy_element2, 'username')
        proxy_element2_password = proxy_ET.SubElement(proxy_element2, 'password')
        proxy_element2_host = proxy_ET.SubElement(proxy_element2, 'host')
        proxy_element2_port = proxy_ET.SubElement(proxy_element2, 'port')
        proxy_element2_nonProxyHosts = proxy_ET.SubElement(proxy_element2, 'id')

        proxy_element2_active.text = 'true'
        proxy_element2_protocol.text = 'https'
        proxy_element2_host.text = 'child-prc.intel.com'
        proxy_element2_port.text = '913'
        proxies_element.append(proxy_element2)

        with open(os.path.join(maven_config_path, "settings.xml"), "w") as f:
            f.write(minidom.parseString(proxy_ET.tostring(maven_config_root)).toprettyxml(indent="  "))


def spark_get_build_name(cmd):
    cmd_array = cmd.split()
    cmd_name = ""
    cmd_ext = ".tgz"
    if "--name" in cmd_array:
        cmd_name = cmd_array[cmd_array.index("--name") + 1]
    return "spark-" + spark_get_build_version() + "-bin-" + cmd_name + cmd_ext

def spark_compile(custom_conf, beaver_env, spark_repo):
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    spark_compile_prepare(master, beaver_env, spark_repo)
    compile_cmd = retrieve_spark_compile_cmd(custom_conf)
    # if beaver_env.get("OAP_with_DCPMM").lower() == "true":
    #     spark_enable_numa(beaver_env)
    spark_compile_with_cmd(beaver_env, compile_cmd)

def spark_enable_numa(beaver_env):
    spark_enable_numa_patch = "Spark." + beaver_env.get("SPARK_VERSION") + ".numa.patch"
    if os.path.isfile(oap_source_code_path + "/docs/" + spark_enable_numa_patch):
        print(colors.LIGHT_BLUE + "Apply the patch " + spark_enable_numa_patch + " to Spark" + colors.ENDC)
        subprocess.check_call("cd " + spark_source_code_path + " && git apply " + oap_source_code_path + "/docs/" + spark_enable_numa_patch , shell=True)
    elif os.path.isfile(oap_source_code_path + "/oap-cache/oap/docs/" + spark_enable_numa_patch):
        print(colors.LIGHT_BLUE + "Apply the patch " + spark_enable_numa_patch + " to Spark" + colors.ENDC)
        subprocess.check_call("cd " + spark_source_code_path + " && git apply " + oap_source_code_path + "/oap-cache/oap/docs/" + spark_enable_numa_patch, shell=True)

def spark_checkout(checkout_cmd):
    spark_git_run("checkout " + checkout_cmd)

def spark_compile_with_cmd(beaver_env, cmd=""):
    spark_source_path = spark_source_code_path
    print(colors.LIGHT_BLUE + "Start to compile Spark with command: " + cmd + colors.ENDC)
    subprocess.check_call("cd " + spark_source_path + " && ./dev/make-distribution.sh " + cmd, shell = True)
    dst_pkg_name = SPARK_COMPONENT + "-" + spark_get_build_version() + '.' + spark_get_build_name(cmd).split(".")[-1]
    print(colors.LIGHT_BLUE+ "Copy build file "+ spark_get_build_name(cmd) + " to ./pakage/build folder and rename it to " + dst_pkg_name + colors.ENDC)
    copyfile(os.path.join(spark_source_path, spark_get_build_name(cmd)), os.path.join(project_path, "package/build/" + dst_pkg_name))

def spark_git_run(cmd):
    cmd = cmd.strip()
    git_command(cmd, spark_source_code_path)

def deploy_package_spark(custom_conf, master, slaves, beaver_env):
    clean_spark(master)
    spark_version = beaver_env.get("SPARK_VERSION")
    setup_env_dist([master], beaver_env, SPARK_COMPONENT)
    #dst_pkg_name = SPARK_COMPONENT + "-" + spark_version + '.' + spark_get_build_name(retrieve_spark_compile_cmd(custom_conf)).split(".")[-1]

    package = SPARK_COMPONENT + "-" + spark_version + ".tgz"
    if not os.path.isfile(os.path.join(build_path, package)):
        print (colors.LIGHT_BLUE + "\tCannot find " + package + ", build " + package + " for you..." + colors.ENDC)
        spark_compile(custom_conf, beaver_env, beaver_env.get("SPARK_GIT_REPO"))
    else:
        print(colors.LIGHT_GREEN + "\t" + package + " has already exists in Beaver build path" + colors.ENDC)
    copy_package_dist([master], os.path.join(build_path, package), SPARK_COMPONENT, spark_version)
    update_copy_spark_conf(master, slaves, custom_conf, beaver_env)

def deploy_compiled_spark_and_update_shuffle(custom_conf, master, slaves, beaver_env):
    deploy_package_spark(custom_conf, master, slaves, beaver_env)
    copy_spark_AE_shuffle_from_source(slaves, beaver_env)
    update_AE_yarn_classpath(master, slaves, beaver_env)
    #start_hadoop_service(master, slaves, beaver_env)

def copy_spark_AE_shuffle_from_source(slaves, beaver_env): #this is used to copy shuffle jar file from source repo
    file_list = []
    file_list.append('spark-[0-9]*[0-9]-yarn-shuffle.jar')  # spark-2.3.2-yarn-shuffle.jar
    file_list.append('spark-network-common_[0-9]*[0-9].jar')  # spark-network-common_2.11-2.3.0.jar
    file_list.append('spark-network-shuffle_[0-9]*[0-9].jar')  # spark-network-shuffle_2.11-2.3.0.jar
    file_list.append('jackson-databind-[0-9]*[0-9].jar')  # jackson-databind-2.6.7.1.jar
    file_list.append('jackson-core-[0-9]*[0-9].jar')  # jackson-core-2.6.7.jar
    file_list.append('jackson-annotations-[0-9]*[0-9].jar')  # jackson-annotations-2.6.7.jar
    file_list.append('metrics-core-[0-9]*[0-9].jar')  # metrics-core-3.1.5.jar
    file_list.append('netty-all-[0-9]*[0-9].Final.jar')  # netty-all-4.1.17.Final.jar
    file_list.append('commons-lang3-[0-9]*[0-9].jar')  # commons-lang3-3.5.jar

    dst_path = os.path.join(package_path, "tmp/spark-lib")
    tmp_path = os.path.join(package_path, "tmp")
    if os.path.exists(dst_path):
        subprocess.check_call('rm -rf ' + dst_path, shell=True)
    subprocess.check_call('mkdir ' + dst_path, shell=True)

    for f in file_list:
        src_file = os.path.join(beaver_env.get("SPARK_HOME"), 'jars/')
        subprocess.check_call("find \"" + src_file + "\" -name " + f + "| xargs -i cp {} " +  dst_path, shell=True)

    for f in file_list:
        src_file = os.path.join(beaver_env.get("SPARK_HOME"), 'yarn')
        subprocess.check_call("find \"" + src_file + "\" -name " + f + "| xargs -i cp {} \"" +  dst_path + "\"", shell=True)

    subprocess.check_call("cd " + tmp_path + ";tar -zcvf spark-lib.tar.gz spark-lib", shell=True)
    subprocess.check_call("cd " + tmp_path + ";rm -rf spark-lib", shell=True)

    node_dst_addr = os.path.join(beaver_env.get("HADOOP_HOME"), "share/hadoop/yarn/lib")
    for node in slaves:
        print (colors.LIGHT_BLUE + "\tCopy spark-shuffle jar to " + node.hostname + "..." + colors.ENDC)
        ssh_execute(node, "find \"" + node_dst_addr + "\" -name netty-[0-9]*[0-9].Final.jar| xargs -i mv -f {} {}.old")
        ssh_copy(node, os.path.join(tmp_path, "spark-lib.tar.gz"), os.path.join(node_dst_addr, "spark-lib.tar.gz"))
        ssh_execute(node, "cd " + node_dst_addr + ";tar zxf spark-lib.tar.gz  --strip-components=1;rm -f spark-lib.tar.gz")


def update_AE_yarn_classpath(master, slaves, beaver_env):
    source_path = os.path.join(beaver_env.get("HADOOP_HOME"), "libexec/hadoop-config.sh")
    tmp_path = os.path.join(package_path, "tmp")
    tmp_conf_file = os.path.join(tmp_path, "hadoop-config.sh")
    new_path = os.path.join(tmp_path, "hadoop_config_for_AE.sh")
    ssh_copy_from_remote(master, source_path, tmp_conf_file)
    with open(tmp_conf_file, "r") as oldf:
        with open(new_path, "w+") as newf:
            for line in oldf:
                if line == "  CLASSPATH=${CLASSPATH}:$HADOOP_YARN_HOME/$YARN_LIB_JARS_DIR'/*'\n":
                    newf.write("  CLASSPATH=$HADOOP_YARN_HOME/$YARN_LIB_JARS_DIR'/*':${CLASSPATH}\n")
                else:
                    newf.write(line)

    for node in slaves:
        print (colors.LIGHT_BLUE + "\tCopy hadoop_config.sh to " + node.hostname + "..." + colors.ENDC)
        ssh_execute(node, "find \"" + os.path.join(beaver_env.get("HADOOP_HOME"), "libexec") + "\" -maxdepth 1 -name hadoop-config.sh""|xargs -i mv {} "
                    + os.path.join(beaver_env.get("HADOOP_HOME"), "libexec/hadoop-config.sh.old-" + str(time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime()))))
        ssh_copy(node, new_path, source_path)


'''
if __name__ == '__main__':
    pars
    parser.add_option('-v', '--version',
                      dest='version',
                      default="2.7.3")
    parser.add_option('--conf',
                      dest='conf_dir',
                      default="")
    parser.add_option('--script',
                      dest='script',
                      default="")
    parser.add_option('--action',
                      dest='action')

    options, remainder = parser.parse_args()

    version = options.version
    conf_dir = options.conf_dir
    script = options.script
    action = options.action

    version = spark_env.get("SPARK_VERSION")

    if action == "update_conf":
        update_conf(SPARK_COMPONENT, conf_dir)
    elif action == "deploy":
        deploy_spark(version)
    elif action == "undeploy":
        undeploy_spark(version)
    elif action == "stop":
        stop_history_server()
    elif action == "start":
        stop_history_server()
        start_spark_history()
    else:
        print("not support")
'''

if __name__ == '__main__':
    args = sys.argv
    if len(args) < 3:
        print ("Please input: [action] [repo_dir]")
    action = args[1]
    custom_conf = args[2]
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(custom_conf)

    if action == "update":
        update_conf(SPARK_COMPONENT, custom_conf)
    elif action == "deploy":
        deploy_spark(custom_conf, master, slaves, beaver_env)
    elif action == "undeploy":
        undeploy_spark(master)
    elif action == "stop":
        stop_spark_service(master)
    elif action == "start":
        start_spark_history_server(master, beaver_env)
    else:
        print("not support")