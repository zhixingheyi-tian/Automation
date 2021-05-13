from core.spark import *

FLINK_COMPONENT = "flink"


def deploy_flink(custom_conf, master, slaves, beaver_env):
    deploy_flink_internal(custom_conf, master, slaves, beaver_env)

def update_copy_flink_conf(custom_conf, master, slaves, beaver_env):
    flink_output_conf = update_conf(FLINK_COMPONENT, custom_conf)
    for conf_file in [file for file in os.listdir(flink_output_conf) if file.endswith(('yaml','masters'))]:
        output_conf_file = os.path.join(flink_output_conf, conf_file)
        dict = get_flink_replace_dict(master, slaves)
        replace_conf_value(output_conf_file, dict)
    for conf_file in [file for file in os.listdir(flink_output_conf) if file.endswith(('slaves'))]:
        output_conf_file = os.path.join(flink_output_conf, conf_file)
        with open(output_conf_file, "w") as f:
            for node in slaves:
                if(node.hostname != master.hostname):
                    f.write(node.hostname+"\n")
    copy_configurations([master], flink_output_conf, FLINK_COMPONENT, beaver_env.get("FLINK_HOME"))


def get_flink_replace_dict(master, slaves):
    print("Calculate vcore and memory configurations into flink-conf.yaml")
    hardware_config_list = calculate_hardware(master)
    node_num = len(slaves)
    total_memory = hardware_config_list[1] * node_num
    numberOfTaskSlots = int(hardware_config_list[0])
    taskmanager_heap_size =  int(total_memory/len(slaves)*0.8)
    dict = {'master_hostname': master.hostname,'{%taskmanager.numberOfTaskSlots%}': str(numberOfTaskSlots), '{%taskmanager_heap_size%}': str(taskmanager_heap_size) + 'm'}
    return dict

def deploy_flink_internal(custom_conf, master, slaves, beaver_env):
    stop_flink_service(master)
    clean_flink([master])
    setup_env_dist([master], beaver_env, FLINK_COMPONENT)
    set_path(FLINK_COMPONENT, [master], beaver_env.get("FLINK_HOME"))
    copy_flink_package([master], FLINK_COMPONENT, beaver_env.get("FLINK_PACKAGE"), beaver_env.get("FLINK_VERSION"))
    update_copy_flink_conf(custom_conf, master, slaves, beaver_env)

def copy_flink_package(nodes, component, package, version):
    print (colors.LIGHT_BLUE + "Distribute " + "tar.gz file" + " for " + component + colors.ENDC)
    download_url = "http://archive.apache.org/dist/" + component + "/" + component + "-" + version
    if os.path.isfile(os.path.join(package_path + "/build", package)):
        print (colors.LIGHT_GREEN + "\t" + package + " has already exists in Beaver build tree" + colors.ENDC)
        copyfile(os.path.join(package_path + "/build", package), os.path.join(package_path, package))
    elif os.path.isfile(os.path.join(package_path, package)):
        print (colors.LIGHT_GREEN + "\t" + package + " has already exists in Beaver package" + colors.ENDC)
    else:
        print (colors.LIGHT_BLUE + "\tDownloading " + package + " from website..." + colors.ENDC)
        os.system("wget -P " + package_path + " " + download_url + "/" + package)
    copy_package_dist(nodes, os.path.join(package_path, package), component, version)

def clean_flink(slaves):
    for node in slaves:
        print(colors.LIGHT_BLUE + "clean flink on " + node.hostname + colors.ENDC)
        ssh_execute(node, "rm -rf /opt/Beaver/flink*")
        ssh_execute(node, "source ~/.bashrc")

def start_flink_service(master,beaver_env):
    flink_home = beaver_env.get("FLINK_HOME")
    stop_flink_service(master)
    print (colors.LIGHT_BLUE + "Start flink on yarn,  it may take a while..." + colors.ENDC)
    ssh_execute(master, flink_home + "/bin/yarn-session.sh")

def stop_flink_service(master):
    service = ssh_execute_withReturn(master, "jps | grep ResourceManager")
    if service == []:
        print(colors.LIGHT_BLUE + "No flink services need to stop!" + colors.ENDC)
        return
    apps = ssh_execute_withReturn(master, "yarn application -list | grep 'Flink session cluster' | awk -F '\t' '{print $1}'")
    for app in apps:
        ssh_execute(master,"yarn application -kill " + app)

def flink_compile(custom_conf, beaver_env, flink_repo, checkout_cmd = ""):
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    flink_compile_prepare(master, beaver_env, flink_repo)
    compile_cmd = retrieve_flink_compile_cmd(custom_conf)
    if checkout_cmd != "" and checkout_cmd != None:
        flink_checkout(checkout_cmd)
    flink_compile_with_cmd(beaver_env, compile_cmd)

def flink_compile_prepare(master, beaver_env, flink_repo):
    subprocess.check_call("rm -rf " + flink_source_code_path, shell=True)
    deploy_local_maven(master, beaver_env)
    deploy_maven(master, beaver_env)
    build_path = os.path.join(package_path, "build")
    tmp_path = os.path.join(package_path, "tmp")
    if not os.path.exists(build_path):
        subprocess.check_call("mkdir " + build_path, shell=True)
    if not os.path.exists(tmp_path):
        subprocess.check_call("mkdir " + tmp_path, shell=True)
    flink_git_clone(flink_repo, flink_source_code_path)
    flink_precompile_pkg_download(flink_source_code_path, beaver_env)
    flink_precompile_set_proxy(flink_source_code_path, beaver_env)

def flink_git_clone(git_repo, dst):
    subprocess.check_call("mkdir -p " + dst, shell=True)
    git_clone(git_repo, dst)

def flink_precompile_pkg_download(flink_source_path, beaver_env):
    flink_mvn_path = flink_source_path
    tmp_path = os.path.join(project_path, "package/tmp")
    subprocess.check_call('wget "https://www.apache.org/dyn/closer.lua?action=download&filename=/maven/maven-3/' + beaver_env.get("MAVEN_VERSION") + '/binaries/apache-maven-' + beaver_env.get("MAVEN_VERSION") + '-bin.tar.gz" -O ' + tmp_path + '/apache-maven-' + beaver_env.get("MAVEN_VERSION") + '-bin.tar.gz',shell=True)
    subprocess.check_call("tar -C " + flink_mvn_path + " -xvf " + os.path.join(tmp_path,"apache-maven-" + beaver_env.get("MAVEN_VERSION") + "-bin.tar.gz"),shell=True)
    subprocess.check_call("rm " + os.path.join(tmp_path, "apache-maven-" + beaver_env.get("MAVEN_VERSION") + "-bin.tar.gz"), shell=True)

def flink_precompile_set_proxy(flink_source_path, beaver_env):
    is_proxy_set = False
    output = subprocess.check_output("set | grep http_proxy", shell=True)
    for element in output.split("\n"):
        if element.split("=")[0] == "http_proxy" and element.split("=")[1] != "":
            print(colors.LIGHT_BLUE + "Find you have set the proxy" + colors.ENDC)
            is_proxy_set = True

    if is_proxy_set:
        print(colors.LIGHT_BLUE + "Setting proxy for Maven" + colors.ENDC)
        maven_config_path = os.path.join(flink_source_path, "apache-maven-" + beaver_env.get("MAVEN_VERSION") + "/conf")
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


def flink_get_build_scala_version():
    flink_ET = ET
    flink_pom_tree = flink_ET.parse(os.path.join(flink_source_code_path, 'pom.xml'))
    flink_pom_root = flink_pom_tree.getroot()
    scala_version = flink_pom_root.find('{http://maven.apache.org/POM/4.0.0}properties').find(
        '{http://maven.apache.org/POM/4.0.0}scala.version').text
    return scala_version

def flink_checkout(cmd):
    cmd = cmd.strip()
    git_command(cmd, flink_source_code_path)

def flink_compile_with_cmd(beaver_env, cmd=""):
    flink_source_path = flink_source_code_path
    print(colors.LIGHT_BLUE + "Start to compile Flink with command: " + cmd + colors.ENDC)
    subprocess.check_call("cd " + flink_source_path + " && mvn clean install " + cmd, shell = True)
    dst_pkg_name = FLINK_COMPONENT + "-" + flink_get_build_version() + ".tgz"
    subprocess.check_call("cd " + flink_source_path + "/flink-dist/target/" + FLINK_COMPONENT + "-" + flink_get_build_version() + "-bin " + "&& tar zcvf " + dst_pkg_name + " " + FLINK_COMPONENT + "-" + flink_get_build_version(), shell = True)
    print(colors.LIGHT_BLUE+ "Copy build file " + dst_pkg_name + " to ./pakage/build folder " + colors.ENDC)
    copyfile(os.path.join(flink_source_path, "flink-dist/target/" + FLINK_COMPONENT + "-" + flink_get_build_version() + "-bin/" + dst_pkg_name), os.path.join(project_path, "package/build/" + dst_pkg_name))

def flink_get_build_version():
    flink_ET = ET
    flink_pom_tree = flink_ET.parse(os.path.join(flink_source_code_path, 'pom.xml'))
    flink_pom_root = flink_pom_tree.getroot()
    flink_version = flink_pom_root.find('{http://maven.apache.org/POM/4.0.0}version').text
    return flink_version