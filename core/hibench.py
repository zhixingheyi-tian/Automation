from core.maven import *
from utils.util import *
from utils.git_cmd import *
from core.spark import calculate_hardware

hibench_repo="https://github.com/Intel-bigdata/HiBench.git"
HIBENCH_COMPONENT = "hibench"

def deploy_hibench(custom_conf, master):
    print("Deploy hibench")
    clean_hibench(master)
    beaver_env = get_merged_env(custom_conf)
    # git clone HiBench to master
    clean_hibench(master)
    git_repo_check(hibench_repo, beaver_env.get("HIBENCH_HOME"), beaver_env.get("HIBENCH_BRANCH"))
    deploy_maven(master, beaver_env)
    # make to compile HiBench
    compile_hibench(master, beaver_env)
    # replace hibench config
    update_copy_hibench_conf(master, custom_conf, beaver_env)

def undeploy_hibench(master):
    print("Undeploy hibench")
    clean_hibench(master)

def clean_hibench(matser):
    ssh_execute(matser, "rm -rf /opt/Beaver/hibench*")

def compile_hibench(master, beaver_env):
    print("compile hibench")
    cmd = "cd " + beaver_env.get("HIBENCH_HOME") + ";"
    if beaver_env.get("HIBENCH_COMPILED_COMMAND") == None:
        cmd += "mvn clean -Dspark=2.4 -Dscala=2.11 clean package"
    else:
        cmd += beaver_env.get("HIBENCH_COMPILED_COMMAND")
    ssh_execute(master, cmd)

def run_hibench(master, slaves, beaver_env, workload):
    if beaver_env.get("RPMEM_shuffle").lower() == "true":
        for node in slaves:
            ssh_execute(node, "ls /dev/dax* | xargs /usr/bin/pmempool rm")
            ssh_execute(node, "/usr/sbin/ldconfig")
    cmd = "rm -rf " + os.path.join(beaver_env.get("HIBENCH_HOME"), "report") + "; source /root/.bashrc; cd  " + beaver_env.get("HIBENCH_HOME") + "&& sh bin/workloads/" + workload  + "/spark/run.sh"
    return os.system(cmd)
    # ssh_execute(master, cmd)

def gen_hibench_data(master, custom_conf, beaver_env, workload):
    if gen_hibench_data_verify(master, custom_conf, beaver_env, workload):
        cmd = "rm -rf " + os.path.join(beaver_env.get("HIBENCH_HOME"), "report") + "; sh " + os.path.join(os.path.join(os.path.join(beaver_env.get("HIBENCH_HOME"), "bin/workloads"), workload), "prepare/prepare.sh")
        ssh_execute(master, cmd)

def gen_hibench_data_verify(master, custom_conf, beaver_env, workload):
    gen_data = False
    hibench_workload = workload.split("/")[-1].strip("\n").capitalize()
    hibench_output_conf = update_conf(HIBENCH_COMPONENT, custom_conf)
    hibench_config_file = os.path.join(hibench_output_conf, "hibench.conf")
    workload_config_file = os.path.join(hibench_output_conf, hibench_workload.lower() + ".conf")
    hibench_config = get_configs_from_properties(hibench_config_file)
    workload_config = get_configs_from_properties(workload_config_file)
    hibench_scale_profile = hibench_config["hibench.scale.profile"]
    granularity = get_hibench_workload_granularity(hibench_workload)
    workload_scale = "hibench." + hibench_workload.lower() + "." + hibench_scale_profile + "." + granularity
    data_path = "/HiBench/" + hibench_workload + "/Input/" + workload_config[workload_scale]
    status = ssh_execute(master, beaver_env.get("HADOOP_HOME") + "/bin/hadoop  fs -test -e " + data_path)
    if status != 0:
        gen_data = True
    return gen_data

def get_hibench_workload_granularity(hibench_workload):
    if hibench_workload == "Kmeans":
        return "num_of_samples"
    elif hibench_workload == "Terasort":
        return "datasize"
    elif hibench_workload == "Svm":
        return "examples"
    else:
        return ""

def update_copy_hibench_conf(master, custom_conf, beaver_env):
    if not os.path.isdir(beaver_env.get("HIBENCH_HOME")):
        git_repo_check(hibench_repo, beaver_env.get("HIBENCH_HOME"), beaver_env.get("HIBENCH_BRANCH"))
        deploy_maven(master, beaver_env)
        # make to compile HiBench
    compile_hibench(master, beaver_env)
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    hibench_output_conf = update_conf(HIBENCH_COMPONENT, custom_conf)
    for conf_file in [file for file in os.listdir(hibench_output_conf) if file.endswith(('.conf', '.xml'))]:
        output_conf_file = os.path.join(hibench_output_conf, conf_file)
        dict = get_hibench_replace_dict(master, slaves, beaver_env)
        replace_conf_value(output_conf_file, dict)
    copy_configurations([master], hibench_output_conf, HIBENCH_COMPONENT, beaver_env.get("HIBENCH_HOME"))

def get_hibench_replace_dict(master, slaves, beaver_env):
    print("Update spark.conf and hadoop.conf")
    hardware_config_list = calculate_hardware(master)
    node_num = len(slaves)
    total_cores = int(hardware_config_list[0]) * node_num
    total_memory = hardware_config_list[1] * node_num
    executor_cores = 4
    instances = int(total_cores / executor_cores)
    executor_memory = str(int(total_memory / instances / 1024 * 0.8))
    hibench_verison = get_hibench_version(beaver_env)

    dict = {'master_hostname':master.hostname,
            '{%spark.executor.cores%}':str(executor_cores),
            '{%spark.executor.instances%}':str(instances),
            '{%spark.executor.memory%}':executor_memory,
            '{%hadoop.home%}':beaver_env.get("HADOOP_HOME"),
            '{%spark.home%}':beaver_env.get("SPARK_HOME"),
            '{%hibench.version%}':hibench_verison}
    return dict

def get_hibench_version(beaver_env):
    hibench_ET = ET
    hibench_pom_path = os.path.join(beaver_env.get("HIBENCH_HOME"), "pom.xml")
    if os.path.isfile(hibench_pom_path):
        hibench_pom_tree = hibench_ET.parse(hibench_pom_path)
        hibench_pom_root = hibench_pom_tree.getroot()
        hibench_version = hibench_pom_root.find('{http://maven.apache.org/POM/4.0.0}version').text
        return hibench_version
    else:
        return ""

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

    if action == "deploy":
        deploy_hibench(custom_conf, master)
    if action == "gen_data":
        if len(args) < 4:
            exit(1)
        else:
            workload = args[3]
        gen_hibench_data(master, custom_conf, beaver_env, workload)
    if action == "run":
        if len(args) < 4:
            exit(1)
        else:
            workload = args[3]
        run_hibench(master, slaves, beaver_env, workload)
    if action == "update":
        update_copy_hibench_conf(master, custom_conf, beaver_env)