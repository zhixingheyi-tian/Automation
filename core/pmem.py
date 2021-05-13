from utils.util import *

def exchage_pmem_mode(custom_conf, beaver_env):
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    for node in slaves:
        if node.role == "master":
            continue
        ssh_copy(node, os.path.join(project_path,"scripts/pmem_mode_exchange.sh"), "/opt/Beaver/pmem_mode_exchange.sh")
        pmem_mode = ""
        if beaver_env.get("OAP_with_external").lower() == "true":
            pmem_mode = "striped"
        elif beaver_env.get("RPMEM_shuffle").lower() == "true":
            pmem_mode = "devdax"
        elif beaver_env.get("OAP_with_DCPMM").lower() == "true":
            pmem_mode = "fsdax"
        if pmem_mode != "":
            print (colors.BLUE + " use pmem with mode " + pmem_mode + " on " + node.hostname + colors.ENDC)
            repeat_execute_command_dist([node], "sh /opt/Beaver/pmem_mode_exchange.sh " + pmem_mode + " > /opt/Beaver/pmem.log")

if __name__ == '__main__':
    args = sys.argv
    if len(args) < 2:
        print ("Please input: [action] [repo_dir]")
    custom_conf = args[1]
    beaver_env = get_merged_env(custom_conf)
    exchage_pmem_mode(custom_conf, beaver_env)
