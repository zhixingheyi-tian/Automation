#!/usr/bin/python

from utils.util import *
from utils.ssh import *
from core.arrow import *

REDIS_COMPONENT = "redis"


def deploy_redis(slaves, beaver_env):
    redis_version = beaver_env.get("REDIS_VERSION")
    stop_redis_service(get_master_node(slaves), beaver_env)
    clean_redis(slaves, beaver_env)
    copy_redis(redis_version, slaves)
    setup_env_dist(slaves, beaver_env, REDIS_COMPONENT)
    set_path(REDIS_COMPONENT, slaves, beaver_env.get("REDIS_HOME"))
    build_gcc()
    compile_redis(slaves, beaver_env)


def compile_redis(slaves, beaver_env):
    for node in slaves:
        ssh_execute(node, "cd " + beaver_env.get("REDIS_HOME") + " && make")


def clean_redis(slaves, beaver_env):
    stop_redis_service(get_master_node(slaves), beaver_env)
    for node in slaves:
        ssh_execute(node, "rm -rf /opt/Beaver/redis*")


def copy_redis(version, slaves):
    package = "redis-" + version + ".tar.gz"
    if not os.path.isfile(os.path.join(package_path, package)):
        download_url = "http://" + download_server + "/software"
        print (colors.LIGHT_BLUE + "/tDownloading " + package + " from our repo..." + colors.ENDC)
        os.system("wget -P " + package_path + " " + download_url + "/" + package)
    else:
        print (colors.LIGHT_GREEN + "\t" + package + " has already exists in Beaver package" + colors.ENDC)
    copy_package_dist(slaves, os.path.join(package_path, package), REDIS_COMPONENT, version)


def start_redis_service(master, beaver_env):
    if beaver_env.get("OAP_with_external").lower() == "true":
        print(colors.LIGHT_BLUE + "Stop Redis service First" + colors.ENDC)
        stop_redis_service(master, beaver_env)
        time.sleep(10)
        print(colors.LIGHT_BLUE + "Start Redis service" + colors.ENDC)
        ssh_execute(master, "cd " + beaver_env.get("REDIS_HOME") +
                    " && ./src/redis-server --protected-mode no --daemonize yes")


def stop_redis_service(master, beaver_env):
    ssh_execute(master, "cd " + beaver_env.get("REDIS_HOME") + " && ./src/redis-cli SHUTDOWN")

# if __name__ == '__main__':
#     custom_conf = "/home/jh/Beaver/repo/workflows/oap_release_pmem_cluster_1_gold/output/output_workflow/oap-cache/TPCDS_3TB_parquet_DCPMM_Plasma_ColumnVector/"
#     cluster_file = get_cluster_file(custom_conf)
#     slaves = get_slaves(cluster_file)
#     master = get_master_node(slaves)
#     beaver_env = get_merged_env(custom_conf)
#     deploy_redis([master], beaver_env)
