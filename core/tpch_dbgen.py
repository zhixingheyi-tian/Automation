from core.maven import *
from utils.util import *
from utils.node import *

TPCH_DBGEN_COMPONENT = "tpch-dbgen"


def deploy_tpch_dbgen(custom_conf, master, slaves):
    print("Deploy tpch-dbgen")
    clean_tpch_dbgen(slaves)
    beaver_env = get_merged_env(custom_conf)
    deploy_maven(master, beaver_env)
    tpch_dbgen_home = beaver_env.get("TPCH_DBGEN_HOME")
    tpch_dbgen_version = beaver_env.get("TPCH_DBGEN_VERSION")
    #download tpch-dbgen to master
    copy_packages([master], TPCH_DBGEN_COMPONENT, tpch_dbgen_version)

    #make to compile tpch-dbgen
    compile_tpch_dbgen(master, TPCH_DBGEN_COMPONENT, tpch_dbgen_version, tpch_dbgen_home)

    #replace tpch_dbgen package
    replace_tpch_dbgen_package(master, slaves, TPCH_DBGEN_COMPONENT, tpch_dbgen_home, tpch_dbgen_version)


def undeploy_tpch_dbgen(slaves):
    print("Undeploy tpch-dbgen")
    clean_tpch_dbgen(slaves)


def replace_tpch_dbgen_package(master, slaves, component, tpch_dbgen_home, version):
    print(colors.LIGHT_BLUE + "remove and cover " + "tar.gz file" + " for " + component + colors.ENDC)
    package = component + "-" + version + ".tar.gz"
    print(colors.LIGHT_BLUE + "\tRemove tpch_dbgen package " + colors.ENDC)
    os.system("rm -rf " + os.path.join(package_path, package))
    print(colors.LIGHT_BLUE + "\tCopy tpch_dbgen " + colors.ENDC)
    #os.system("cp -r " + tpcds_kit_home + "-" + version + ".tar.gz " + package_path) #if master is not the beaver DEV computer, this will not work
    ssh_copy_from_remote(master,  tpch_dbgen_home + "-" + version + ".tar.gz", os.path.join(package_path, package)) # add this to solve the issue mentioned above
    copy_package_dist(slaves, os.path.join(package_path, package), component, version)


def compile_tpch_dbgen(master, component, tpch_dbgen_version, tpch_dbgen_home):
    print("+++++++++++++++++++++++++++++")
    print("Install gcc. Downloads, compiles and compile sbt")
    print("+++++++++++++++++++++++++++++")
    cmd = "yum -y install make patch unzip;"
    cmd += "cd " + tpch_dbgen_home + " && git checkout 0469309147b42abac8857fa61b4cf69a6d3128a8; make clean && make; cd ..;"
    cmd += "tar -zcvf " + tpch_dbgen_home + "-" + tpch_dbgen_version + ".tar.gz" + " " + component + "-" + tpch_dbgen_version
    ssh_execute(master, cmd)


def clean_tpch_dbgen(slaves):
    for node in slaves:
        ssh_execute(node, "rm -rf /opt/Beaver/tpch-dbgen*")

if __name__ == '__main__':
    conf_path = "/home/haojin/Beaver/repo/function_test_OAP_0.8/oap-cache/DailyTest_200GB_DCPMM_Guava"
    cluster_file = get_cluster_file(conf_path)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    deploy_tpch_dbgen(conf_path, master, slaves)