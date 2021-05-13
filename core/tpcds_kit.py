#!/usr/bin/python

from core.maven import *
from utils.util import *

TPCDS_KIT_COMPONENT = "tpcds-kit"


def deploy_tpcds_kit(custom_conf, master, slaves):
    print("Deploy tpcds-kit")
    clean_tpcds_kit(slaves)
    beaver_env = get_merged_env(custom_conf)
    deploy_maven(master, beaver_env)
    tpcds_kit_home = beaver_env.get("TPCDS_KIT_HOME")
    tpcds_kit_version = beaver_env.get("TPCDS_KIT_VERSION")
    #download tpcds-kit to master
    copy_packages([master], TPCDS_KIT_COMPONENT, tpcds_kit_version)

    #make to compile tpcds-kit
    compile_tpcds_kit(master, TPCDS_KIT_COMPONENT, tpcds_kit_version, tpcds_kit_home)

    #replace tpcds_kit package
    replace_tpcds_kit_package(master, slaves, TPCDS_KIT_COMPONENT, tpcds_kit_home, tpcds_kit_version )


def undeploy_tpcds_kit(slaves):
    print("Undeploy tpcds-kit")
    clean_tpcds_kit(slaves)


def replace_tpcds_kit_package(master, slaves, component, tpcds_kit_home, version):
    print(colors.LIGHT_BLUE + "remove and cover " + "tar.gz file" + " for " + component + colors.ENDC)
    package = component + "-" + version + ".tar.gz"
    print(colors.LIGHT_BLUE + "\tRemove tpcds_kit package " + colors.ENDC)
    os.system("rm -rf " + os.path.join(package_path, package))
    print(colors.LIGHT_BLUE + "\tCopy tpcds_kit " + colors.ENDC)
    #os.system("cp -r " + tpcds_kit_home + "-" + version + ".tar.gz " + package_path) #if master is not the beaver DEV computer, this will not work
    ssh_copy_from_remote(master,  tpcds_kit_home + "-" + version + ".tar.gz", os.path.join(package_path, package)) # add this to solve the issue mentioned above
    copy_package_dist(slaves, os.path.join(package_path, package), component, version)


def compile_tpcds_kit(master, component, tpcds_kit_version, tpcds_kit_home):
    print("+++++++++++++++++++++++++++++")
    print("Install gcc. Downloads, compiles and compile sbt")
    print("+++++++++++++++++++++++++++++")
    cmd = "yum -y install gcc make flex bison byacc unzip;"
    cmd += "yum -y install patch;"
    cmd += "cd " + tpcds_kit_home + "/tools;cp -r Makefile.suite Makefile;make;cd ../../;"
    cmd += "tar -zcvf " + tpcds_kit_home + "-" + tpcds_kit_version + ".tar.gz" + " " + component + "-" + tpcds_kit_version
    ssh_execute(master, cmd)


def clean_tpcds_kit(slaves):
    for node in slaves:
        ssh_execute(node, "rm -rf /opt/Beaver/tpcds-kit*")