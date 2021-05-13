#!/usr/bin/python

# yum will install cmake-2.8, and Arrow build need a higher version

from utils.util import *
from utils.ssh import *
from utils.node import *
CMAKE_COMPONENT = "cmake"


def deploy_cmake(master, beaver_env):
    cmake_version = beaver_env.get("CMAKE_VERSION")
    # TODO: check whether cmake is already installed by cmake version
    installed_cmake_version = subprocess.Popen(" cmake -version | grep version | gawk -F ' ' '{print $3}'", shell=True,
                         stdout=subprocess.PIPE).stdout.readline().strip()
    if cmake_version != installed_cmake_version:
        clean_cmake(master)
        copy_cmake(cmake_version, master)
        install_cmake(cmake_version, master)
        setup_env_dist([master], beaver_env, CMAKE_COMPONENT)
        set_path(CMAKE_COMPONENT, [master], beaver_env.get("CMAKE_HOME"))

def clean_cmake(master):
    ssh_execute(master, "rm -rf /opt/Beaver/cmake*")


def copy_cmake(version, master):
    package = "cmake-" + version + ".tar.gz"
    if not os.path.isfile(os.path.join(package_path, package)):
        download_url = "https://cmake.org/files/v3.12"  # TODO: tricky here
        proxy_command = "http_proxy=http://child-prc.intel.com:913"
        os.system("wget -e \"" + proxy_command + "\" -P " + package_path + " " + download_url + "/" + package)
    else:
        print(colors.LIGHT_GREEN + "\t" + package + " has already exists in Beaver package" + colors.ENDC)
    copy_package_dist([master], os.path.join(package_path, package), CMAKE_COMPONENT, version)

def install_cmake(version, master):
    package = "cmake-" + version + ".tar.gz"
    os.system("cd " + package_path + " && tar -zxvf " + package)
    cmake_path = "/opt/Beaver/cmake-" + version
    os.system("cd " + cmake_path + " && ./bootstrap --prefix=/usr --system-curl" +
              " && make -j$(nproc) && make install")



def deploy_local_cmake(master, beaver_env):
    copy_package_to_local(master.hostname, CMAKE_COMPONENT, beaver_env.get("CMAKE_VERSION"))
    setup_local_env(master.hostname, beaver_env, CMAKE_COMPONENT)
    set_local_path(master.hostname, CMAKE_COMPONENT, beaver_env.get("CMAKE_HOME"))

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
        deploy_cmake(master, beaver_env)

