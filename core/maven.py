#!/usr/bin/python

from utils.util import *
from utils.ssh import *

MAVEN_COMPONENT = "maven"


def deploy_maven(master, beaver_env):
    maven_version = beaver_env.get("MAVEN_VERSION")
    clean_maven(master)
    copy_maven(maven_version, master)
    setup_env_dist([master], beaver_env, MAVEN_COMPONENT)
    set_path(MAVEN_COMPONENT, [master], beaver_env.get("MAVEN_HOME"))


def clean_maven(master):
    ssh_execute(master, "rm -rf /opt/Beaver/maven*")


def copy_maven(version, master):
    package = "maven-" + version + ".tar.gz"
    if not os.path.isfile(os.path.join(package_path, package)):
        download_url = "http://" + download_server + "/software"
        print(colors.LIGHT_BLUE + "/tDownloading " + package + " from our repo..." + colors.ENDC)
        os.system("wget --no-proxy -P " + package_path + " " + download_url + "/" + package)
    else:
        print(colors.LIGHT_GREEN + "\t" + package + " has already exists in Beaver package" + colors.ENDC)
    copy_package_dist([master], os.path.join(package_path, package), MAVEN_COMPONENT, version)

def deploy_local_maven(master, beaver_env):
    copy_package_to_local(master.hostname, MAVEN_COMPONENT, beaver_env.get("MAVEN_VERSION"))
    setup_local_env(master.hostname, beaver_env, MAVEN_COMPONENT)
    set_local_path(master.hostname, MAVEN_COMPONENT, beaver_env.get("MAVEN_HOME"))