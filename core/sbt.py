from utils.util import *
from utils.ssh import *

SBT_COMPONENT = "sbt"


def deploy_sbt(master, beaver_env):
    copy_packages([master], SBT_COMPONENT, beaver_env.get("SBT_VERSION"))
    setup_env_dist([master], beaver_env, SBT_COMPONENT)
    set_path(SBT_COMPONENT, [master], beaver_env.get("SBT_HOME"))

def clean_sbt(master):
    ssh_execute(master, "rm -rf /opt/Beaver/sbt*")

def deploy_local_sbt(master, beaver_env):
    copy_package_to_local(master.hostname, SBT_COMPONENT, beaver_env.get("SBT_VERSION"))
    setup_local_env(master.hostname, beaver_env, SBT_COMPONENT)
    set_local_path(master.hostname, SBT_COMPONENT, beaver_env.get("SBT_HOME"))
