from core.hadoop import *
from utils.util import *
from utils.git_cmd import *
from xml.dom import minidom
from shutil import copyfile
from core.spark import *
from utils.node import *
from utils.util import *
from core.sbt import *
from core.maven import *
from core.jdk import *
from core.maven import *
from core.cmake import *
from core.arrow import *
from core.redis import *
import commands

SBT_COMPONENT = "sbt"
OAP_PERF_COMPONENT = "oap-perf"
oap_perf_jar_name = "oap-perf-suite-assembly-1.0.jar"
OAP_RUN_ENV = "oapenv"
OAP_CONDA_CHANNEL = "intel-bigdata"

def deploy_conda_oap_internal(slaves, beaver_env):
    clean_conda_oap_all(slaves, OAP_RUN_ENV)
    deploy_conda(slaves)
    if beaver_env.has_key("CONDA_OAP_VERSION"):
        conda_oap_version = beaver_env.get("CONDA_OAP_VERSION")
        deploy_conda_package(slaves, OAP_RUN_ENV, "oap", conda_oap_version)
    else:
        print (colors.RED + "Please define CONDA_OAP_VERSION in env.conf" + colors.ENDC)
        exit(1)



def clean_conda_oap_all(slaves, env_name = OAP_RUN_ENV):
    repeat_execute_command_dist(slaves, "conda env remove -n " + env_name)


def deploy_conda_package(slaves, env_name, package, conda_version, channel=OAP_CONDA_CHANNEL):
    repeat_execute_command_dist(slaves, "conda create -n " + env_name + " -y python=3.7")
    repeat_execute_command_dist(slaves, "conda activate " + env_name)
    repeat_execute_command_dist(slaves,
                                "conda activate " + env_name + "&&conda install -c conda-forge -c intel-beaver -c " + channel + " -y " + package + "=" + conda_version)


def deploy_conda(slaves):
    package = "Miniconda2-latest-Linux-x86_64.sh"
    exit_status = repeat_execute_command_dist(slaves, "conda info")
    if (exit_status == 0):
        return
    download_url = "https://repo.anaconda.com/miniconda/Miniconda2-latest-Linux-x86_64.sh"
    repeat_execute_command_dist(slaves, "wget -P " + package_path + " " + download_url)
    repeat_execute_command_dist(slaves, "cd " + package_path + "&&bash " + package + " -b")
    repeat_execute_command_dist(slaves, "~/miniconda2/bin/conda init")
    repeat_execute_command_dist(slaves, "source ~/.bashrc")


def conda_oap_compile(beaver_env, master):
    conda_oap_compile_prepare(beaver_env, master)
    oap_reset_commit(beaver_env)
    conda_oap_compile_with_cmd(beaver_env, master)

def set_conda_upload_conf(slaves):
    repeat_execute_command_dist(slaves, "conda activate base&&conda install -y anaconda-client conda-build")
    repeat_execute_command_dist(slaves, "echo \"Y\"|anaconda login  --username 'intel-beaver' --password 'bdpe123'")
    repeat_execute_command_dist(slaves, "conda config --set anaconda_upload yes")

def deploy_oap_internal(custom_conf, master, slaves, beaver_env):
    clean_oap_all(master, beaver_env)
    deploy_oap(custom_conf, master, slaves, beaver_env)

def oap_git_clone(oap_repo):
    dst = oap_source_code_path
    subprocess.check_call("mkdir -p " + dst, shell=True)
    git_clone(oap_repo, dst)

def oap_compile(beaver_env, master):
    oap_compile_prepare(beaver_env, master)
    oap_reset_commit(beaver_env)
    oap_compile_with_cmd(beaver_env)

def oap_reset_commit(beaver_env):
    if beaver_env.has_key("OAP_COMMIT"):
        commit = beaver_env.get("OAP_COMMIT")
        oap_git_run("reset --hard " + commit)

def oap_checkout(checkout_cmd):
    oap_git_run("checkout " + checkout_cmd)


def conda_arrow_compile_with_cmd(beaver_env, master):
    arrow_branch = beaver_env.get("ARROW_BRANCH")
    arrow_repo = beaver_env.get("ARROW_GIT_REPO")
    subprocess.check_call(
        "mkdir -p " + oap_source_code_path + "/dev/thirdparty/", shell=True)
    arrow_source_code_path = os.path.join(oap_source_code_path,
                                 "dev/thirdparty/arrow")
    if os.path.exists(arrow_source_code_path):
        subprocess.check_call(
            "cd " + arrow_source_code_path + ";git pull; git checkout " + arrow_branch, shell=True)
    else:
        subprocess.check_call(
            "cd " + os.path.join(oap_source_code_path,
                                 "dev/thirdparty/") + "; git clone " + arrow_repo + ";cd arrow; git checkout " + arrow_branch,
            shell=True)
    slaves = []
    slaves.append(master)
    set_conda_upload_conf(slaves)
    repeat_execute_command_dist(slaves, "conda config --add channels intel")
    repeat_execute_command_dist(slaves, "conda config --add channels conda-forge")
    repeat_execute_command_dist(slaves, "cd " + oap_source_code_path + "/dev/conda-release/conda-recipes/arrow&&conda build .")


def conda_oap_compile_with_cmd(beaver_env, master):
    oap_version = ""
    if beaver_env.get("OAP_BRANCH").startswith("v"):
        oap_version = beaver_env.get("OAP_BRANCH").split("-")[0][1:-2]
    elif beaver_env.get("OAP_BRANCH").startswith("branch"):
        oap_version = beaver_env.get("OAP_BRANCH").split("-")[1]
    slaves = []
    slaves.append(master)
    deploy_conda(slaves)
    set_conda_upload_conf(slaves)

    if beaver_env.get("OAP_BRANCH") == "master" or float(oap_version) > 0.8:
        build_folder = os.path.join(package_path, "build")
        conda_arrow_compile_with_cmd(beaver_env, master)
        clean_conda_oap_all(slaves, "oapbuild")
        deploy_conda_package(slaves, "oapbuild", "oap-arrow", "0.17.0", channel=OAP_CONDA_CHANNEL)
        subprocess.check_call("sh " + os.path.join(oap_source_code_path, "dev/scripts/prepare_oap_env.sh --prepare_conda_build"), shell=True)
        repeat_execute_command_dist(slaves, "cd " + oap_source_code_path+"; export ONEAPI_ROOT=/tmp/; conda activate oapbuild&&mvn clean package -pl com.intel.oap:spark-columnar-core  -am -DskipTests")
        subprocess.check_call("sh " + os.path.join(oap_source_code_path, "dev/compile-oap.sh --oap-conda"), shell=True)
    else:
        subprocess.check_call("sh " + os.path.join(oap_source_code_path, "dev/install-compile-time-dependencies.sh"),
                              shell=True)
        subprocess.check_call("sh " + os.path.join(oap_source_code_path, "dev/compile-oap.sh"), shell=True)
    copy_pmdk_to_local(oap_source_code_path+"/dev/thirdparty/arrow/")
    repeat_execute_command_dist(slaves, "cd " + oap_source_code_path + "/dev/conda-release/conda-recipes/oap&&conda build .")


def oap_compile_with_cmd(beaver_env):
    oap_version = ""
    if beaver_env.get("OAP_BRANCH").startswith("v"):
        oap_version = beaver_env.get("OAP_BRANCH").split("-")[0][1:-2]
    elif beaver_env.get("OAP_BRANCH").startswith("branch"):
        oap_version = beaver_env.get("OAP_BRANCH").split("-")[1]

    if beaver_env.get("OAP_BRANCH") == "master" or float(oap_version) > 0.7:
        build_folder = os.path.join(package_path, "build")
        mkdirs(build_folder)
        # subprocess.check_call("sh " + os.path.join(oap_source_code_path, "dev/install-compile-time-dependencies.sh ") + "; sh " + os.path.join(oap_source_code_path, "dev/compile-oap.sh"), shell=True)
        repeat_execute_command_dist([], "sh " + os.path.join(oap_source_code_path, "dev/install-compile-time-dependencies.sh ") + "; sh " + os.path.join(oap_source_code_path, "dev/compile-oap.sh"))
        subprocess.check_call("cp " + oap_source_code_path + "/oap-cache/oap/target/*.jar " + build_folder, shell=True)
        subprocess.check_call("cp " + oap_source_code_path + "/oap-shuffle/remote-shuffle/target/*.jar " + build_folder, shell=True)
        subprocess.check_call("cp " + oap_source_code_path + "/oap-common/target/*.jar " + build_folder, shell=True)
        subprocess.check_call("cp " + oap_source_code_path + "/oap-spark/target/*.jar " + build_folder, shell=True)
        if os.path.isdir(os.path.join(oap_source_code_path,"oap-native-sql/core/target")):
            subprocess.check_call("cp " + oap_source_code_path + "/oap-native-sql/core/target/*.jar " + build_folder, shell=True)
        if os.path.isdir(os.path.join(oap_source_code_path,"oap-data-source/arrow/target")):
            try:
                subprocess.check_call("cp " + oap_source_code_path + "/oap-data-source/arrow/standard/target/*.jar " + build_folder, shell=True)
            except:
                print (colors.RED + "no jars on oap-data-source/arrow/standard/target" + colors.ENDC)
        if os.path.isdir(os.path.join(oap_source_code_path,"oap-data-source/arrow/target")):
            try:
                subprocess.check_call("cp " + oap_source_code_path + "/oap-data-source/arrow/target/*.jar " + build_folder, shell=True)
            except:
                print (colors.RED + "no jars on oap-data-source/arrow/target" + colors.ENDC)
        # TODO: when the code of oap-RPMEM-shuffle is ready:
        if os.path.isdir(os.path.join(oap_source_code_path,"oap-data-source/arrow/standard/target")):
            subprocess.check_call("cp " + oap_source_code_path + "/oap-data-source/arrow/standard/target/*.jar " + build_folder, shell=True)
        if os.path.isdir(os.path.join(oap_source_code_path,"oap-shuffle/RPMem-shuffle/core/target")):
            subprocess.check_call("cp " + oap_source_code_path + "/oap-shuffle/RPMem-shuffle/core/target/*.jar " + build_folder, shell=True)
        if os.path.isdir(os.path.join(oap_source_code_path,"oap-mllib/mllib-dal/target")):
            subprocess.check_call("cp " + oap_source_code_path + "/oap-mllib/mllib-dal/target/*.jar " + build_folder, shell=True)
    else:
        oap_source_path = oap_source_code_path
        oap_tar_path = os.path.join(oap_source_path, "target")
        build_folder = os.path.join(package_path, "build")

        if not os.path.isdir(build_folder):
            subprocess.check_call("mkdir -p " + build_folder, shell=True)

        subprocess.check_call("cd " + oap_source_path + " && source ~/.bashrc && mvn clean", shell=True)
        oap_cache_compile_cmd = "mvn clean -Ppersistent-memory -Pvmemcache -DskipTests package"
        print(colors.LIGHT_BLUE + "Start to compile OAP with command: " + oap_cache_compile_cmd.strip() + colors.ENDC)
        subprocess.check_call("cd " + oap_source_path + " && source ~/.bashrc && " + oap_cache_compile_cmd, shell=True)
        dst_pkg_name = "oap" + "-" + oap_cache_get_build_version() + '-with-spark-' + oap_cache_get_build_spark_version() + ".jar"
        copyfile(os.path.join(oap_tar_path, dst_pkg_name), os.path.join(build_folder, dst_pkg_name))
        oap_perf_pkg_name = "oap" + "-" + oap_cache_get_build_version() + '-test-jar-with-dependencies.jar'
        if os.path.isfile(os.path.join(oap_tar_path, oap_perf_pkg_name)):
            copyfile(os.path.join(oap_tar_path, oap_perf_pkg_name), os.path.join(build_folder, oap_perf_pkg_name))


def get_oap_jar_path(beaver_env):
    if beaver_env.get("OAP_HOME") != None:
        return os.path.join(beaver_env.get("OAP_HOME"), "oap_jar")
    else:
        return ""

def oap_get_jar_name(beaver_env, spark_version):
    oap_version = beaver_env.get("OAP_VERSION")
    if oap_version is None:
        oap_version = ""
    return "oap" + "-" + oap_version + '-with-spark-' + spark_version + ".jar"

def remote_shuffle_get_build_version():
    remote_shuffle_ET = ET
    remote_shuffle_pom_path = os.path.join(oap_source_code_path, "oap-shuffle/remote-shuffle/pom.xml")
    if os.path.isfile(remote_shuffle_pom_path):
        remote_shuffle_pom_tree = remote_shuffle_ET.parse(remote_shuffle_pom_path)
        remote_shuffle_pom_root = remote_shuffle_pom_tree.getroot()
        remote_shuffle_version = remote_shuffle_pom_root.find('{http://maven.apache.org/POM/4.0.0}version').text
        return remote_shuffle_version
    else:
        return ""

def oap_cache_get_build_spark_version(path=""):
    oap_ET = ET
    oap_pom_path = os.path.join(os.path.join(oap_source_code_path, path), 'pom.xml')
    if os.path.isfile(oap_pom_path):
        oap_pom_tree = oap_ET.parse(oap_pom_path)
        oap_pom_root = oap_pom_tree.getroot()
        oap_pom_properties = oap_pom_root.find('{http://maven.apache.org/POM/4.0.0}properties')
        spark_internal_version = oap_pom_properties.find('{http://maven.apache.org/POM/4.0.0}spark.internal.version').text
        return spark_internal_version
    else:
        return ""

def oap_cache_get_build_version(path=""):
    oap_ET = ET
    oap_pom_path = os.path.join(os.path.join(oap_source_code_path, path), 'pom.xml')
    if os.path.isfile(oap_pom_path):
        oap_pom_tree = oap_ET.parse(oap_pom_path)
        oap_pom_root = oap_pom_tree.getroot()
        oap_version = oap_pom_root.find('{http://maven.apache.org/POM/4.0.0}version').text
        return oap_version
    else:
        return ""

def oap_jar_dist(master, slaves, beaver_env):
    oap_version = ""
    if beaver_env.get("OAP_BRANCH").startswith("v"):
        oap_version = beaver_env.get("OAP_BRANCH").split("-")[0][1:-2]
    elif beaver_env.get("OAP_BRANCH").startswith("branch"):
        oap_version = beaver_env.get("OAP_BRANCH").split("-")[1]

    for node in slaves:
        print (colors.LIGHT_BLUE + "\tCopy OAP jar to " + node.hostname + "..." + colors.ENDC)
        build_folder = os.path.join(package_path, "build")
        subprocess.check_call("mkdir -p " + build_folder, shell=True)
        spark_version = beaver_env.get("SPARK_VERSION")
        dst_path = get_oap_jar_path(beaver_env)
        ssh_execute(node, "mkdir -p " + dst_path)

        # oap-cache
        oap_cache_files = subprocess.check_output("find " + build_folder + " -name \"oap-*-with-spark-" + spark_version.strip() + ".jar\"", shell=True).strip('\r\n')
        if oap_cache_files == "":
            oap_compile(beaver_env, master)
            oap_cache_files = subprocess.check_output("find " + build_folder + " -name \"oap-*-with-spark-" + spark_version.strip() + ".jar\"", shell=True).strip('\r\n')
        for oap_cache_file in oap_cache_files.split("\n"):
            oap_cache_file_name = os.path.basename(oap_cache_file)
            print (colors.LIGHT_BLUE + "\tCopy " + oap_cache_file_name + " to " + node.hostname + "..." + colors.ENDC)
            ssh_copy(node, os.path.join(build_folder, oap_cache_file_name), os.path.join(dst_path, oap_cache_file_name))

        if beaver_env.get("OAP_BRANCH") == "master" or float(oap_version) > 0.7:
            #oap-common
            if node.role == "master":
                print (colors.LIGHT_BLUE + "\tCopy oap-common.jar ..." + colors.ENDC)
                oap_common_files = subprocess.check_output(
                    "find " + build_folder + " -name \"oap-common-*-with-spark-" + spark_version.strip() + ".jar\"", shell=True).strip('\r\n')
                if oap_common_files == "":
                    oap_compile(beaver_env, master)
                    oap_common_files = subprocess.check_output(
                        "find " + build_folder + " -name \"oap-common-*-with-spark-" + spark_version.strip() + ".jar\"",
                        shell=True).strip('\r\n')
                for oap_common_file in oap_common_files.split("\n"):
                    oap_common_file_name = os.path.basename(oap_common_file)
                    print (colors.LIGHT_BLUE + "\tCopy " + oap_common_file_name + " to " + node.hostname + "..." + colors.ENDC)
                    ssh_copy(node, os.path.join(build_folder, oap_common_file_name),
                             os.path.join(dst_path, oap_common_file_name))

            # oap-shuffle
            print (colors.LIGHT_BLUE + "\tCopy remote-shuffle.jar ..." + colors.ENDC)
            oap_remote_shuffle_files = subprocess.check_output("find " + build_folder + " -name \"oap-remote-shuffle-*.jar\"", shell=True).strip('\r\n')
            if oap_remote_shuffle_files == "":
                oap_compile(beaver_env, master)
                oap_remote_shuffle_files = subprocess.check_output("find " + build_folder + " -name \"oap-remote-shuffle-*-with-spark-" + spark_version.strip() + ".jar\"", shell=True).strip('\r\n')
            for oap_remote_shuffle_file in oap_remote_shuffle_files.split("\n"):
                oap_remote_shuffle_file_name = os.path.basename(oap_remote_shuffle_file)
                print (colors.LIGHT_BLUE + "\tCopy " + oap_remote_shuffle_file_name + " to " + node.hostname + "..." + colors.ENDC)
                ssh_copy(node, os.path.join(build_folder, oap_remote_shuffle_file_name), os.path.join(dst_path, oap_remote_shuffle_file_name))

            # oap-spark
            print (colors.LIGHT_BLUE + "\tCopy oap-spark.jar ..." + colors.ENDC)
            oap_spark_files = subprocess.check_output(
                "find " + build_folder + " -name \"oap-spark-*-with-spark-" + spark_version.strip() + ".jar\"",
                shell=True).strip('\r\n')
            if oap_spark_files == "":
                oap_compile(beaver_env, master)
                oap_spark_files = subprocess.check_output(
                    "find " + build_folder + " -name \"oap-spark-*-with-spark-" + spark_version.strip() + ".jar\"",
                    shell=True).strip('\r\n')
            for oap_spark_file in oap_spark_files.split("\n"):
                oap_spark_file_name = os.path.basename(oap_spark_file)
                print (colors.LIGHT_BLUE + "\tCopy " + oap_spark_file_name + " to " + node.hostname + "..." + colors.ENDC)
                ssh_copy(node, os.path.join(build_folder, oap_spark_file_name),  os.path.join(dst_path, oap_spark_file_name))

            # oap-RPMEM-shuffle
            # TODO: when the code of oap-RPMEM-shuffle is ready:
            print (colors.LIGHT_BLUE + "\tCopy oap-rpmem-shuffle-*-jar-with-dependencies.jar ..." + colors.ENDC)
            oap_RPMEM_shuffle_files = subprocess.check_output("find " + build_folder + " -name \"oap-rpmem-shuffle-*-jar-with-dependencies.jar\"",  shell=True).strip('\r\n')
            if oap_RPMEM_shuffle_files == "":
                oap_compile(beaver_env, master)
                oap_RPMEM_shuffle_files = subprocess.check_output("find " + build_folder + " -name \"oap-rpmem-shuffle-*-jar-with-dependencies.jar\"", shell=True).strip('\r\n')
            for oap_RPMEM_shuffle_file in oap_RPMEM_shuffle_files.split("\n"):
                oap_RPMEM_shuffle_file_name = os.path.basename(oap_RPMEM_shuffle_file)
                print (colors.LIGHT_BLUE + "\tCopy " + oap_RPMEM_shuffle_file_name + " to " + node.hostname + "..." + colors.ENDC)
                ssh_copy(node, os.path.join(build_folder, oap_RPMEM_shuffle_file_name),  os.path.join(dst_path, oap_RPMEM_shuffle_file_name))

            if beaver_env.get("OAP_BRANCH") == "master" or float(oap_version) > 0.8:
                # oap-data-source
                print(colors.LIGHT_BLUE + "\tCopy spark-arrow-datasource-*.jar ..." + colors.ENDC)
                oap_data_source_files = subprocess.check_output(
                    "find " + build_folder + " -name \"spark-arrow-datasource-*.jar\"",
                    shell=True).strip('\r\n')
                if oap_data_source_files == "" and oap_version != "0.8":
                    oap_compile(beaver_env, master)
                    oap_native_sql_files = subprocess.check_output(
                        "find " + build_folder + " -name \"spark-arrow-datasource-*-jar-with-dependencies.jar\"",
                        shell=True).strip('\r\n')
                for oap_data_source_file in oap_data_source_files.split("\n"):
                    oap_data_source_file_name = os.path.basename(oap_data_source_file)
                    print(colors.LIGHT_BLUE + "\tCopy " + oap_data_source_file_name + " to " + node.hostname + "..." + colors.ENDC)
                    ssh_copy(node, os.path.join(build_folder, oap_data_source_file_name), os.path.join(dst_path, oap_data_source_file_name))

                # native-sql
                print(colors.LIGHT_BLUE + "\tCopy spark-columnar-core-*-jar-with-dependencies.jar ..." + colors.ENDC)
                oap_native_sql_files = subprocess.check_output(
                    "find " + build_folder + " -name \"spark-columnar-core-*-jar-with-dependencies.jar\"",
                    shell=True).strip('\r\n')
                if oap_native_sql_files == "" and oap_version != "0.8":
                    oap_compile(beaver_env, master)
                    oap_native_sql_files = subprocess.check_output(
                        "find " + build_folder + " -name \"spark-columnar-core-*-jar-with-dependencies.jar\"",
                        shell=True).strip('\r\n')
                for oap_native_sql_file in oap_native_sql_files.split("\n"):
                    oap_native_sql_file_name = os.path.basename(oap_native_sql_file)
                    print(colors.LIGHT_BLUE + "\tCopy " + oap_native_sql_file_name + " to " + node.hostname + "..." + colors.ENDC)
                    ssh_copy(node, os.path.join(build_folder, oap_native_sql_file_name), os.path.join(dst_path, oap_native_sql_file_name))
                # arrow-dataset
                # print(colors.LIGHT_BLUE + "\tCopy arrow-dataset-*.jar ..." + colors.ENDC)
                # arrow_dataset_files = subprocess.check_output("find " + build_folder + " -name \"arrow-dataset-*.jar\"", shell=True).strip('\r\n')
                # if arrow_dataset_files == "" and oap_version != "0.8":
                #     arrow_deploy(beaver_env)
                #     arrow_dataset_files = subprocess.check_output("find " + build_folder + " -name \"arrow-dataset-*.jar\"", shell=True).strip('\r\n')
                # for arrow_dataset_file in arrow_dataset_files.split("\n"):
                #     arrow_dataset_file_name = os.path.basename(arrow_dataset_file)
                #     print(colors.LIGHT_BLUE + "\tCopy " + arrow_dataset_file_name + " to " + node.hostname + "..." + colors.ENDC)
                #     ssh_copy(node, os.path.join(build_folder, arrow_dataset_file_name), os.path.join(dst_path, arrow_dataset_file_name))

                # oap-mllib
                print(colors.LIGHT_BLUE + "\tCopy oap-mllib-*.jar ..." + colors.ENDC)
                oap_mllib_files = subprocess.check_output("find " + build_folder + " -name \"oap-mllib-*.jar\"", shell=True).strip('\r\n')
                if oap_mllib_files == "" and oap_version != "0.8":
                    oap_compile(beaver_env, master)
                    oap_mllib_files = subprocess.check_output("find " + build_folder + " -name \"oap-mllib-*.jar\"", shell=True).strip('\r\n')
                    for oap_mllib_file in oap_mllib_files.split("\n"):
                        oap_mllib_file_name = os.path.basename(oap_mllib_file)
                        print(colors.LIGHT_BLUE + "\tCopy " + oap_mllib_file_name + " to " + node.hostname + "..." + colors.ENDC)
                        ssh_copy(node, os.path.join(build_folder, oap_mllib_file_name), os.path.join(dst_path, oap_mllib_file_name))
        # ldconfig
        ssh_execute(node, "/usr/sbin/ldconfig")


def deploy_oap(custom_conf, master, slaves, beaver_env):
    clean_oap_all(master, beaver_env)
    # update_spark_config_with_oap(custom_conf, master, beaver_env)
    oap_dependency_prepare(custom_conf, master, slaves, beaver_env)
    oap_jar_dist(master, slaves, beaver_env)
    if beaver_env.get("OAP_with_external").lower() == "true":
        deploy_redis([master], beaver_env)

def oap_dependency_prepare(custom_conf, master, slaves, beaver_env):
    deploy_local_maven(master, beaver_env)
    deploy_maven(master, beaver_env)
    deploy_cmake(master, beaver_env)
    if beaver_env.get("RPMEM_shuffle").lower() == "true":
        build_gcc()
        # install libfabric (HPNL required)
        libfabric_repo = "https://github.com/ofiwg/libfabric.git"
        libfabric_code_path = "/opt/Beaver/tmp/libfabric"
        git_repo_check(libfabric_repo, libfabric_code_path, "v1.6.0")
        subprocess.check_call("cd " + libfabric_code_path + " && ./autogen.sh", shell=True)
        subprocess.check_call("cd " + libfabric_code_path + " && ./configure --prefix=/usr --disable-sockets --disable-verbs --disable-mlx", shell=True)
        subprocess.check_call("cd " + libfabric_code_path + " && make -j", shell=True)
        subprocess.check_call("cd " + libfabric_code_path + " && make install", shell=True)

        # install HPNL
        ssh_execute(master, "yum -y install cmake boost-devel boost-system")
        HPNL_repo = "https://github.com/Intel-bigdata/HPNL.git"
        HPNL_code_path = "/opt/Beaver/tmp/HPNL"
        git_repo_check(HPNL_repo, HPNL_code_path, "origin/spark-pmof-test")
        git_command("submodule update --init --recursive", HPNL_code_path)
        subprocess.check_call("cd " + HPNL_code_path + " && mkdir -p build && cd build && cmake -DCMAKE_INSTALL_PREFIX=/usr/ -DWITH_VERBS=ON -DWITH_JAVA=ON .. && make -j && sudo make install && cd ../java/hpnl && mvn install", shell=True)

        # install ndctl
        ssh_execute(master, "yum -y install autoconf")
        ssh_execute(master, "yum -y install rubygem-asciidoctor")
        ssh_execute(master, "yum -y grroupinstall \"Development Tools\" ")

        ssh_execute(master, "yum -y install rubygem-asciidoctor")
        ssh_execute(master, "yum -y install kmod-devel.x86_64")
        ssh_execute(master, "yum -y install libudev-devel")
        ssh_execute(master, "yum -y install libuuid-devel")
        ssh_execute(master, "yum -y install json-c-devel")
        ssh_execute(master, "yum -y install jemalloc-devel")
        ndctl_repo = "https://github.com/pmem/ndctl.git"
        ndctl_code_path = "/opt/Beaver/tmp/ndctl"
        git_repo_check(ndctl_repo, ndctl_code_path, "v63")
        subprocess.check_call("cd " + ndctl_code_path + " && ./autogen.sh", shell=True)
        subprocess.check_call("cd " + ndctl_code_path + " && ./configure CFLAGS='-g -O2' --prefix=/usr --sysconfdir=/etc --libdir=/usr/lib64 ", shell=True)
        subprocess.check_call("cd " + ndctl_code_path + " && make -j", shell=True)
        subprocess.check_call("cd " + ndctl_code_path + " && make check", shell=True)
        subprocess.check_call("cd " + ndctl_code_path + " && make install", shell=True)

        # install pmdk
        ssh_execute(master, "yum -y install pandoc")
        pmdk_repo = "https://github.com/pmem/pmdk.git"
        pmdk_code_path = "/opt/Beaver/tmp/pdmk"
        git_repo_check(pmdk_repo, pmdk_code_path, "tags/1.8")
        subprocess.check_call("cd " + pmdk_code_path + " && make -j", shell=True)
        subprocess.check_call("cd " + pmdk_code_path + " && make install", shell=True)

        # install libcuckoo
        libcuckoo_repo = "https://github.com/efficient/libcuckoo"
        libcuckoo_code_path = "/opt/Beaver/tmp/libcuckoo"
        git_repo_check(libcuckoo_repo, libcuckoo_code_path, "master")
        subprocess.check_call("cd " + libcuckoo_code_path + " && mkdir -p build && cd build && cmake -DCMAKE_INSTALL_PREFIX=/usr/ -DBUILD_EXAMPLES=1 -DBUILD_TESTS=1 ..  && make all && make install", shell=True)

        # copy
        for node in slaves:
            print(colors.LIGHT_BLUE + "Copy libfabric.so, libhpnl.so, libpmemblk.so, libpmemlog.so, libpmemobj.so, libpmempool.so,libpmem.so, libjnipmdk.so, pmempool to " + node.hostname + "..." + colors.ENDC)
            ssh_execute(node, "rm -f /lib64/libfabric.so")
            ssh_execute(node, "rm -f /lib64/libhpnl.so")
            ssh_execute(node, "rm -f /lib64/libpmemblk.so")
            ssh_execute(node, "rm -f /lib64/libpmemlog.so")
            ssh_execute(node, "rm -f /lib64/libpmemobj.so")
            ssh_execute(node, "rm -f /lib64/libpmempool.so")
            ssh_execute(node, "rm -f /lib64/libpmem.so")
            ssh_execute(node, "rm -f /lib64/libjnipmdk.so")

            ssh_copy(node, os.path.join(libfabric_code_path, "src/.libs/libfabric.so.1.9.13"), "/lib64/libfabric.so")
            ssh_copy(node, os.path.join(HPNL_code_path, "build/lib/libhpnl.so"), "/lib64/libhpnl.so")
            ssh_copy(node, os.path.join(pmdk_code_path, "src/nondebug/libpmemblk.so.1.0.0"), "/lib64/libpmemblk.so")
            ssh_copy(node, os.path.join(pmdk_code_path, "src/nondebug/libpmemlog.so.1.0.0"), "/lib64/libpmemlog.so")
            ssh_copy(node, os.path.join(pmdk_code_path, "src/nondebug/libpmemobj.so.1.0.0"), "/lib64/libpmemobj.so")
            ssh_copy(node, os.path.join(pmdk_code_path, "src/nondebug/libpmempool.so.1.0.0"), "/lib64/libpmempool.so")
            ssh_copy(node, os.path.join(pmdk_code_path, "src/nondebug/libpmem.so.1.0.0"), "/lib64/libpmem.so")
            ssh_copy(node, os.path.join(oap_source_code_path, "oap-shuffle/RPMem-shuffle/native/src/libjnipmdk.so"), "/lib64/libjnipmdk.so")
            ssh_copy(node, os.path.join(oap_source_code_path, "oap-shuffle/RPMem-shuffle/native/src/libjnipmdk.so"), "/usr/local/lib/libjnipmdk.so")
            ssh_copy(node, "/usr/local/bin/pmempool", "/usr/bin/pmempool")

            ssh_execute(node, "chmod 755 /lib64/libfabric.so")
            ssh_execute(node, "chmod 755 /lib64/libhpnl.so")
            ssh_execute(node, "chmod 755 /lib64/libpmemblk.so")
            ssh_execute(node, "chmod 755 /lib64/libpmemlog.so")
            ssh_execute(node, "chmod 755 /lib64/libpmemobj.so")
            ssh_execute(node, "chmod 755 /lib64/libpmempool.s")
            ssh_execute(node, "chmod 755 /lib64/libpmem.so")
            ssh_execute(node, "chmod 755 /lib64/libjnipmdk.so")
            ssh_execute(node, "chmod 755 /usr/bin/pmempool")

    if beaver_env.get("NATIVE_SQL_ENGINE").lower() == "true" or beaver_env.get("ARROW_DATA_SOURCE").lower() == "true":
        arrow_deploy(beaver_env)
        if not os.path.isfile(os.path.join(package_path, "libprotobuf.so.13")):
            os.system("wget -P " + package_path + " https://github.com/Intel-bigdata/OAP/raw/branch-nativesql-spark-3.0.0/oap-native-sql/cpp/src/resources/libprotobuf.so.13")
        if not os.path.isfile(os.path.join(package_path, "libhdfs3.so")):
            os.system("wget -P " + package_path + " https://github.com/Intel-bigdata/OAP/raw/branch-nativesql-spark-3.0.0/oap-native-sql/cpp/src/resources/libhdfs3.so")

        for node in slaves:
            # NATIVE_SQL_ENGINE need libs
            if beaver_env.get("NATIVE_SQL_ENGINE").lower() == "true":
                ssh_execute(node, "yum -y install gcc-c++")
                ssh_execute(node, "yum -y install gtest-devel")
                ssh_execute(node, "yum -y install gmock")
                print(colors.LIGHT_BLUE + "Copy libgandiva_jni.so, libgandiva.so to " + node.hostname + "..." + colors.ENDC)
                ssh_execute(node, "rm -f /lib64/libgandiva_jni.so")
                ssh_execute(node, "rm -f /lib64/libgandiva.so")
                ssh_copy(node, os.path.join(arrow_build_dir, "release/libgandiva_jni.so.17.0.0"), "/lib64/libgandiva_jni.so")
                ssh_copy(node, os.path.join(arrow_build_dir, "release/libgandiva.so.17.0.0"), "/lib64/libgandiva.so")
                ssh_execute(node, "chmod 755 /lib64/libgandiva_jni.so")
                ssh_execute(node, "chmod 755 /lib64/libgandiva.so")
                if node.hostname != socket.gethostname():
                    print(colors.LIGHT_BLUE + "Copy arrow, gandiva, parquet include file to " + node.hostname + "..." + colors.ENDC)
                    ssh_execute(node, "rm -rf /usr/include/gandiva")
                    os.system("scp -r /usr/include/gandiva " + node.hostname + ":/usr/include/")
                    ssh_execute(node, "rm -rf /usr/include/arrow")
                    os.system("scp -r /usr/include/arrow " + node.hostname + ":/usr/include/")
                    ssh_execute(node, "rm -rf /usr/include/parquet")
                    os.system("scp -r /usr/include/parquet " + node.hostname + ":/usr/include/")

            #NATIVE_SQL_ENGINE and ARROW_DATASOURCE all need libs
            print(colors.LIGHT_BLUE + "Copy libarrow.so, libgandiva_jni.so, libgandiva.so, libparquet.so, libarrow_dataset.so, libarrow_dataset_jni.so, libprotobuf.so.13, libhdfs3.so to " + node.hostname + "..." + colors.ENDC)
            ssh_execute(node, "rm -f /lib64/libarrow.so")
            ssh_execute(node, "rm -f /lib64/libparquet.so")
            ssh_execute(node, "rm -f /lib64/libarrow_dataset_jni.so")
            ssh_execute(node, "rm -f /lib64/libarrow_dataset.so")

            ssh_copy(node, os.path.join(arrow_build_dir, "release/libarrow.so.17.0.0"), "/lib64/libarrow.so")
            ssh_copy(node, os.path.join(arrow_build_dir, "release/libparquet.so.17.0.0"), "/lib64/libparquet.so")
            ssh_copy(node, os.path.join(arrow_build_dir, "release/libarrow_dataset_jni.so.17.0.0"), "/lib64/libarrow_dataset_jni.so")
            ssh_copy(node, os.path.join(arrow_build_dir, "release/libarrow_dataset.so.17.0.0"), "/lib64/libarrow_dataset.so")

            ssh_execute(node, "chmod 755 /lib64/libarrow.so")
            ssh_execute(node, "chmod 755 /lib64/libparquet.so")
            ssh_execute(node, "chmod 755 /lib64/libarrow_dataset_jni.so")
            ssh_execute(node, "chmod 755 /lib64/libarrow_dataset.so")

            ssh_execute(node, "yum -y install libgsasl")
            ssh_copy(node, os.path.join(package_path, "libprotobuf.so.13"), "/lib64/libprotobuf.so.13")
            ssh_copy(node, os.path.join(package_path, "libhdfs3.so"), os.path.join(beaver_env.get("HADOOP_HOME"), "lib/native/libhdfs.so"))
            # TODO: do we need add /usr/lib64 to /etc/ld.so.conf file ?
            ssh_execute(node, "/usr/sbin/ldconfig")

    if beaver_env.get("OAP_with_DCPMM").lower() == "true":
        # copy persistent-memory.xml to spark folder
        copy_DCPMM_conf_to_spark(master, custom_conf, beaver_env)
        # copy libmemkind.so.0 to workers and install numctl
        memkind_repo = "https://github.com/memkind/memkind.git"
        memkind_code_path = "/opt/Beaver/tmp/memkind"
        if os.path.exists(memkind_code_path):
            remote_memkind_url = subprocess.check_output("cd " + memkind_code_path + "  && git remote -v | grep push | awk '{print $2}' ", shell=True).strip('\r\n')
            if remote_memkind_url == memkind_repo:
                subprocess.check_call("cd " + memkind_code_path + " && git checkout master && git pull", shell=True)
            else:
                subprocess.check_call("rm -rf " + memkind_code_path, shell=True)
                git_clone(memkind_repo, memkind_code_path)
        else:
            git_clone(memkind_repo, memkind_code_path)
        subprocess.check_call("cd " + memkind_code_path + " && git checkout v1.10.1", shell=True)
        subprocess.check_call("cd " + memkind_code_path + " && ./build.sh", shell=True)
        subprocess.check_call("cd " + memkind_code_path + " && make", shell=True)
        subprocess.check_call("cd " + memkind_code_path + " && make install", shell=True)

        vmemcache_repo = "https://github.com/pmem/vmemcache.git"
        vmemcache_code_path = "/opt/Beaver/tmp/vmemcache"
        if os.path.exists(vmemcache_code_path):
            remote_vmemcache_url = subprocess.check_output(
                "cd " + vmemcache_code_path + " && git remote -v | grep push | awk '{print $2}' ", shell=True).strip(
                '\r\n')
            if remote_vmemcache_url == vmemcache_repo:
                subprocess.check_call("cd " + vmemcache_code_path + " && git pull", shell=True)
            else:
                subprocess.check_call("rm -rf " + vmemcache_code_path, shell=True)
                git_clone(vmemcache_repo, vmemcache_code_path)
        else:
            git_clone(vmemcache_repo, vmemcache_code_path)

        try:
            subprocess.check_call("rpm -e --nodeps `rpm -qa | grep libvmemcache`", shell=True)
        except subprocess.CalledProcessError, e:
            print(colors.RED + "libvmemcache is not installed")

        subprocess.check_call("cd " + vmemcache_code_path + " && rm -rf build && mkdir build", shell=True)
        # On RPM-based Linux distros(Fedora, openSUSE, RHEL, SLES
        subprocess.check_call("cd " + vmemcache_code_path + "/build" + " && cmake .. -DCMAKE_INSTALL_PREFIX=/usr -DCPACK_GENERATOR=rpm && make package && rpm -i libvmemcache*.rpm", shell=True)
        rpm_list = subprocess.Popen("cd " + vmemcache_code_path + "/build" + " && ls  libvmemcache*.rpm", shell=True, stdout=subprocess.PIPE)
        rpm_packages = rpm_list.stdout.readlines()

        if beaver_env.get("OAP_with_external").lower() == "true":
            # install arrow and plasma, only install on master
            # TODO: slave will not work
            arrow_deploy(beaver_env)

        for node in slaves:
            print(colors.LIGHT_BLUE + "Copy libmemkind.so.0 to " + node.hostname + "..." + colors.ENDC)
            ssh_copy(node, os.path.join(memkind_code_path, ".libs/libmemkind.so.0"), "/lib64/libmemkind.so.0")
            print(colors.LIGHT_BLUE + "Deploy numactl on " + node.hostname + "..." + colors.ENDC)
            ssh_execute(node, "yum -y install numactl")
            print(colors.LIGHT_BLUE + "install vmemcache to " + node.hostname + "..." + colors.ENDC)
            for rpm_package in rpm_packages:
                rpm_package = rpm_package.strip()
                ssh_copy(node, vmemcache_code_path + "/build/" + rpm_package, "/opt/Beaver/" + rpm_package)
                ssh_execute(node, "rpm -e --nodeps `rpm -qa | grep " + rpm_package + "`")
                ssh_execute(node, "rpm -i /opt/Beaver/" + rpm_package)
            if beaver_env.get("OAP_with_external").lower() == "true":
                print(colors.LIGHT_BLUE + "Copy libarrow.so, libplasma.so, libplasma_java.so, plasma-store-server to " + node.hostname + "..." + colors.ENDC)
                if node.role != "master":
                    ssh_execute(node, "rm -f /lib64/libarrow*")
                    ssh_execute(node, "rm -f /lib64/libplasma*")
                    ssh_execute(node, "rm -f /lib64/libplasma_java*")
                    ssh_execute(node, "rm -f /bin/plasma-store-server")

                    ssh_copy(node, os.path.join(arrow_build_dir, "release/libarrow.so." + arrow_version), "/lib64/libarrow.so")
                    ssh_copy(node, os.path.join(arrow_build_dir, "release/libplasma.so." + arrow_version), "/lib64/libplasma.so")
                    ssh_copy(node, os.path.join(arrow_build_dir, "release/libplasma_java.so"), "/lib64/libplasma_java.so")
                    ssh_copy(node, os.path.join(arrow_build_dir, "release/plasma-store-server"), "/bin/plasma-store-server")

                    ssh_execute(node, "chmod 755 /lib64/libarrow.so")
                    ssh_execute(node, "chmod 755 /lib64/libplasma.so")
                    ssh_execute(node, "chmod 755 /lib64/libplasma_java.so")
                    ssh_execute(node, "chmod 755 /bin/plasma-store-server")
                    # TODO: do we need add /usr/lib64 to /etc/ld.so.conf file ?
                    ssh_execute(node, "/usr/sbin/ldconfig")

def copy_DCPMM_conf_to_spark(master, custom_conf, beaver_env):
    if beaver_env.get("OAP_with_DCPMM").lower() == "true":
        DCPMM_conf_file = os.path.join(custom_conf, "spark/persistent-memory.xml")
        spark_conf_folder = os.path.join(beaver_env.get("SPARK_HOME"), "conf")
        if os.path.isfile(DCPMM_conf_file):
            print(colors.LIGHT_BLUE + "Start to copy persistent-memory.xml to spark config folder" + colors.ENDC)
            ssh_copy(master, DCPMM_conf_file, os.path.join(spark_conf_folder, "persistent-memory.xml"))

def update_spark_config_with_oap(custom_conf, master, beaver_env):
    dict = {}
    oap_jar_path = get_oap_jar_path(beaver_env)
    oap_jar_name = oap_get_jar_name(beaver_env, beaver_env.get("SPARK_VERSION"))
    dict["spark.files"] = os.path.join(oap_jar_path, oap_jar_name)
    dict["spark.executor.extraClassPath"] = os.path.join("./", oap_jar_name)
    dict["spark.driver.extraClassPath"] = os.path.join(oap_jar_path, oap_jar_name)
    spark_output_conf = os.path.join(custom_conf, "output/spark")
    spark_defaults_config = os.path.join(spark_output_conf, "spark-defaults.conf")
    add_config_value(spark_defaults_config, dict, " ")
    print (colors.LIGHT_BLUE + "Update spark-defaults.conf about the location of oap_jar_file" + colors.ENDC)
    ssh_copy(master, spark_defaults_config, os.path.join(beaver_env.get("SPARK_HOME"), "conf/spark-defaults.conf"))


def clean_oap(master, beaver_env):
    ssh_execute(master, "rm -rf " + os.path.join(beaver_env.get("OAP_HOME"), "oap_jar"))

def oap_git_clone_branch(beaver_env, master):
    if beaver_env.has_key("OAP_GIT_REPO"):
        oap_repo = beaver_env.get("OAP_GIT_REPO")
    else:
        print (colors.RED + "Please define OAP_GIT_REPO in env.conf" + colors.ENDC)
        exit(1)

    # Determine whether to clone source code or not
    if os.path.exists(oap_source_code_path):
        remote_oap_url = subprocess.check_output("cd " + oap_source_code_path + " && git remote -v | grep push | awk '{print $2}' ", shell=True).strip('\r\n')
        if remote_oap_url == oap_repo:
            try:
                subprocess.check_call("cd " + oap_source_code_path + " && git checkout master && git pull", shell=True)
            except subprocess.CalledProcessError, e:
                print e
                subprocess.check_call("rm -rf " + oap_source_code_path, shell=True)
                oap_git_clone(oap_repo)
        else:
            subprocess.check_call("rm -rf " + oap_source_code_path, shell=True)
            oap_git_clone(oap_repo)
    else:
        oap_git_clone(oap_repo)

    oap_checkout(beaver_env.get("OAP_BRANCH"))
    try:
        subprocess.check_call("cd " + oap_source_code_path + " && git pull", shell=True)
    except subprocess.CalledProcessError:
        print (colors.RED +  beaver_env.get("OAP_BRANCH") + " is a tag not a branch" + colors.ENDC)


def conda_oap_compile_prepare(beaver_env, master):
    deploy_local_jdk(master, beaver_env)
    deploy_jdk([master], beaver_env)
    deploy_local_maven(master, beaver_env)
    deploy_maven(master, beaver_env)
    oap_git_clone_branch(beaver_env, master)

def oap_compile_prepare(beaver_env, master):
    deploy_local_jdk(master, beaver_env)
    deploy_jdk([master], beaver_env)
    deploy_local_maven(master, beaver_env)
    deploy_maven(master, beaver_env)
    deploy_cmake(master, beaver_env)
    oap_git_clone_branch(beaver_env, master)
    # stop Nailgun server before compiling.
    try:
        nailgun_id = subprocess.check_output("source ~/.bashrc && jps | grep Nailgun", shell = True).strip('\r\n').split()[0]
        if nailgun_id:
            subprocess.check_call("kill " + nailgun_id, shell=True)
    except subprocess.CalledProcessError, e:
        print e

def oap_git_run(cmd):
    cmd = cmd.strip()
    git_command(cmd, oap_source_code_path)

def clean_oap_all(master, beaver_env):
    clean_oap(master, beaver_env)
    ssh_execute(master, "rm -rf " + os.path.join(beaver_env.get("OAP_HOME"), "*"))


if __name__ == '__main__':
    args = sys.argv
    if len(args) < 3:
        print ("Please input: [action] [repo_dir] [plugin]")
    action = args[1]
    custom_conf = args[2]
    plugin = args[3]
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_merged_env(custom_conf)

    if action == "compile":
        if plugin == "oap":
            oap_compile(beaver_env, master)
        if plugin == "conda_oap":
            conda_oap_compile(beaver_env, master)
    elif action == "deploy":
        if plugin == "oap":
            deploy_oap(custom_conf, master, slaves, beaver_env)
        if plugin == "conda_oap":
            deploy_conda_oap_internal(slaves, beaver_env)
    elif action == "undeploy":
        if plugin == "oap":
            clean_oap_all(master, beaver_env)
        if plugin == "conda_oap":
            clean_conda_oap_all(master, beaver_env)
    else:
        print("not support")
