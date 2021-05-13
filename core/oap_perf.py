from core.spark import *
from utils.node import *
from core.sbt import *
from core.jdk import *
from core.oap import *

SBT_COMPONENT = "sbt"
OAP_PERF_COMPONENT = "oap-perf"


def deploy_oap_perf(custom_conf, master, slaves, beaver_env):
    clean_oap_perf(master, beaver_env)
    #oap_perf_set_build_script(beaver_env)
    copy_oap_perf_test_script(custom_conf, master, beaver_env)
    oap_perf_jar_dist([master], beaver_env)
    copy_oap_perf_conf(master,custom_conf,beaver_env)
    oap_perf_link_tpcds_tools(master, beaver_env)

def oap_perf_git_clone(oap_perf_repo):
    dst = oapperf_source_code_path
    subprocess.check_call("mkdir -p " + dst, shell=True)
    git_clone(oap_perf_repo, dst)

def oap_perf_compile_prepare(master, beaver_env, oap_perf_repo):
    # Determine whether to clone source code or not
    if os.path.exists(oapperf_source_code_path):
        remote_oap_perf_url = subprocess.check_output("cd " + oapperf_source_code_path + " && git remote -v | grep push | awk '{print $2}' ", shell=True).strip('\r\n')
        if remote_oap_perf_url == oap_perf_repo:
            subprocess.check_call("cd " + oapperf_source_code_path + " && git pull", shell=True)
        else:
            subprocess.check_call("rm -rf " + oapperf_source_code_path, shell=True)
            oap_perf_git_clone(oap_perf_repo)
    else:
        oap_perf_git_clone(oap_perf_repo)
    setup_oap_perf_build_env()
    deploy_local_sbt(master, beaver_env)
    deploy_sbt(master, beaver_env)
    oap_perf_set_build_script(beaver_env)

def oap_perf_git_run(cmd):
    cmd = cmd.strip()
    git_command(cmd, oapperf_source_code_path)

def oap_perf_checkout(checkout_cmd):
    oap_perf_git_run("checkout " + checkout_cmd)

def setup_oap_perf_build_env():
    subprocess.check_call("yum -y install cmake", shell = True)
    subprocess.check_call("yum -y install numactl-devel", shell = True)

def oap_perf_set_build_script(beaver_env):
#    origin_pattern1 = '/home/oap/oap/oap-0.4.0-SNAPSHOT.jar'
    origin_pattern1 = '/home/oap/oap_jars/oap.jar'
    replace_pattern1 = oapperf_source_code_path + "/lib/" + oap_get_jar_name(beaver_env, spark_get_build_version().strip())
    #print replace_pattern1
    origin_pattern2 = '"org.apache.spark" %% "spark-sql" % "2.3.0" % "provided"'
    replace_pattern2 = '"org.apache.spark" %% "spark-sql" % "'+ spark_get_build_version().strip() + '" % "provided"'
    with open(os.path.join(oapperf_source_code_path, "build.sbt")) as f:
        read = f.read()
    read = re.sub(origin_pattern1, replace_pattern1, read)
    read = re.sub(origin_pattern2, replace_pattern2, read)
    with open(os.path.join(oapperf_source_code_path, "build.sbt"), 'w') as f:
        f.write(read)

def oap_perf_compile(master, beaver_env, oap_perf_repo):
    oap_perf_compile_prepare(master, beaver_env, oap_perf_repo)
    subprocess.check_call("cp -f " + build_path + "/" + oap_get_jar_name(beaver_env, spark_get_build_version().strip()) + " " + oapperf_source_code_path + "/lib/", shell=True)
    subprocess.check_call("cd "+ oapperf_source_code_path +";sbt -Dsbt.log.noformat=true --error assembly",shell=True)
    subprocess.check_call("cp -f " + oapperf_source_code_path + "/target/scala-2.11/" + oap_perf_jar_name + " " + build_path + "/", shell=True)

def copy_oap_perf_conf(node, custom_conf, beaver_env):

    oap_perf_output_conf = update_conf(OAP_PERF_COMPONENT, custom_conf)
    oap_perf_conf_file = os.path.join(oap_perf_output_conf, "oap-benchmark-default.conf")
    dst_path = os.path.join(beaver_env.get("OAP_HOME"), "oap-perf/" + beaver_env.get("SPARK_VERSION") + "/src/test/oap-perf-suite/conf/oap-benchmark-default.conf")
    ssh_execute(node, "mkdir -p " + os.path.join(beaver_env.get("OAP_HOME"), "oap-perf/" + beaver_env.get("SPARK_VERSION") + "/src/test/oap-perf-suite/conf"))
    print (colors.LIGHT_BLUE + "\tCopy oap-perf conf to " + node.hostname + "..." + colors.ENDC)
    ssh_copy(node, oap_perf_conf_file, dst_path)

def copy_oap_perf_test_script(custom_conf, node, beaver_env):
    script_folder = os.path.join(tool_path, "oap-perf")
    dst_path = os.path.join(beaver_env.get("OAP_HOME"), "oap-perf")
    #spark_version = spark_get_build_version().strip()
    spark_version = beaver_env.get("SPARK_VERSION")
    dict = oap_perf_gen_test_dict(custom_conf, node, beaver_env, spark_version)
    print (colors.LIGHT_BLUE + "\tCopy oap-perf script to " + node.hostname + "..." + colors.ENDC)
    copy_spark_test_script_to_remote(node, script_folder, dst_path, beaver_env, dict)

def oap_perf_jar_dist(slaves, beaver_env):
    master = get_master_node(slaves)
    for node in slaves:
        print (colors.LIGHT_BLUE + "\tCopy OAP perf jar to " + node.hostname + "..." + colors.ENDC)
        spark_version = beaver_env.get("SPARK_VERSION")
        build_folder = os.path.join(package_path, "build")
        dst_path = os.path.join(beaver_env.get("OAP_HOME"), "oap-perf")
        dst_path += "/" + spark_version.strip()
        oap_cache_test_files = subprocess.check_output(
            "find " + build_folder + " -name \"oap-*-test-jar-with-dependencies.jar\"", shell=True).strip('\r\n')
        if oap_cache_test_files == "":
            oap_compile(beaver_env, master)
            oap_cache_test_files = subprocess.check_output(
                "find " + build_folder + " -name \"oap-*-test-jar-with-dependencies.jar\"", shell=True).strip('\r\n')
        for oap_cache_test_file in oap_cache_test_files.split("\n"):
            oap_cache_test_file_name = os.path.basename(oap_cache_test_file)
            print (colors.LIGHT_BLUE + "\tCopy " + oap_cache_test_file_name + " to " + node.hostname + "..." + colors.ENDC)
            ssh_copy(node, os.path.join(build_folder, oap_cache_test_file_name),os.path.join(dst_path, oap_cache_test_file_name))

def oap_perf_run_test(master, beaver_env):
    oap_perf_gen_data(master, beaver_env)
    oap_perf_run_query(master, beaver_env)

def oap_perf_run_query(custom_conf, master, beaver_env):
    oap_perf_home = os.path.join(beaver_env.get("OAP_HOME"), "oap-perf/" + beaver_env.get("SPARK_VERSION"))
    return ssh_execute(master, "unset $SPARK_HOME;sh " + oap_perf_home + "/oap_perf_test.sh --rerun -d " + os.path.abspath(custom_conf))

def oap_perf_gen_data(master, beaver_env):
    oap_perf_home = os.path.join(beaver_env.get("OAP_HOME"), "oap-perf/" + beaver_env.get("SPARK_VERSION"))
    if oap_perf_gen_data_verfiy(master, beaver_env, oap_perf_home):
        ssh_execute(master, "cd " + oap_perf_home + " ;sh run_data_gen.sh")

def oap_perf_gen_data_verfiy(master, beaver_env, oap_perf_home):
    gen_data = False
    oap_benchmark_default_conf = parse_properties(
        os.path.join(oap_perf_home, "src/test/oap-perf-suite/conf/oap-benchmark-default.conf"), " ")
    spark_default_conf = parse_properties(os.path.join(beaver_env.get("SPARK_HOME"), "conf/spark-defaults.conf"), " ")
    if spark_default_conf.has_key("spark.sql.oap.index.directory"):
        index_root = spark_default_conf["spark.sql.oap.index.directory"].strip("\n").strip(" ")
    else:
        index_root = "/"
    hdfs_file_root_dir = oap_benchmark_default_conf["oap.benchmark.hdfs.file.root.dir"].strip("\n").strip(" ")
    data_scale = oap_benchmark_default_conf["oap.benchmark.tpcds.data.scale"].strip("\n").strip(" ")
    data_path = []
    orc_data_path = os.path.join(hdfs_file_root_dir, "orc" + data_scale)
    orc_index_path = os.path.join(os.path.join(index_root, hdfs_file_root_dir.lstrip("/")), "orc" + data_scale)
    parquet_data_path = os.path.join(hdfs_file_root_dir, "parquet" + data_scale)
    parquet_index_path = os.path.join(os.path.join(index_root, hdfs_file_root_dir.lstrip("/")), "parquet" + data_scale)
    data_path.append(orc_data_path)
    data_path.append(orc_index_path)
    data_path.append(parquet_data_path)
    data_path.append(parquet_index_path)
    for path in data_path:
        status = ssh_execute(master, beaver_env.get("HADOOP_HOME") + "/bin/hadoop  fs -test -e " + path)
        if status != 0:
            gen_data = True
            break
    return gen_data

def oap_perf_link_tpcds_tools(master, beaver_env):
    ssh_execute(master, "mkdir -p /home/oap")
    ssh_execute(master, "ln -s " + beaver_env.get("TPCDS_KIT_HOME") + " /home/oap/tpcds-kit")

def oap_perf_gen_test_dict(custom_conf, master, beaver_env, spark_version):
    dict = {};
    oap_perf_output_conf = update_conf(OAP_PERF_COMPONENT, custom_conf)
    oap_perf_conf_file = os.path.join(oap_perf_output_conf, "oap-benchmark-default.conf")
    config_dict = get_configs_from_properties(oap_perf_conf_file)
    data_scale = config_dict.get("oap.benchmark.tpcds.data.scale").strip()
    data_format = config_dict.get("oap.benchmark.tpcds.data.format").strip()
    data_hdfs_root = config_dict.get("oap.benchmark.hdfs.file.root.dir").strip()

    spark_version = spark_version.strip()
    #oap_version = beaver_env.get("OAP_VERSION")
    dict["{%scale%}"] = data_scale
    dict["{%data.format%}"] = data_format
    dict["{%oap.benchmark.hdfs.file.root.dir%}"] = data_hdfs_root
    #dict["{%oap_version%}"] = oap_version
    dict["{%hostname%}"] = master.hostname
    dict["{%sparksql.perf.home%}"] = beaver_env.get("SPARK_SQL_PERF_HOME")
    dict["{%oap.home%}"] = beaver_env.get("OAP_HOME")
    dict["{%pat.home%}"] = beaver_env.get("PAT_HOME")
    dict["{%spark.major.version%}"] = spark_version.split(".")[0].strip()
    dict["{%spark.mid.version%}"] = spark_version.split(".")[1].strip()
    dict["{%spark.minor.version%}"] = spark_version.split(".")[2].strip()
    dict["{%spark.home%}"] = beaver_env.get("SPARK_HOME")#os.path.join(beaver_env.get("BEAVER_OPT_HOME"), "spark-" + spark_version)
    dict["{%tpcds.home%}"] = beaver_env.get("TPCDS_KIT_HOME")
    dict["{%oap.perf.home%}"] = os.path.join(beaver_env.get("OAP_HOME"), "oap-perf")
    dict["{%tpcds.script.home%}"] = os.path.join(beaver_env.get("OAP_HOME"), "test_script/" + spark_version)
    if int(dict["{%spark.major.version%}"]) < 3:
        dict["{%scala.version%}"] = "2.11"
    else:
        dict["{%scala.version%}"] = "2.12"

    for key, val in dict.items():
        if val == None:
            dict[key] = ""
    return dict

def clean_oap_perf(master, beaver_env):
    ssh_execute(master, "rm -rf " + os.path.join(beaver_env.get("OAP_HOME"), "oap-perf"))
