#!/usr/bin/python

from core.maven import *
from core.oap import *
from core.sbt import *
import pexpect

SPARK_SQL_PERF_COMPONENT = "spark-sql-perf"
TPCDS_COMPONENT = "TPC-DS"
TPCH_COMPONENT = "TPC-H"

def deploy_spark_sql_perf(custom_conf, master, test_mode=[]):
    print("Deploy spark_sql_perf")
    clean_spark_sql_perf(master)
    beaver_env = get_merged_env(custom_conf)
    copy_packages([master], SPARK_SQL_PERF_COMPONENT, beaver_env.get("SPARK_SQL_PERF_VERSION"))
    deploy_sbt(master, beaver_env)
    copy_tpcds_test_script(master, custom_conf, beaver_env, test_mode)
    copy_tpch_test_script(master, custom_conf, beaver_env, test_mode)

def undeploy_spark_sql_perf(master):
    print("Undeploy spark_sql_perf")
    clean_spark_sql_perf(master)

def run_spark_sql_perf(master, custom_conf, beaver_env):
    print (colors.LIGHT_BLUE + "RUN SPARK-SQL-PERF..." + colors.ENDC)
    spark_sql_perf_home = beaver_env.get("SPARK_SQL_PERF_HOME")
    spark_sql_home = beaver_env.get("SPARK_HOME")
    tpcds_kit_home = beaver_env.get("TPCDS_KIT_HOME")
    spark_env_home = os.path.join(custom_conf, "spark")
    ssh_copy(master, spark_env_home + "/spark-env.sh", spark_sql_home + "/conf/spark-env.sh")
    run_tpcds(master, custom_conf, spark_sql_home, spark_sql_perf_home, tpcds_kit_home)
    #ssh_execute(master, "hadoop fs -get /spark/sql/performance/* /opt/Beaver/result/")
    ssh_execute(master, "hadoop fs -get /tpcds_perf/result/part-* /opt/Beaver/result/")

def tpcds_result_copy(master):
    if not os.path.exists("/opt/Beaver/result/"):
        subprocess.check_call("mkdir /opt/Beaver/result/")
    ssh_execute(master, "hadoop fs -get /tpcds_perf/result/part-* /opt/Beaver/result/")


def run_tpcds(master, custom_conf, spark_sql_home, spark_sql_perf_home, tpcds_kit_home):
    tpc_ds_config_file = os.path.join(custom_conf, "TPC-DS/config")
    config_dict = get_configs_from_properties(tpc_ds_config_file)
    scale = config_dict.get("scale")
    data_format = config_dict.get("format")
    table_dir = config_dict.get("table_dir")
    result_dir = config_dict.get("result_dir")
    user = master.username
    ip = master.ip

    test_script = []
    test_script.append('val sqlContext = new org.apache.spark.sql.SQLContext(sc)')
    test_script.append('import sqlContext.implicits._')
    test_script.append('import com.databricks.spark.sql.perf.tpcds.Tables')
    test_script.append('val tables = new Tables(sqlContext, "' + tpcds_kit_home + '/tools", ' + scale + ')')
    test_script.append('tables.genData("hdfs://'+ ip +':9000/' + table_dir +'","' + data_format + '", true, false, false, false, false)')
    test_script.append('tables.createExternalTables("hdfs://' + ip +':9000/' + table_dir + '","' + data_format +'","finaltest", false)')
    test_script.append('import com.databricks.spark.sql.perf.tpcds.TPCDS')
    test_script.append('val timeout = 60 * 60')
    test_script.append('val tpcds = new TPCDS (sqlContext = sqlContext)')
    test_script.append('val experiment = tpcds.runExperiment(tpcds.tpcds1_4Queries, iterations = 1)')
    test_script.append('experiment.waitForFinish(timeout)')
    test_script.append('import org.apache.hadoop.fs.Path')
    test_script.append('import org.apache.hadoop.fs.FileSystem')
    test_script.append('val fs = FileSystem.get(sc.hadoopConfiguration)')
    #test_script.append('if(fs.exits(new Path("' + result_dir + '")))')
    test_script.append('fs.delete(new Path("' + result_dir + '"), true)')
    test_script.append('experiment.getCurrentResults.select("name", "parsingTime", "analysisTime", "optimizationTime", "planningTime", "executionTime").repartition(1).write.format("csv").save("'+ result_dir + '")')
    test_script.append('System.exit(1)')

    tpcds_file_name = "tpcds_test.scala"
    tpcds_file_path = os.path.join(package_path, 'tmp/' + tpcds_file_name)

    with open(tpcds_file_path, "w+") as f:
        for line in test_script:
            f.write(line + '\n')

    ssh_copy(master, tpcds_file_path, os.path.join(spark_sql_perf_home, tpcds_file_name))

    ssh_execute(master, spark_sql_home + '/bin/spark-shell '
                                         '--jars ' + spark_sql_perf_home + '/target/scala-2.11/spark-sql-perf_2.11-0.4.12-SNAPSHOT.jar '
                                        '--conf "spark.driver.extraJavaOptions=-Dhttp.proxyHost=child-prc.intel.com -Dhttp.proxyPort=913 -Dhttps.proxyHost=child-prc.intel.com -Dhttps.proxyPort=913" '
                                        '--packages com.typesafe.scala-logging:scala-logging-slf4j_2.10:2.1.2 --num-executors 8 '
                                       '-i ' + os.path.join(spark_sql_perf_home, tpcds_file_name))

    '''
    pexpect_child = pexpect.spawn('ssh %s@%s' % (user, ip))
    pexpect_child.expect('#')
    pexpect_child.sendline('cd ' + spark_sql_home)
    pexpect_child.sendline('bin/spark-shell --jars ' + spark_sql_perf_home + '/target/scala-2.11/spark-sql-perf_2.11-0.4.12-SNAPSHOT.jar --conf "spark.driver.extraJavaOptions=-Dhttp.proxyHost=child-prc.intel.com -Dhttp.proxyPort=913 -Dhttps.proxyHost=child-prc.intel.com -Dhttps.proxyPort=913" --packages com.typesafe.scala-logging:scala-logging-slf4j_2.10:2.1.2 --num-executors 8')
    #pexpect_child.sendline(':load ' + os.path.join(spark_sql_perf_home, tpcds_file_name))
    pexpect_child.sendline('val sqlContext = new org.apache.spark.sql.SQLContext(sc)')
    pexpect_child.sendline('import sqlContext.implicits._')
    pexpect_child.sendline('import com.databricks.spark.sql.perf.tpcds.Tables')
    pexpect_child.sendline('val tables = new Tables(sqlContext, "' + tpcds_kit_home + '/tools", ' + scale + ')')
    #pexpect_child.sendline('tables.genData("hdfs://'+ ip +':9000/' + table_dir +'","' + data_format + '", true, false, false, false, false)')
    #pexpect_child.sendline('tables.createExternalTables("hdfs://' + ip +':9000/' + table_dir + '","' + data_format +'","finaltest", false)')
    pexpect_child.sendline('import com.databricks.spark.sql.perf.tpcds.TPCDS')
    pexpect_child.sendline('val timeout = 60')
    pexpect_child.sendline('val tpcds = new TPCDS (sqlContext = sqlContext)')
    pexpect_child.sendline('val experiment = tpcds.runExperiment(tpcds.tpcds1_4Queries, iterations = 1)')
    #pexpect_child.sendline('experiment.waitForFinish(timeout)')
    #print pexpect_child.before
    #pexpect_child.sendline('experiment.getCurrentResults.select("name", "parsingTime", "analysisTime", "optimizationTime", "planningTime", "executionTime").write.format("csv").save("'+ result_dir + '")')
    pexpect_child.interact()
    #experiment.getCurrentResults.select("name", "parsingTime", "analysisTime", "optimizationTime", "planningTime", "executionTime").write.format("csv").save("/tpcds_perf/result")
    #pexpect_child.expect('[' + master.username + '@' + master.hostname + ' ~]#*')
    #pass
    '''

def run_tpc_query(master, slaves, beaver_env, iteration, workload):
    if beaver_env.get("RPMEM_shuffle").lower() == "true":
        for node in slaves:
            if node.role != "master":
                pmempool_file_status=ssh_execute(node, "ls /usr/bin/pmempool")
                if pmempool_file_status == 0:
                    ssh_execute(node, "/usr/sbin/ldconfig")
                    repeat_execute_command_dist([node], "ls /dev/dax* | xargs /usr/bin/pmempool rm")
                else:
                    repeat_execute_command_dist([node], "export LD_LIBRARY_PATH=/root/miniconda2/envs/oapenv/lib/:$LD_LIBRARY_PATH; ls /dev/dax* | xargs /root/miniconda2/envs/oapenv/bin/pmempool rm")

    script_folders = get_remote_script_folder_dict(beaver_env, workload)
    arrow_format = ""
    if beaver_env.get("NATIVE_SQL_ENGINE").lower() == "true" or beaver_env.get("ARROW_DATA_SOURCE").lower() == "true":
        arrow_format = "arrow"
    for k, v in script_folders.items():
        print (colors.LIGHT_BLUE + "\tStart to run test..." + colors.ENDC)
        if beaver_env.get("THROUGHPUT_TEST").lower() == "true":
            status = ssh_execute(master, "unset SPARK_HOME;cd " + v + ";sh run_" + workload + "_sparksql_throughput_test.sh " + str(
                iteration) + " " + arrow_format)
        else:
            status = ssh_execute(master, "unset SPARK_HOME;cd " + v + ";sh run_" + workload + "_sparksql.sh " + str(
                iteration) + " " + arrow_format)
        log_folder = os.path.join(v, workload+ "/logs")
        result_folder = os.path.join(beaver_env.get("BEAVER_OPT_HOME"), "result/" + workload + "/logs-" + str(time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime())))
        ssh_execute(master, "mkdir -p " + result_folder)
        ssh_execute(master, "cp -rf " + log_folder + "/* " + result_folder)
    return status

def run_tpc_query_validation(master, slaves, beaver_env, workload, runType):
    if beaver_env.get("RPMEM_shuffle").lower() == "true":
        for node in slaves:
            if node.role != "master":
                pmempool_file_status=ssh_execute(node, "ls /usr/bin/pmempool")
                if pmempool_file_status == 0:
                    ssh_execute(node, "/usr/sbin/ldconfig")
                    repeat_execute_command_dist([node], "ls /dev/dax* | xargs /usr/bin/pmempool rm")
                else:
                    repeat_execute_command_dist([node], "export LD_LIBRARY_PATH=/root/miniconda2/envs/oapenv/lib/:$LD_LIBRARY_PATH; ls /dev/dax* | xargs /root/miniconda2/envs/oapenv/bin/pmempool rm")
    script_folders = get_remote_script_folder_dict(beaver_env, workload)
    for k, v in script_folders.items():
        print (colors.LIGHT_BLUE + "\tStart to run " + workload + " test with validation..." + colors.ENDC)
        ssh_execute(master, "unset SPARK_HOME;cd " + v + ";sh run_" + workload + "_sparksql_validation.sh " + runType)
        log_folder = os.path.join(v, workload+ "/logs_validation")
        result_folder = os.path.join(beaver_env.get("BEAVER_OPT_HOME"), "result/" + workload + "/logs-validation-" + str(time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime())))
        ssh_execute(master, "mkdir -p " + result_folder)
        ssh_execute(master, "cp -rf " + log_folder + "/* " + result_folder)

def gen_tpc_data(master, custom_conf, beaver_env, workload):
    if gen_tpc_data_verify(master, custom_conf, beaver_env, workload):
        script_folders = get_remote_script_folder_dict(beaver_env, workload)
        for k, v in script_folders.items():
            print (colors.LIGHT_BLUE + "\tStart to generate " + workload + " data..." + colors.ENDC)
            ssh_execute(master, "unset SPARK_HOME;cd " + v + ";sh run_" + workload + "_datagen.sh")

def gen_tpc_data_verify(master, custom_conf, beaver_env, workload):
    gen_data = False
    config_dict = {}
    if workload == "tpcds":
        output_conf_dir = update_conf(TPCDS_COMPONENT, custom_conf)
        tpc_ds_config_file = os.path.join(output_conf_dir, "config")
        config_dict = get_configs_from_properties(tpc_ds_config_file)
    elif workload == "tpch":
        output_conf_dir = update_conf(TPCH_COMPONENT, custom_conf)
        tpc_h_config_file = os.path.join(output_conf_dir, "config")
        config_dict = get_configs_from_properties(tpc_h_config_file)

    spark_version = beaver_env.get("SPARK_VERSION")
    data_scale = config_dict.get("scale").strip()
    data_format = config_dict.get("format").strip()
    spark_major_version = spark_version.split(".")[0].strip()
    spark_mid_version = spark_version.split(".")[1].strip()

    data_path = "/" + workload + "_" +  data_format + "/" + data_scale
    status = ssh_execute(master, beaver_env.get("HADOOP_HOME") + "/bin/hadoop  fs -test -e " + data_path)
    if status != 0:
        gen_data = True
    return gen_data

def copy_tpcds_test_script(node, custom_conf, beaver_env, test_mode=[]):
    script_folder = os.path.join(tool_path, "tpcds/tpcds_script")
    dst_path = os.path.join(beaver_env.get("SPARK_SQL_PERF_HOME"), "tpcds_script")
    spark_version = beaver_env.get("SPARK_VERSION")
    dict = gen_test_dict(node, custom_conf, beaver_env, spark_version, test_mode)
    print (colors.LIGHT_BLUE + "\tCopy tpcds script to " + node.hostname + "..." + colors.ENDC)
    copy_spark_test_script_to_remote(node, script_folder, dst_path, beaver_env, dict)
    sql_tar_name = "tpcds99queries.tar.gz"
    sql_tar_folder = os.path.join(dst_path, spark_version + "/tpcds/tpcds-queries")
    ssh_execute(node, "tar -C " + sql_tar_folder +" -xf " + os.path.join(sql_tar_folder, sql_tar_name))
    ssh_execute(node, "rm -f " + os.path.join(sql_tar_folder, sql_tar_name))

def copy_tpch_test_script(node, custom_conf, beaver_env, test_mode=[]):
    script_folder = os.path.join(tool_path, "tpch/tpch_script")
    dst_path = os.path.join(beaver_env.get("SPARK_SQL_PERF_HOME"), "tpch_script")
    spark_version = beaver_env.get("SPARK_VERSION")
    dict = gen_tpch_test_dict(node, custom_conf, beaver_env, spark_version, test_mode)
    print (colors.LIGHT_BLUE + "\tCopy tpch script to " + node.hostname + "..." + colors.ENDC)
    copy_spark_test_script_to_remote(node, script_folder, dst_path, beaver_env, dict)
    sql_tar_name = "tpch22queries.tar.gz"
    sql_tar_folder = os.path.join(dst_path, spark_version + "/tpch/tpch-queries")
    ssh_execute(node, "tar -C " + sql_tar_folder + " -xf " + os.path.join(sql_tar_folder, sql_tar_name))
    ssh_execute(node, "rm -f " + os.path.join(sql_tar_folder, sql_tar_name))

def get_remote_script_folder_dict(beaver_env, workload):
    spark_version = beaver_env.get("SPARK_VERSION")
    script_path = os.path.join(beaver_env.get("SPARK_SQL_PERF_HOME"), workload + "_script")
    result = {}
    result[spark_version.strip()] = os.path.join(script_path, spark_version.strip())
    return result

def gen_test_dict(master, custom_conf ,beaver_env, spark_version, mode=[]):
    dict = {};
    output_conf_dir = update_conf(TPCDS_COMPONENT, custom_conf)
    tpc_ds_config_file = os.path.join(output_conf_dir, "config")
    config_dict = get_configs_from_properties(tpc_ds_config_file)
    data_scale = config_dict.get("scale").strip()
    data_format = config_dict.get("format").strip()
    tables_partition = config_dict.get("partitionTables").strip()
    #table_dir = config_dict.get("table_dir").strip()
    #result_dir = config_dict.get("result_dir").strip()
    spark_version = spark_version.strip()
    queries = config_dict.get("queries").strip()
    dict["{%useDoubleForDecimal%}"] = config_dict.get("useDoubleForDecimal").strip()
    dict["{%partitionTables%}"] = tables_partition
    dict["{%scale%}"] = data_scale
    dict["{%data.format%}"] = data_format
    dict["{%hostname%}"] = master.hostname
    dict["{%oap.home%}"] = beaver_env.get("OAP_HOME")
    dict["{%pat.home%}"] = beaver_env.get("PAT_HOME")
    dict["{%spark.major.version%}"] = spark_version.split(".")[0].strip()
    dict["{%spark.mid.version%}"] = spark_version.split(".")[1].strip()
    dict["{%spark.minor.version%}"] = spark_version.split(".")[2].strip()
    dict["{%spark.home%}"] = beaver_env.get("SPARK_HOME")#os.path.join(beaver_env.get("BEAVER_OPT_HOME"), "spark-" + spark_version)
    dict["{%tpcds.home%}"] = beaver_env.get("TPCDS_KIT_HOME")
    dict["{%sparksql.perf.home%}"] = beaver_env.get("SPARK_SQL_PERF_HOME")
    dict["{%tpcds.script.home%}"] = os.path.join(beaver_env.get("SPARK_SQL_PERF_HOME"), "tpcds_script/" + spark_version)#os.path.join(beaver_env.get("OAP_HOME"), "test_script/" + spark_version)

    if int(dict["{%spark.major.version%}"]) < 3:
        dict["{%scala.version%}"] = "2.11"
    else:
        dict["{%scala.version%}"] = "2.12"

    if queries == 'all':
        dict["{%queries%}"] = ""
        for i in range(1, 100):
            dict["{%queries%}"] = dict["{%queries%}"] + str(i) + " "
    else:
        dict["{%queries%}"] = queries.replace(",", " ")

    if spark_version == "2.3.0" and "oap" in mode and data_format == "orc":
        dict["{%scala.spark.2.3.0.oap.orc%}"] = ""
    else:
        dict["{%scala.spark.2.3.0.oap.orc%}"] = "//"

    if "oap" in mode and data_format == "parquet":
        dict["{%scala.spark.oap.parquet%}"] = ""
    else:
        dict["{%scala.spark.oap.parquet%}"] = "//"

    for key, val in dict.items():
        if val == None:
            dict[key] = ""

    return dict

def gen_tpch_test_dict(master, custom_conf ,beaver_env, spark_version, mode=[]):
    dict = {};
    output_conf_dir = update_conf(TPCH_COMPONENT, custom_conf)
    tpc_h_config_file = os.path.join(output_conf_dir, "config")
    config_dict = get_configs_from_properties(tpc_h_config_file)
    data_scale = config_dict.get("scale").strip()
    data_format = config_dict.get("format").strip()
    ifPartitioned = config_dict.get("partitionTables").strip()
    tables_partition = config_dict.get("partition").strip()
    spark_version = spark_version.strip()
    queries = config_dict.get("queries").strip()
    dict["{%partitionTables%}"] = ifPartitioned
    dict["{%scale%}"] = data_scale
    dict["{%data.format%}"] = data_format
    dict["{%partition%}"] = tables_partition
    dict["{%hostname%}"] = master.hostname
    dict["{%spark.version%}"] = spark_version
    dict["{%spark.major.version%}"] = spark_version.split(".")[0].strip()
    dict["{%spark.mid.version%}"] = spark_version.split(".")[1].strip()
    dict["{%spark.minor.version%}"] = spark_version.split(".")[2].strip()
    dict["{%spark.home%}"] = beaver_env.get("SPARK_HOME")#os.path.join(beaver_env.get("BEAVER_OPT_HOME"), "spark-" + spark_version)
    dict["{%tpch.home%}"] = beaver_env.get("TPCH_DBGEN_HOME")
    dict["{%sparksql.perf.home%}"] = beaver_env.get("SPARK_SQL_PERF_HOME")
    dict["{%tpch.script.home%}"] = os.path.join(beaver_env.get("SPARK_SQL_PERF_HOME"), "tpch_script/" + spark_version)#os.path.join(beaver_env.get("OAP_HOME"), "test_script/" + spark_version)

    if int(dict["{%spark.major.version%}"]) < 3:
        dict["{%scala.version%}"] = "2.11"
    else:
        dict["{%scala.version%}"] = "2.12"

    if queries == 'all':
        dict["{%queries%}"] = ""
        for i in range(1, 23):
            dict["{%queries%}"] = dict["{%queries%}"] + str(i) + " "
    else:
        dict["{%queries%}"] = queries.replace(",", " ")

    for key, val in dict.items():
        if val == None:
            dict[key] = ""

    return dict

def clean_spark_sql_perf(master):
    ssh_execute(master, "rm -rf /opt/Beaver/spark-sql-perf*")

