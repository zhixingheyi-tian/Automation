#!/usr/bin/python

from core.jdk import *
import fileinput

PRESTO_COMPONENT="presto"
def deploy_presto(custom_conf, master, slaves, beaver_env):
    # Deploy Presto
    deploy_jdk(slaves, beaver_env)
    stop_presto_service(slaves)
    deploy_presto_internal(custom_conf, master, slaves)

def undeploy_presto(slaves):
    stop_presto_service(slaves)
    clean_presto(slaves)

def stop_presto_service(slaves):
    print (colors.LIGHT_BLUE + "Stop presto related services, it may take a while..." + colors.ENDC)
    for node in slaves:
        ssh_execute(node, "$PRESTO_HOME/bin/launcher stop")

def clean_presto(slaves):
    for node in slaves:
        ssh_execute(node, "rm -rf /opt/Beaver/presto*")
        ssh_execute(node, "source ~/.bashrc")

def deploy_presto_internal(custom_conf, master, slaves):
    beaver_env = get_merged_env(custom_conf)
    clean_presto(slaves)
    setup_env_dist(slaves, beaver_env, PRESTO_COMPONENT)
    copy_packages(slaves, PRESTO_COMPONENT, beaver_env.get("PRESTO_VERSION"))
    update_copy_presto_conf(custom_conf, master, slaves)
    stop_firewall(slaves)
    #download presto command line interface
    ssh_execute(master, "wget --no-proxy -P /opt/Beaver/presto/ "+"http://" + download_server + "/presto/presto")
    # download presto benchmark driver
    ssh_execute(master, "wget --no-proxy -P /opt/Beaver/presto/ " + "http://" + download_server + "/presto/presto-benchmark-driver")

def update_copy_presto_conf(custom_conf, master, slaves):
    output_presto_conf = update_conf(PRESTO_COMPONENT, custom_conf)
    totalmemory = 0
    for node in slaves:
        totalmemory += calculate_memory(node)
    for node in slaves:
        if node is master:
            conf_path = "/opt/Beaver/presto/" + str(time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime()))
            cmd = "mkdir -p " + conf_path + "/catalog;ln -s " + conf_path + " /opt/Beaver/presto/etc"
            ssh_execute(node, cmd)
            ssh_copy(node, os.path.join(output_presto_conf, "log.properties"), "/opt/Beaver/presto/etc/log.properties")
            for file in os.listdir(output_presto_conf):
                if file.__eq__("jvm.config"):
                    replace_line_in_bak(os.path.join(output_presto_conf, file), os.path.join(output_presto_conf, file + ".bak"), {"-XmxxxG":"-Xmx" + str(calculate_memory(node)) + "G"})
                    ssh_copy(node, os.path.join(output_presto_conf, file + ".bak"), "/opt/Beaver/presto/etc/jvm.config")
                if file.__eq__("config.properties"):
                    replace_line_in_bak(os.path.join(output_presto_conf, file), os.path.join(output_presto_conf, file + ".bak"), {"${xx}":str(totalmemory*0.5), "${xxx}":str(calculate_memory(node)*0.5), "master_hostname":master.ip})
                    ssh_copy(node, os.path.join(output_presto_conf, file + ".bak"), "/opt/Beaver/presto/etc/config.properties")
                if file.__eq__("node.properties"):
                    replace_line_in_bak(os.path.join(output_presto_conf, file), os.path.join(output_presto_conf, file + ".bak"), {"xxxxxx":node.hostname})
                    ssh_copy(node, os.path.join(output_presto_conf, file + ".bak"), "/opt/Beaver/presto/etc/node.properties")
                if file.__eq__("suite.json"):
                    tpc_ds_config_file = os.path.join(custom_conf, "TPC-DS/config")
                    config_array = get_configs_from_properties(tpc_ds_config_file)
                    scale = config_array.get("scale")
                    data_format = config_array.get("format")
                    replace_line_in_bak(os.path.join(output_presto_conf, file),os.path.join(output_presto_conf, file + ".bak"), {"{fileformat}":data_format, "{scale}":scale})
                    ssh_copy(node, os.path.join(output_presto_conf, file + ".bak"),"/opt/Beaver/presto/suite.json")
                if file.__eq__("hive.properties"):
                    replace_line_in_bak(os.path.join(output_presto_conf, file), os.path.join(output_presto_conf, file + ".bak"), {"{master_hostname}":node.hostname})
                    ssh_copy(node, os.path.join(output_presto_conf, file + ".bak"),"/opt/Beaver/presto/etc/catalog/hive.properties")
        else:
            conf_path = "/opt/Beaver/presto/" + str(time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime()))
            cmd = "mkdir -p " + conf_path + "/catalog;ln -s " + conf_path + " /opt/Beaver/presto/etc"
            ssh_execute(node, cmd)
            ssh_copy(node, os.path.join(output_presto_conf, "log.properties"), "/opt/Beaver/presto/etc/log.properties")
            for file in os.listdir(output_presto_conf):
                if file.__eq__("jvm.config"):
                    replace_line_in_bak(os.path.join(output_presto_conf, file), os.path.join(output_presto_conf, file + ".bak"), {"-XmxxxG":"-Xmx" + str(calculate_memory(node)) + "G"})
                    ssh_copy(node, os.path.join(output_presto_conf, file + ".bak"), "/opt/Beaver/presto/etc/jvm.config")
                if file.__eq__("nodeconfig.properties"):
                    replace_line_in_bak(os.path.join(output_presto_conf, file), os.path.join(output_presto_conf, file + ".bak"), {"${xx}":str(totalmemory * 0.5), "${xxx}":str(calculate_memory(node) * 0.5), "master_hostname":master.ip})
                    ssh_copy(node, os.path.join(output_presto_conf, file + ".bak"), "/opt/Beaver/presto/etc/config.properties")
                if file.__eq__("node.properties"):
                    replace_line_in_bak(os.path.join(output_presto_conf, file), os.path.join(output_presto_conf, file + ".bak"), {"xxxxxx":node.hostname})
                    ssh_copy(node, os.path.join(output_presto_conf, file + ".bak"), "/opt/Beaver/presto/etc/node.properties")
                if file.__eq__("hive.properties"):
                    # replace_line_in_bak(os.path.join(output_presto_conf, file), os.path.join(output_presto_conf, file + ".bak"), {"{master_hostname}":node.hostname})
                    ssh_copy(node, os.path.join(output_presto_conf, file + ".bak"),"/opt/Beaver/presto/etc/catalog/hive.properties")

def replace_line_in_bak(infilepath, outfilepath, replacements):
    with open(infilepath) as infile, open(outfilepath, 'w') as outfile:
        for line in infile:
            for old,new in replacements.iteritems():
                line = line.replace(old, new)
            outfile.write(line)

def calculate_memory(node):
    cmd = "cat /proc/meminfo | grep \"MemTotal\""
    stdout = ssh_execute_withReturn(node, cmd)
    for line in stdout:
        memory = int(int(line.split()[1]) / 1024 / 1024 * 0.8)
    return memory

def start_presto_service(slaves):
    print (colors.LIGHT_BLUE + "Start presto related services, it may take a while..." + colors.ENDC)
    for node in slaves:
        ssh_execute(node, "$PRESTO_HOME/bin/launcher start")

def run_presto_tpc_ds(master, beaver_env, custom_conf):
    presto_tpcds_kit = beaver_env.get("presto_tpcds_kit")
    if(presto_tpcds_kit == "TRUE"):
        cmd = "rm -rf /opt/Beaver/presto/sql;mkdir -p /opt/Beaver/presto/sql;"
        cmd += "yes|ln -s /opt/Beaver/presto_tpcds_kit/* /opt/Beaver/presto/sql"
    else:
        cmd = "rm -rf /opt/Beaver/presto/sql;mkdir -p /opt/Beaver/presto/sql;"
        cmd += "yes|cp -r /opt/Beaver/TPC-DS/sample-queries-tpcds/query*.sql /opt/Beaver/presto/sql/;"
        cmd += "sed -i 's/;//g' /opt/Beaver/presto/sql/query*.sql"

    ssh_execute(master,cmd)
    tpc_ds_result = os.path.join(beaver_env.get("TPC_DS_RES_DIR"), str(time.strftime("%Y-%m-%d-%H-%M-%S",time.localtime())))
    tpc_ds_config_file = os.path.join(custom_conf, "TPC-DS/config")
    config_array = get_configs_from_properties(tpc_ds_config_file)
    master_port = config_array.get("presto_master_port")
    cmd = "chmod +x /opt/Beaver/presto/presto-benchmark-driver;cd /opt/Beaver/presto;sleep 15;nohup ./presto-benchmark-driver --server "+master.ip+":"+master_port+" --catalog hive --runs 1 --warm 0 >> " + tpc_ds_result + " &"
    ssh_execute(master,cmd)
