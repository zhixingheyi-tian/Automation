#!/usr/bin/python

from utils.util import *
from utils.ssh import *
from utils.node import *

PAT_COMPONENT = "pat"

def deploy_pat(custom_conf, master):
    clean_pat(master)
    beaver_env = get_merged_env(custom_conf)
    cluster_file = get_cluster_file(custom_conf)
    slaves = get_slaves(cluster_file)
    copy_packages([master], PAT_COMPONENT, beaver_env.get("PAT_VERSION"))
    pat_config_dist(master, slaves, custom_conf,  beaver_env)

def clean_pat(master):
    ssh_execute(master, "rm -rf /opt/Beaver/pat*")

#looks like this only copy config file rather than merge the property so we have to set the property in base conf folder
def populate_pat_conf(custom_conf):
    output_conf = os.path.join(custom_conf, "output")
    return copy_conf_tree(custom_conf, PAT_COMPONENT, output_conf)

def pat_config_dist(master, slaves, config_path,  beaver_env, extra_dict = None):
    populate_pat_conf(config_path)
    collect_config_output_folder = os.path.join(config_path, "output/pat/PAT-collecting-data")
    collect_dst_path = os.path.join(beaver_env.get("PAT_HOME"), "PAT-collecting-data")

    post_config_output_folder = os.path.join(config_path, "output/pat/PAT-post-processing")
    post_dst_path = os.path.join(beaver_env.get("PAT_HOME"), "PAT-post-processing")

    dict = pat_gen_replace_dict(slaves, beaver_env)
    if extra_dict != None:
        for k, v in extra_dict.items():
            dict[k] = v

    collect_output_folder_star = collect_config_output_folder + "/*"
    final_collect_config_files = glob.glob(collect_output_folder_star)
    for file in final_collect_config_files:
        replace_conf_value(file, dict)
        print (colors.LIGHT_BLUE + "\tCopy PAT collecting config to " + master.hostname + "..." + colors.ENDC)
        ssh_copy(master, file, os.path.join(collect_dst_path, os.path.basename(file)))

    post_output_folder_star = post_config_output_folder + "/*"
    post_collect_config_files = glob.glob(post_output_folder_star)
    for file in post_collect_config_files:
        replace_conf_value(file, dict)
        print (colors.LIGHT_BLUE + "\tCopy PAT post procesing config to " + master.hostname + "..." + colors.ENDC)
        ssh_copy(master, file, os.path.join(post_dst_path, os.path.basename(file)))

def pat_gen_replace_dict(slaves, beaver_env):
    dict = {};
    str = ""
    for slave in slaves:
        str = str + slave.ip + " "
    dict["{%nodes.ip%}"] = str.strip()
    dict["{%oap.home%}"] = beaver_env.get("OAP_HOME")
    dict["{%pat.home%}"] = beaver_env.get("PAT_HOME")

    for key, val in dict.items():
        if val == None:
            dict[key] = ""

    return dict

def run_pat(master, slaves, beaver_env, script_path, flag = None):
    abs_script_path = os.path.abspath(script_path.strip())
    dst_pat_path = os.path.join(beaver_env.get("PAT_HOME"), "PAT-collecting-data")
    customized_script_folder = os.path.join(dst_pat_path, "user-script")
    ssh_execute(master, "mkdir -p " + customized_script_folder)
    if os.path.isfile(abs_script_path):
        dst_script = os.path.join(customized_script_folder, os.path.basename(script_path))
        ssh_copy(master, abs_script_path, dst_script)
        ssh_execute(master, "chmod +x " + dst_script)
        result_name = os.path.basename(abs_script_path)[:-3] + "_result_" + time.strftime("%Y-%m-%d_%H:%M:%S",time.localtime())
        result_path = os.path.join(dst_pat_path, "results/" + result_name + "/instruments")
        dict = {}
        dict["{%pat.script.path%}"] = dst_script
        dict["{%pat.result.path%}"] = result_path
        if flag == 'exclude' :
            dict["{%exclude-master%}"] = master.ip
            pat_config_dist(master, slaves, config_path, beaver_env, extra_dict=dict)
        ssh_execute(master, "cd " +dst_pat_path + ";sh ./pat run " + result_name)
        post_run_path = os.path.join(beaver_env.get("PAT_HOME"), "PAT-post-processing")
        ssh_execute(master, "cd " +post_run_path + ";python pat-post-process.py")
        pat_result_dir = os.path.join(beaver_env.get("BEAVER_OPT_HOME"),"result/pat")
        ssh_execute(master, "mkdir -p " + pat_result_dir)
        ssh_execute(master, "zip -r " + os.path.join(pat_result_dir, result_name) +".zip " + os.path.join(dst_pat_path, "results/" + result_name))
        ssh_execute(master, "rm -rf " + os.path.join(dst_pat_path, "results/" + result_name))

    else:
        print(colors.RED + "Can't find pointed file" + colors.ENDC)


