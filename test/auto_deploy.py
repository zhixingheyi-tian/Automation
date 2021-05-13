#!/usr/bin/python

import os
import sys
import socket

#get this node IP
def get_local_IP():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    return s.getsockname()[0]

#get this node hostname
def get_local_hostname():
    check_this_hostname = "hostname"
    this_hostname=os.popen(check_this_hostname).readline().strip('\r\n')
    return this_hostname

#cover slave.custom
def auto_deploy_slave(file_path):
    deploy_slave_master = get_local_hostname()+" "+get_local_IP() +" root bdpe123 master"
    slave_custom_fld=file_path+"/nodes.conf"
    os.system("echo "+deploy_slave_master+" > "+slave_custom_fld)

#change engine
def auto_deploy_engine(engine_name,file_path):
    engine_conf_fld=file_path+"/BB/conf/userSettings.conf"
    f = open(engine_conf_fld,"r+")
    engine_conf_act="cat "+engine_conf_fld
    engine_conf_in=traverse_file(engine_conf_act)
    replace_name="export BIG_BENCH_DEFAULT_ENGINE"
    engine_flg=False
    if engine_name == "hive":
        for line in engine_conf_in:
            if replace_name in line:
                line = replace_name + "=\"hive\"\n"
            f.writelines(line)
        engine_flg = True
    elif engine_name=="spark":
        for line in engine_conf_in:
            if replace_name in line:
                line = replace_name + "=\"spark_sql\"\n"
            f.writelines(line)
        engine_flg=True
    else:
        for line in engine_conf_in:
            if replace_name in line:
                line = replace_name + "=\""+engine_name+"\"\n"
            f.writelines(line)
        engine_flg=True
    f.close()
    return engine_flg

def traverse_file(file_act):
    file_in=[]
    for line in os.popen(file_act).readlines():
        file_in.append(line)
    return file_in


if __name__ == '__main__':
    slave_custom_cat="cat ../conf/nodes.conf"
    slave_custom_default=os.popen(slave_custom_cat).readline().strip('\r\n')
    conf_deploy_act="mkdir /home/custom;cp -rf ../conf/* /home/custom"
    conf_deploy_act1="mkdir /home/custom1;cp -rf ../conf/* /home/custom1"
    args = sys.argv
    if len(args)<2:
        engine_name = "hive"
        os.system("if [ -d /home/custom ]; then rm -rf /home/custom; fi")
        os.system(conf_deploy_act)
        engine_flg = auto_deploy_engine(engine_name,"/home/custom")
        auto_deploy_slave("/home/custom")

        engine_name1 = "spark"
        os.system("if [ -d /home/custom1 ]; then rm -rf /home/custom1; fi")
        os.system(conf_deploy_act1)
        engine_flg1 = auto_deploy_engine(engine_name1,"/home/custom1")
        auto_deploy_slave("/home/custom1")
    else:
        engine_name = args[1]
        os.system("if [ -d /home/custom ]; then rm -rf /home/custom; fi")
        os.system(conf_deploy_act)
        engine_flg=auto_deploy_engine(engine_name,"/home/custom")
        auto_deploy_slave("/home/custom")
