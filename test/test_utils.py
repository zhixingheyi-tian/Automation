#!/usr/bin/python

import os

def check_service():
    check_service_jps="/opt/Beaver/jdk/bin/jps"
    NameNode_flg = False
    DataNode_flg = False
    NodeManager_flg = False
    ResourceManager_flg = False
    RunJar_flg = False
    HistoryServer_flg = False
    JobHistoryServer_flg = False
    SecondaryNameNode_flg = False
    for line in os.popen(check_service_jps).readlines():
        service_line = line.strip('\r\n')
        service_split=service_line.split(" ")
        service_name=service_split[1]
        if service_name == "NameNode":
            NameNode_flg = True
        elif service_name == "DataNode":
            DataNode_flg = True
        elif service_name == "NodeManager":
            NodeManager_flg = True
        elif service_name == "ResourceManager":
            ResourceManager_flg = True
        elif service_name == "RunJar":
            RunJar_flg = True
        elif service_name == "HistoryServer":
            HistoryServer_flg = True
        elif service_name == "JobHistoryServer":
            JobHistoryServer_flg = True
        elif service_name == "SecondaryNameNode":
            SecondaryNameNode_flg = True

    if NameNode_flg & DataNode_flg & NodeManager_flg & ResourceManager_flg & RunJar_flg & HistoryServer_flg & JobHistoryServer_flg & SecondaryNameNode_flg:
        return 0
    else:
        f=file("log.txt","a+")
        if NameNode_flg == False:
            f.write("NameNode did not start\n")
        if DataNode_flg == False:
            f.write("DataNode did not start\n")
        if NodeManager_flg == False:
            f.write("NodeManager did not start\n")
        if ResourceManager_flg == False:
            f.write("ResourceManager did not start\n")
        if RunJar_flg == False:
            f.write("Metastore did not start\n")
        if HistoryServer_flg == False:
            f.write("HistoryServer did not start\n")
        if JobHistoryServer_flg == False:
            f.write("JobHistoryServer did not start\n")
        if SecondaryNameNode_flg == False:
            f.write("SecondaryNameNode did not start\n")
        f.close()
        return 1
