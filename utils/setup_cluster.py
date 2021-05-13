import sys
import os
from util import *
from node import *
from core.jdk import *

def usage():
    print("Usage: set-cluster.py [path/to/conf]")
    exit(1)

def setup_cluster(conf_path) :
    cluster_file = get_cluster_file(conf_path)
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    setup_ssh_keys(master, slaves)

if __name__ == '__main__':
    args = sys.argv
    if len(args) < 2:
        usage()
    
    conf_path = os.path.abspath(args[1])
    
    setup_cluster(conf_path)
    
