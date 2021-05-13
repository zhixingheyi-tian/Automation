import paramiko
import os
import select
import time
import util
import subprocess
import socket
from  utils.node import *

def ssh_execute(node, cmd):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.load_system_host_keys()
    ssh.connect(node.ip, username=node.username,password=node.password)
    # ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command(cmd)
    channel = ssh.get_transport().open_session()
    stdin, stdout, stderr = ssh.exec_command(cmd)
    exit_status=stdout.channel.recv_exit_status()
    for line in stdout:
        print ('....' + line.strip('\n'))
    while not stdout.channel.exit_status_ready():
        if stdout.channel.recv_ready():
            rl, wl, xl = select.select([channel], [], [], 0.0)
            if len(rl) > 0:
                print (channel.recv(1024))
    ssh.close()
    return exit_status

def ssh_execute_withReturn(node, cmd):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.load_system_host_keys()
    ssh.connect(node.ip, username=node.username,password=node.password)
    # ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command(cmd)
    channel = ssh.get_transport().open_session()
    stdin, stdout, stderr = ssh.exec_command(cmd)
    res = []
    for l in stdout.readlines():
        res.append(str(l).strip())
    ssh.close()
    return res


def ssh_execute_forMetastore(node, cmd):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.load_system_host_keys()
    ssh.connect(node.ip, username=node.username,password=node.password)
    # ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command(cmd)
    channel = ssh.get_transport().open_session()
    stdin, stdout, stderr = ssh.exec_command(cmd)
    while not stdout.channel.exit_status_ready():
        if stdout.channel.recv_ready():
            rl, wl, xl = select.select([channel], [], [], 0.0)
            if len(rl) > 0:
                print (channel.recv(1024))
    # print "Metastore is starting, it may take a while..."
    time.sleep(10)
    ssh.close()

def ssh_copy(node, src, dst):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.load_system_host_keys()
    ssh.connect(node.ip, username=node.username, password=node.password)
    sftp = ssh.open_sftp()
    sftp.put(src, dst)
    sftp.close()
    ssh.close()

def ssh_download(node, remote_path, local_path):
    transport = paramiko.Transport((node.ip,22))
    transport.connect(username=node.username, password=node.password)
    sftp = paramiko.SFTPClient.from_transport(transport)
    sftp.get(remote_path, local_path)
    transport.close()

#looks like it remote has to be absolute path
def ssh_copy_from_remote(node, remote, local):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.load_system_host_keys()
    ssh.connect(node.ip, username=node.username, password=node.password)
    sftp = ssh.open_sftp()
    sftp.get(remote, local)
    sftp.close()
    ssh.close()

def setup_ssh_keys_history_server(master):
    history_server = get_history_server()
    ssh_execute(master, "ssh-keyscan -t ecdsa-sha2-nistp256 " + history_server.ip + ">> ~/.ssh/known_hosts")
    ssh_execute(master, "ssh-keyscan -t ecdsa-sha2-nistp256 " + history_server.hostname + ">> ~/.ssh/known_hosts")
    tmp_pubkey_name = "id_rsa_" + master.hostname + ".pub"
    ssh_execute(history_server, "rm -f ~/.ssh/" + tmp_pubkey_name)
    ssh_copy(history_server, "/root/.ssh/id_rsa.pub", "/" + master.username + "/.ssh/" + tmp_pubkey_name)
    ssh_execute(history_server, "cat ~/.ssh/" + tmp_pubkey_name + " >> ~/.ssh/authorized_keys")
    ssh_execute(history_server, "rm -f ~/.ssh/" + tmp_pubkey_name)

def setup_ssh_keys(master, slaves):
    local_hostname = socket.gethostname()
    #set up ssh key for local pc to master pc if local pc is not master pc
    if local_hostname != master.hostname:
        master_host_str = subprocess.check_output("ssh-keyscan -t ecdsa-sha2-nistp256 " + master.ip, shell=True)
        is_master_inknownfile = False
        if os.path.exists('/root/.ssh/known_hosts'):
            with open("/root/.ssh/known_hosts", "r") as f:
                for line in f:
                    if line.strip("/r/n").strip() == master_host_str.strip("/r/n").strip():
                        is_master_inknownfile = True

        if not is_master_inknownfile:
            subprocess.check_output("ssh-keyscan -t ecdsa-sha2-nistp256 " + master.ip + ">> ~/.ssh/known_hosts", shell=True)

        if not os.path.exists('~/.ssh/id_rsa') or not os.path.exists('~/.ssh/id_rsa.pub'):
            subprocess.check_call("ssh-add -D", shell=True)
            subprocess.check_call("rm -f ~/.ssh/id_rsa", shell=True)
            subprocess.check_call("rm -f ~/.ssh/id_rsa.pub", shell=True)
            subprocess.check_call("ssh-keygen -f ~/.ssh/id_rsa -t rsa -N ''", shell=True)
            subprocess.check_call("ssh-add", shell=True)
        ssh_execute(master, "mkdir ~/.ssh")
        ssh_copy(master, '/root/.ssh/id_rsa.pub', "/" + master.username + "/.ssh/id_rsa_" + local_hostname + ".pub")
        ssh_execute(master, "cat ~/.ssh/id_rsa_" + local_hostname + ".pub>> ~/.ssh/authorized_keys")
        ssh_execute(master, "rm -f ~/.ssh/id_rsa_" + local_hostname + ".pub")

    # set up ssh key for master pc to slaves pc
    ssh_execute(master, "ssh-keyscan -t ecdsa-sha2-nistp256 " + master.hostname + " > ~/.ssh/known_hosts")
    for node in slaves:
        #subprocess.check_call("ssh-keyscan -t ecdsa-sha2-nistp256 " + node.ip + ">> ~/.ssh/known_hosts", shell=True)
        ssh_execute(master, "ssh-keyscan -t ecdsa-sha2-nistp256 " + node.ip + ">> ~/.ssh/known_hosts")
        ssh_execute(master, "ssh-keyscan -t ecdsa-sha2-nistp256 " + node.hostname + ">> ~/.ssh/known_hosts")
    ssh_execute(master, "ssh-keyscan -t ecdsa-sha2-nistp256 0.0.0.0 >> ~/.ssh/known_hosts")
    ssh_execute(master, "ssh-keyscan -t ecdsa-sha2-nistp256 127.0.0.1 >> ~/.ssh/known_hosts")
    ssh_execute(master, "ssh-keyscan -t ecdsa-sha2-nistp256 localhost >> ~/.ssh/known_hosts")

    subprocess.check_call("mkdir -p " + os.path.join(util.project_path, "package/tmp/"), shell=True)
    prikey_name = "id_rsa"
    pubkey_name = "id_rsa.pub"
    tmp_pubkey_name = "id_rsa_" + master.hostname + ".pub"
    tmp_pubkey_path = os.path.join(util.project_path, "package/tmp/" + tmp_pubkey_name)
    ssh_execute(master, "rm -f ~/.ssh/" + prikey_name)
    ssh_execute(master, "rm -f ~/.ssh/" + pubkey_name)
    ssh_execute(master, "ssh-keygen -f ~/.ssh/" + prikey_name +" -t rsa -N ''")
    ssh_copy_from_remote(master, "/" + master.username + "/.ssh/" + pubkey_name, tmp_pubkey_path)
    for node in slaves:
        print "Setting SSH key for " + node.hostname
        ssh_execute(node, "mkdir ~/.ssh")
        ssh_execute(node, "rm -f ~/.ssh/" + tmp_pubkey_name)
        ssh_copy(node, tmp_pubkey_path, "/" + master.username + "/.ssh/" + tmp_pubkey_name)
        ssh_execute(node, "cat ~/.ssh/" + tmp_pubkey_name + " >> ~/.ssh/authorized_keys")
        ssh_execute(node, "rm -f ~/.ssh/" + tmp_pubkey_name)

    setup_ssh_keys_history_server(master)
    if local_hostname == master.hostname:
        try:
            subprocess.check_call("ssh-agent bash", shell=True)
            subprocess.check_call("ssh-add", shell=True)
        except subprocess.CalledProcessError, e:
            print e
    subprocess.check_call("rm -f " + tmp_pubkey_path, shell=True)

def quick_clean_ssh_keys(nodes):
    for node in nodes:
        ssh_execute(node, "rm -r -f ~/.ssh/*")
