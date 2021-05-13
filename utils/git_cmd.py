import os
import subprocess
from utils.colors import *
def git_clone(repo, target_dir):

    subprocess.check_call("git clone " + repo + " " + target_dir, shell=True)

def git_command(cmd, target_dir):
    try:
        subprocess.check_call("cd " + target_dir + " && git " + cmd, shell=True)
    except Exception, e:
        print e

def git_repo_check(repo_url, repo_code_path, branch):
    if os.path.exists(repo_code_path):
        remote_url = subprocess.check_output("cd " + repo_code_path + "  && git remote -v | grep push | awk '{print $2}' ", shell=True).strip('\r\n')
        if remote_url == repo_url:
            subprocess.check_call("cd " + repo_code_path + " && git checkout master && git pull", shell=True)
        else:
            subprocess.check_call("rm -rf " + repo_code_path, shell=True)
            git_clone(repo_url, repo_code_path)
    else:
        git_clone(repo_url, repo_code_path)
    subprocess.check_call("cd " + repo_code_path + " && git checkout " + branch, shell=True)
    try:
        subprocess.check_call("cd " + repo_code_path + " && git pull", shell=True)
    except subprocess.CalledProcessError:
        print (colors.RED + branch + " is a tag not a branch" + colors.ENDC)