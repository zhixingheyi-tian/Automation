from utils.run_workflow_utils import *
from utils.setup_cluster import *
from utils.html_generator import HtmlGenerator


def collect_conda_build_result(version_list, receivers_list):
    package_collect_date = time.strftime("%Y-%m-%d-%H:%M:%S", time.localtime())
    target_path = "conda-build/endtime-" + package_collect_date
    build_folder = os.path.join(package_path, target_path)
    conda_build_path = "/root/miniconda2/conda-bld/linux-64/"
    package_list = get_files_by_suffix(conda_build_path, "tar.bz2")
    subprocess.check_call("mkdir -p " + build_folder, shell=True)
    package_name_list = []
    for package in package_list:
        if os.path.basename(package).startswith("oap"):
            subprocess.check_call("cp " + package + " " + build_folder, shell=True)
            package_name_list.append(os.path.basename(package))
    subprocess.check_call("rm -f " + conda_build_path + "*.tar.bz2", shell=True)
    html_path = process_conda_build_result(version_list, target_path, package_name_list)
    scp_path_to_history_server(package_path, target_path)
    subject = "Conda build result:" + package_collect_date
    sendmail(subject, html_path, receivers_list)
    return package_name_list


def process_conda_build_result(version_list, target_path, package_name_list):
    res_url = "http://" + os.environ.get("PACKAGE_SERVER", "10.239.44.95") + "/" + target_path
    html_content = []
    for version in version_list:
        result_dict = {"version": version}
        for package in package_name_list:
            if package.startswith("oap-" + version):
                result_dict["status"] = "Success"
                result_dict["color"] = "green"
        if "status" not in result_dict.keys():
            result_dict["status"] = "Fail"
            result_dict["color"] = "red"
        result_dict["link"] = res_url
        html_content.append(result_dict)
    return HtmlGenerator.generate_by_dict(html_content, os.path.join(package_path, target_path), 'conda_build_template.html',
                                   'conda_build_result.html')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='manual to this script')
    parser.add_argument('--confs', type=str, default="")
    args = parser.parse_args()

    confs = args.confs.strip().strip("\n").split(",")
    if len(confs) == 0:
        print "Please input right workflow"
        exit(1)

    target_list = []
    receivers_list = []
    for conf in confs:
        cluster_file = get_cluster_file(conf)
        slaves = get_slaves(cluster_file)
        master = get_master_node(slaves)
        beaver_env = get_merged_env(conf)
        receivers_list.extend(beaver_env.get("OAP_EMAIL_RECEIVER").strip().split(","))
        conda_oap_compile(beaver_env, master)
        if beaver_env.has_key("CONDA_OAP_VERSION"):
            conda_oap_version = beaver_env.get("CONDA_OAP_VERSION")
            target_list.append(conda_oap_version)
    receivers_list = list(set(receivers_list))
    collect_conda_build_result(target_list, receivers_list)
