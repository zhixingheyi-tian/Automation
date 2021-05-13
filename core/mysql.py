from utils.util import *
from utils.ssh import *

package = "mysql-community-release-el7-9.noarch.rpm"
package57 = "mysql57-community-release-el7-9.noarch.rpm"
mysql_repo_url =  "https://dev.mysql.com/get"


def install_mysql(node, user, password):
    download_url = "http://" + download_server + "/" + "mysql"
    download_package = os.path.join(package_path, package)
    if not os.path.isfile(download_package):
        os.system("wget -P " + package_path + " " + download_url + "/" + package)
    ssh_copy(node, download_package, "/opt/" + package)

    repo_package = "mysql-community.repo"
    download_url = "http://" + download_server + "/" + "mysql"
    download_package = os.path.join(package_path, repo_package)
    if not os.path.isfile(download_package):
        os.system("wget -P " + package_path + " " + download_url + "/" + repo_package)
    ssh_copy(node, download_package, "/etc/yum.repos.d/" + repo_package)

    cmd = "rpm -qa | grep mysql-community-server;"
    installed = ssh_execute_withReturn(node, cmd)
    # For mysql5.7, command "mysqladmin -u username password pass" is not effective.
    install_cmd = "cd /opt;rpm -ivh " + package + ";yum -y install mysql-community-server;" \
                  + "systemctl start mysqld;mysqladmin -u " + user + " password " + password
    if "mysql-community-server" in installed:
        cmd = "systemctl stop mysqld;yum -y remove mysql-*;rm -rf /var/lib/mysql;"
        cmd += install_cmd
    else:
        cmd = install_cmd
    ssh_execute(node, cmd)
    cmd_grant_privilege = "mysql -u root -p123456 -Bse \"GRANT ALL PRIVILEGES ON *.* TO '" + user \
                          + "'@'" + node.hostname + "' IDENTIFIED BY '" + password + "' with grant option;FLUSH PRIVILEGES;\""
    ssh_execute(node, cmd_grant_privilege)


def deploy_mysql(master, custom_conf):
    hive_site_name = "hive/hive-site.xml"
    hive_site_file = os.path.join(custom_conf, hive_site_name)
    output_hive_conf_file = os.path.join(custom_conf, "output/temp", hive_site_name)
    merge_xml_properties_tree(custom_conf, hive_site_file, output_hive_conf_file)
    username = get_config_value(output_hive_conf_file, "javax.jdo.option.ConnectionUserName")
    password = get_config_value(output_hive_conf_file, "javax.jdo.option.ConnectionPassword")
    install_mysql(master, username, password)

def install_mysql_without_localftp(node, user, password, isv57=False):
    cmd = "rpm -qa | grep mysql-community-server;"
    installed = ssh_execute_withReturn(node, cmd)
    print (colors.LIGHT_BLUE + "Prepare to install mysql" + colors.ENDC)
    if len(installed) != 0:
        old_mysql = ""
        for item in installed:
            old_mysql = old_mysql + ' ' + item
        print (colors.LIGHT_BLUE + "Removing old mysql: " + old_mysql + colors.ENDC)
        remove_mysql(node)

    download_package = os.path.join(package_path, package57)
    if not os.path.isfile(download_package):
        subprocess.check_call("wget -P " + package_path + " " + mysql_repo_url + "/" + package57, shell=True)
    ssh_copy(node, download_package, "/opt/" + package57)

    ssh_execute(node, "cd /opt;rpm -ivh " + package57)

    if isv57:
        ssh_execute(node, "yum-config-manager --disable mysql56-community")
        ssh_execute(node, "yum-config-manager --enable mysql57-community")
    else:
        ssh_execute(node, "yum-config-manager --disable mysql57-community")
        ssh_execute(node, "yum-config-manager --enable mysql56-community")

    print (colors.LIGHT_BLUE + "Start to download and install mysql" + colors.ENDC)
    ssh_execute(node, "yum -y install mysql-community-server")
    print (colors.LIGHT_BLUE + "Start mysql service" + colors.ENDC)
    ssh_execute(node, "systemctl start mysqld")

    print (colors.LIGHT_BLUE + "Changing initial password and grant privilege" + colors.ENDC)
    if isv57:
        cmd = "grep 'temporary password' /var/log/mysqld.log"
        temppass_fdbk = ssh_execute_withReturn(node, cmd)
        temppass = temppass_fdbk[0].split(":")[-1].strip()
        ssh_execute(node, 'mysqladmin -u root -p\'' + temppass + '\' password \'Password4$\'')
        ssh_execute(node, 'mysql -u root -p\'Password4$\' -e \'SET GLOBAL validate_password_policy=0\'')
        ssh_execute(node, 'mysql -u root -p\'Password4$\' -e \'SET GLOBAL validate_password_length=1\'')
        ssh_execute(node, 'mysql -u root -p\'Password4$\' -e \'SET GLOBAL validate_password_mixed_case_count=0\'')
        ssh_execute(node, 'mysql -u root -p\'Password4$\' -e \'SET GLOBAL validate_password_number_count=0\'')
        ssh_execute(node, 'mysql -u root -p\'Password4$\' -e \'SET GLOBAL validate_password_special_char_count=0\'')
        ssh_execute(node, 'mysqladmin -u root -p\'Password4$\' password \'' + password + '\'')
    else:
        ssh_execute(node, 'mysqladmin -u root password \'' + password + '\'')

    grant_privilege_cmd = 'GRANT ALL PRIVILEGES ON *.* TO \'' + user + '\'@\'%\' IDENTIFIED BY \'' + password + '\' with grant option;'
    ssh_execute(node, 'mysql -u root -p\'' + password + '\' -e \"' + grant_privilege_cmd + '\"')

    grant_privilege_cmd = 'GRANT ALL PRIVILEGES ON *.* TO \'' + user + '\'@\'' + node.hostname + '\' IDENTIFIED BY \'' + password + '\' with grant option;'
    ssh_execute(node, 'mysql -u root -p\'' + password + '\' -e \"' + grant_privilege_cmd + '\"')

def deploy_mysql_without_localftp(master, custom_conf, isv57=False):
    hive_site_name = "hive/hive-site.xml"
    hive_site_file = os.path.join(custom_conf, hive_site_name)
    output_hive_conf_file = os.path.join(custom_conf, "output/temp", hive_site_name)
    merge_xml_properties_tree(custom_conf, hive_site_file, output_hive_conf_file)
    username = get_config_value(output_hive_conf_file, "javax.jdo.option.ConnectionUserName")
    password = get_config_value(output_hive_conf_file, "javax.jdo.option.ConnectionPassword")
    install_mysql_without_localftp(master, username, password, isv57)

def remove_mysql(node):
    ssh_execute(node, "yum -y remove mysql-*")
    ssh_execute(node, "yum -y remove mysql57-*")
    ssh_execute(node, "rm -rf /var/lib/mysql")
    ssh_execute(node, "rm -rf /var/log/mysqld.log")

