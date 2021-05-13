#!/bin/bash

CURRENT_DIR=$(pwd)
BEAVER_BIN="$(cd $(dirname "${BASH_SOURCE[0]}") >/dev/null && pwd)"

source ${BEAVER_BIN}/start-beaver.sh

# setup bashrc for source start-beaver.sh so that every time we have that environment
HAS_USE_BEAVER=$(grep "\..*start-beaver\.sh" ~/.bashrc)

if [[ -z "$HAS_USE_BEAVER" ]]; then
   echo $'\n' >> ~/.bashrc
   echo ". ${BEAVER_BIN}/start-beaver.sh" >> ~/.bashrc
else
   echo "s/\. .*start-beaver\.sh/\. $(eval echo "$BEAVER_BIN" | sed 's/\//\\\//g')\/start-beaver\.sh/g" > sed.script
   find ~/.bashrc -type f -print0 | xargs -0 sed -i -f sed.script
   rm -f sed.script
fi

rm -f /etc/yum.repos.d/mysql-community.repo
rm -f /etc/yum.repos.d/cm.repo

yum -y install gcc python-devel libffi-devel.x86_64 openssl-devel.x86_64 krb5-devel.x86_64

pexpect_file="${BEAVER_HOME}/package/pexpect-2.3.tar.gz"
if [ ! -f "$pexpect_file" ];
then
    wget -P ${BEAVER_HOME}/package http://$PACKAGE_SERVER/python/pexpect-2.3.tar.gz
fi
cd ${BEAVER_HOME}/package
tar zxvf pexpect-2.3.tar.gz
cd ${BEAVER_HOME}/package/pexpect-2.3
python ./setup.py install

#python $BEAVER_HOME/utils/get-pip.py
#To install Pip for python run the following command
rpm -iUvh https://dl.fedoraproject.org/pub/epel/7/x86_64/Packages/e/epel-release-7-13.noarch.rpm
yum -y install python-pip
pip install --upgrade pip
pip install paramiko==1.17.0
pip install --upgrade python-gssapi
pip install xlwt xlrd
pip install jinja2
#the line below need to double check to see if it works
pip uninstall gssapi

# for impala
pip install prettytable sqlparse thrift

# for git
#pip install gitpython

# for memkind && vmemcache compile oap to support DCPMM
yum -y install autoconf
yum -y install automake
yum -y install gcc-c++
yum -y install libnuma-devel
yum -y install libtool
yum -y install numactl-devel
yum -y install unzip
yum -y install cmake
yum -y install yum-utils
yum -y install rpm-build

# cmake build arrow/plasma && external cache
yum -y install zlib-devel
yum -y install libcurl-devel

#for PAT module
yum -y install gawk
yum -y install sysstat
yum -y install perf
yum -y install python-matplotlib

# pip install --upgrade numpy scipy matplotlib
matplotlib_pat="${BEAVER_HOME}/package/matplotlib_pat.tar.gz"
if [ ! -f "$matplotlib_pat" ];
then
wget -P $BEAVER_HOME/package http://$PACKAGE_SERVER/pat/matplotlib_pat.tar.gz
fi
tar zxvf $BEAVER_HOME/package/matplotlib_pat.tar.gz -C /

pip install XlsxWriter
yum -y install flex bison byacc make gcc git

#for ansilbe module
yum install ansible -y

if [ ! -f "/usr/bin/pssh" ]; then
    wget -P $BEAVER_HOME/package http://$PACKAGE_SERVER/software/pssh-2.3.1.tar.gz
    cd ${BEAVER_HOME}/package
    tar zxf pssh-2.3.1.tar.gz
    cd pssh-2.3.1
    python setup.py build
    python setup.py install
    cd ${BEAVER_HOME}/package
    rm -rf pssh-2.3.1.tar.gz
    rm -rf pssh-2.3.1
fi


sshpass_file="${BEAVER_HOME}/package/sshpass-1.06.tar.gz"
if [ ! -f "sshpass_file" ];
then
    wget -P ${BEAVER_HOME}/package http://sourceforge.net/projects/sshpass/files/sshpass/1.06/sshpass-1.06.tar.gz
fi
cd ${BEAVER_HOME}/package
tar zxvf sshpass-1.06.tar.gz
cd ${BEAVER_HOME}/package/sshpass-1.06
./configure
make && make install

cd $CURRENT_DIR
#compile arrow need
yum - y install python3

#sendmail
yum -y install sendmail
yum -y install mailx
systemctl start sendmail
