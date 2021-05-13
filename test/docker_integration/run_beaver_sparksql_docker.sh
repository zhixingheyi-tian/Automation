project_name=$1
image_name=$2
project_path=$3

repo_url=http://$PACKAGE_SERVER
os_repo_path=$repo_url/repodata/os.repo
pip_conf=$repo_url/repodata/pip.conf

##delete container if exit slave or master
docker ps -a|grep -E 'slave|master' |awk '{print $1}'|xargs docker stop;
docker ps -a|grep -E 'slave|master' |awk '{print $1}'|xargs docker rm;

##run image for master
masterId=$(docker run --privileged --name=master -dti -v /sys/fs/cgroup:/sys/fs/cgroup:ro -p 50002:22 $image_name)
echo "containerId(master):$masterId"
master_hostname=$(docker ps -a|grep master|awk '{print $1}')
echo "hostname:$master_hostname"
master_ip=$(docker inspect --format='{{.NetworkSettings.IPAddress}}' $masterId|awk '{print $1}')
echo "master IP:$master_ip"

wget -O /opt/os.repo $os_repo_path
docker cp  $project_path/$project_name $masterId:/home/
docker cp /opt/os.repo $masterId:/etc/yum.repos.d/

##add pip mirror
wget -O /opt/pip.conf $pip_conf
docker exec $masterId mkdir ~/.pip
docker cp /opt/pip.conf $masterId:~/.pip/

##run image for slave1
slave1Id=$(docker run --privileged --name=slave1 -dti -v /sys/fs/cgroup:/sys/fs/cgroup:ro -p 50003:22 $image_name)
echo "containerId(slave1):$slave1Id"
slave1_hostname=$(docker ps -a|grep slave1|awk '{print $1}')
echo "hostname:$slave1_hostname"
slave1_ip=$(docker inspect --format='{{.NetworkSettings.IPAddress}}' $slave1Id|awk '{print $1}')
echo "slave1 IP:$slave1_ip"

##The master need several seconds to start sshserver
sleep 30s

/usr/bin/expect<<-EOF
set timeout 7200
set pass bdpe123
spawn sed -i "/$master_ip/d" /root/.ssh/known_hosts
spawn ssh $master_ip
expect {
        "(yes/no)" {send "yes\r"; exp_continue}
        "password:" {send "bdpe123\r"; exp_continue}
        "~]#" {send "source /home/$project_name/bin/setup-env.sh\r"}
}
expect {
        "~]#" {send "cp -r /home/$project_name/conf/  /opt/sqlconf;echo \"$master_hostname $master_ip root bdpe123 master\" > /opt/sqlconf/nodes.conf;echo \"$slave1_hostname $slave1_ip root bdpe123 slave\" >> /opt/sqlconf/nodes.conf;sed -i 's/SPARK_VERSION=2.0.0/SPARK_VERSION=2.0.0-hive/g' /opt/sqlconf/env.conf;sed -i 's/power_test_0=1-30/power_test_0=1-30/g' /opt/sqlconf/BB/conf/bigBench.properties\r"}
}
expect {
        "~]#" {send "sed -i 's/{\%yarn.nodemanager.resource.memory-mb\%}/20480/g' /opt/sqlconf/hadoop/yarn-site.xml;sed -i 's/{\%yarn.nodemanager.resource.cpu-vcores\%}/30/g' /opt/sqlconf/hadoop/yarn-site.xml;sed -i 's/{\%yarn.scheduler.maximum-allocation-mb\%}/20000/g' /opt/sqlconf/hadoop/yarn-site.xml;sed -i 's/opt/tmp/g' /opt/sqlconf/hadoop/hdfs-site.xml;sed -i 's/opt/tmp/g' /opt/sqlconf/hadoop/yarn-site.xml\r"}
}
expect {
	"~]#" {send "echo \"\rspark.master yarn\">>/opt/sqlconf/spark/spark-defaults.conf;echo \"spark.deploy.mode client\">>/opt/sqlconf/spark/spark-defaults.conf;echo \"spark.sql.hive.metastore.version 1.2.1\">>/opt/sqlconf/spark/spark-defaults.conf;echo \"spark.sql.warehouse.dir hdfs:\/\/master_hostname:9000\/spark-warehouse\">>/opt/sqlconf/spark/spark-defaults.conf;sed -i 's%<value>3<\/value>%<value>1<\/value>%g' /opt/sqlconf/hadoop/hdfs-site.xml;sed -i '/<configuration>/a<property><name>hive.metastore.warehouse.dir<\/name><value>hdfs:\/\/master_hostname:9000\/user\/hive\/warehouse<\/value><\/property>' /opt/sqlconf/hive/hive-site.xml;cp -r /opt/sqlconf/hive/hive-site.xml /opt/sqlconf/spark/;sed -i '/<configuration>/a<property><name>mapreduce.jobtracker.http.address<\/name><value>master_hostname:50030<\/value><\/property>' /opt/sqlconf/hadoop/mapred-site.xml;sed -i '/<configuration>/a<property><name>mapred.job.tracker<\/name><value>master_hostname:9001<\/value><\/property>' /opt/sqlconf/hadoop/mapred-site.xml;sed -i 's/local\\\\\[\\\\\*\\\\\]/yarn/g' /opt/sqlconf/BB/engines/spark_sql/conf/engineSettings.conf;sed -i 's%\\\$SPARK_HOME%/opt/Beaver/spark%g' /opt/sqlconf/BB/engines/spark_sql/conf/engineSettings.conf;sed -i 's%spark-submit%\/opt\/Beaver\/spark\/bin\/spark-submit%g' /opt/sqlconf/BB/engines/hive/conf/engineSettings.conf;sed -i 's%hive%spark_sql%g' /opt/sqlconf/BB/conf/userSettings.conf\r"}
}
expect {
        "~]#" {send "rm -rf /etc/yum.repos.d/CentOS-*;cd /home/$project_name/;benchmark/BBonSparkSQL.py deploy_and_run /opt/sqlconf/ -pat\r"}
}
expect {
        "]#" {send "cd /home/$project_name/test/;source test_utils.sh;service_check;cp log.txt /opt/Beaver/result/;cat log.txt;exit\r"}
}
expect eof
EOF

##copy the result to localhost
nowdate=$(date +%Y-%-m-%-d-%-H-%-M-%-S)
mkdir -p /opt/Beaver/result/docker/
docker cp $masterId:/opt/Beaver/result/ /opt/Beaver/result/docker/$nowdate
