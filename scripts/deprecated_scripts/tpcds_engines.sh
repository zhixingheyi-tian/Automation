#!/bin/bash
#parameter=$(echo $1|sed 's/,/ /g')
path=$(dirname $0)
path=${path/\./$(pwd)}
backupdir="/opt/New_emerging_SQL_comparative_analysis"
PATDIR="/opt/Beaver/pat"
BEAVER_PATH="${path}/Beaver"
QUERY_DIR="${BEAVER_PATH}/repo/TPCDS_99/99queries"
log_dir="/opt/Beaver/result/logs"
hive_home="/opt/Beaver/hive"
export PYTHONPATH=$PYTHONPATH:$BEAVER_PATH
hostname=$(hostname)
check_run(){
    network_speed=$(pssh -i -h /opt/Beaver/hadoop/etc/hadoop/slaves "ip addr|grep 'state UP'|tail -n 1|awk -F[:] '{print\$2}'|xargs ethtool|grep 'Speed:*.10000Mb'"|grep 'Speed:*.10000Mb'|wc -l)
    echo $network_speed
    datanode_status=$(hdfs dfsadmin -report|grep 'Decommission Status'|wc -l)
    echo $datanode_status
    if [ ${hostname} = 'bdpe101' ];then
        num=7
    elif [ ${hostname} = 'sr542' ];then
        num=8
    else
        num=1
    fi
    if [ X$network_speed != X${num} ]; then
        echo 'some datanode Network Speed not 10000Mb!'
        exit 0
    fi
    if [ X$datanode_status != X${num} ]; then
        echo "datanode is down"
        exit 0
    fi
    mount |grep $backupdir > /dev/null
    if [ $? -eq 1 ]; then
        echo "mount samba client data /opt/New_emerging_SQL_comparative_analysis" 1>&2
        mount -t cifs  //$PACKAGE_SERVER/New_emerging_SQL_comparative_analysis/ /opt/New_emerging_SQL_comparative_analysis/ -o username=smbuser,password=bdpe123
        backupdir="$backupdir/`hostname`"
        mkdir -p $backupdir
    fi
    #ansible bdpe -m shell -a 'ntpdate 10.239.47.162'
    ansible bdpe -m shell -a 'echo 3 > /proc/sys/vm/drop_caches && swapoff -a'
    ansible bdpe -m shell -a "lsof |grep "PAT.*deleted"|awk '{print\$2}'|xargs -I{} kill -9 {}"
}


stop_server(){

    slider stop llap_service
    unset SPARK_HOME
    /opt/Beaver/spark-Phive/sbin/stop-thriftserver.sh
    ps -ef|grep impalad|grep -v 'grep' > /dev/null
    if [ $? -eq 0 ]; then
        ansible bdpe -m shell -a 'source $IMPALA_HOME/bin/impala-config.sh && $IMPALA_HOME/bin/start-impala-cluster.py --force_kill'
    fi
    wait
    ansible bdpe -m shell -a '$PRESTO_HOME/bin/launcher stop'
    wait
}
function pat_run() {
    # cmd=`echo ${@:1}`
    #query=$2
    #echo $query
    #engine=$engine
    # echo $cmd,$engine
    date=$(date +"%Y-%m-%d-%H-%M")
    cmd="${BEAVER_PATH}/shell/run_tpcds_engines.sh $engine $query"
    if [ ${query} ];then
        backuplogdir="$backupdir/logs/queries/q${query}/${date}/${engine}"
    else
        backuplogdir="$backupdir/logs/${engine}/${date}"
    fi
    mkdir -p ${backuplogdir}
    echo $cmd
    #exit 0
    sed -i "/^CMD_PATH:/cCMD_PATH: $cmd" $PATDIR/PAT-collecting-data/config
    cd $PATDIR/PAT-collecting-data && ./pat run $engine
    wait
    echo $L

    pdfdir="<source>$PATDIR/PAT-collecting-data/results/${engine}/instruments/</source>"
    sed -i "/<source>/c$pdfdir" $PATDIR/PAT-post-processing/config.xml
    cd $PATDIR/PAT-post-processing && ./pat-post-process.py
    # wait
    cp $PATDIR/PAT-collecting-data/results/$engine/instruments/PAT-Result.pdf ${backuplogdir}/${engine}_${date}.pdf
    if [ ${query} ];then
        mv ${log_dir}/* ${backuplogdir}/
        #mv ${log_dir}/result.log ${backuplogdir}/${engine}_${date}_result.log
        find $BEAVER_PATH/repo/TPCDS_99/ -maxdepth 1 -iname ${engine}|xargs -I{} cp -rf {} ${backuplogdir}/${engine}_${date}_config
    else
        mv $log_dir/* ${backuplogdir}/
        find $BEAVER_PATH/repo/TPCDS_99/ -maxdepth 1 -iname ${engine}|xargs -I{} cp -rf {} $backupdir/logs/${engine}/${date}/
        wait
        python ${BEAVER_PATH}/tools/make_xlsx.py ${engine} ${date}
    fi

}

impala_log() {
    for host in bdpe101 bdpe102 bdpe71 bdpe72 bdpe81 bdpe82 bdpe91 bdpe92;do
        mkdir -p $backupdir/logs/Impala/${date}/Full_log/${host}
        scp root@${host}:/opt/Beaver/impala/logs/cluster/*.log* $backupdir/logs/Impala/${date}/Full_log/${host}
    done
}

run_Impala(){
    # ansible bdpe -m shell -a 'rm -rf /opt/Beaver/impala/logs/cluster/*'
    # mkdir -p $backupdir/logs/Impala/${date}/{Full_log,Query_log}
    $BEAVER_PATH/cluster/Impala.py update_and_run $BEAVER_PATH/repo/TPCDS_99/
    wait
    sleep 1m
    pat_run $engine $query
    # impala_log
}

run_Presto() {

    $BEAVER_PATH/cluster/Presto.py update_and_run $BEAVER_PATH/repo/TPCDS_99/
    wait
    sleep 1m
    pat_run $engine $query
}
run_Sparksql() {
    ansible bdpe -m shell -a 'rm -f /opt/Beaver/hadoop/share/hadoop/yarn/lib/spark-yarn-shuffle.jar'
    $HADOOP_HOME/sbin/stop-yarn.sh
    $HADOOP_HOME/sbin/start-yarn.sh
    sleep 30s
    unset SPARK_HOME
    /opt/Beaver/spark-Phive/sbin/start-thriftserver.sh
    sleep 1m
    #$BEAVER_PATH/benchmark/TPCDSonSparkSQL.py update_and_run $BEAVER_PATH/repo/TPCDS_99/
    pat_run $engine $query
}
run_Spark() {
    ansible bdpe -m shell -a 'ln -s /opt/Beaver/spark/spark-2.0.2-yarn-shuffle.jar /opt/Beaver/hadoop/share/hadoop/yarn/lib/spark-yarn-shuffle.jar'
    $HADOOP_HOME/sbin/stop-yarn.sh
    $HADOOP_HOME/sbin/start-yarn.sh
    sleep 1m
    pat_run $engine $query

}

run_TEZ() {
    pat_run $engine $query

}

run_LLAP() {
    #sed -i "/^set hive.execution.engine/cset hive.execution.engine=tez;" $BEAVER_PATH/repo/TPCDS_99/llap/testbench.settings
    \cp $BEAVER_PATH/repo/TPCDS_99/llap/hive-site.xml ${hive_home}/conf/
    sh /opt/Beaver/llap_run.sh
    wait
    pat_run $engine $query
#    wait
#    \cp $BEAVER_PATH/repo/TPCDS_99/llap/hive-site.xml.map /opt/Beaver/hive/conf/hive-site.xml
#    sh /opt/Beaver/llap_run.sh
#    sleep 1m
#    pat_run $engine $query
}

query=$2
echo $query
if [ $1 ];then
    engines=$(echo $1|sed 's/,/ /g')
    for engine in $engines; do
        logfile="${log_dir}/$(date +%F)_${engine}_${2}.txt"
        check_run > $logfile 2>&1
        if [ ! ${query} ];then
            stop_server >> $logfile 2>&1
            #sleep 30m
        fi
        run_$engine $query >> $logfile 2>&1
        wait
    done
else
    echo '$0 [Impala,Presto,Sparksql,LLAP,Spark,TEZ] [22,33]'
fi
