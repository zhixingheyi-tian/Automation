#!/bin/bash
#parameter=$(echo $1|sed 's/,/ /g')
path=$(dirname $0)
path=${path/\./$(pwd)}
#echo $path
date=$(date +"%Y-%m-%d-%H-%M")
backupdir="/opt/New_emerging_SQL_comparative_analysis"
PATDIR="/opt/Beaver/pat"
BEAVER_PATH="/home/feiren/Beaver"
QUERY_DIR="${BEAVER_PATH}/repo/TPCDS_99/99queries"

log_dir="/opt/Beaver/result/logs"


export PYTHONPATH=$PYTHONPATH:$BEAVER_PATH
check_run(){
    #network_speed=$(pssh -i -h /opt/Beaver/hadoop/etc/hadoop/slaves 'ethtool ens802f0'|grep 'Speed:*.10000Mb'|wc -l)
    #echo $network_speed
    #if [ X$network_speed != 'X7' ]; then
    #    echo 'some datanode ens802f0 Speed not 10000Mb!'
    #    exit 0
    #fi
    #mount |grep $backupdir > /dev/null
    #if [ $? -eq 1 ]; then
    #    echo "mount samba client data /opt/New_emerging_SQL_comparative_analysis" 1>&2
    #    mount -t cifs  //$PACKAGE_SERVER/New_emerging_SQL_comparative_analysis /opt/New_emerging_SQL_comparative_analysis/ -o username=smbuser,password=bdpe123
    #fi
    #ansible bdpe -m shell -a 'ntpdate 10.239.47.162'
    ansible bdpe -m shell -a 'echo 3 > /proc/sys/vm/drop_caches && swapoff -a'
#    ansible bdpe -m shell -a "lsof |grep "PAT.*deleted"|awk '{print\$2}'|xargs -I{} kill -9 {}"
}
stop_server(){

    slider stop llap_service
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

    mkdir -p $backupdir/logs/$engine/${date}_tun_q${query}
    cmd="${BEAVER_PATH}/shell/run_tpcds_engines.sh $engine $query"
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
    cp $PATDIR/PAT-collecting-data/results/$engine/instruments/PAT-Result.pdf $backupdir/logs/${engine}/${date}_tun_q${query}/${engine}_${date}.pdf
    mv $log_dir/* $backupdir/logs/${engine}/${date}_tun_q${query}/
    # python ${BEAVER_PATH}/shell/format_ds_result.py $backupdir/logs/${engine}/${date}/
    # wait
    # mv $PATDIR/PAT-collecting-data/results/${engine}{,_$date}
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
    # /opt/Beaver/spark-Phive/sbin/start-thriftserver.sh
    # sleep 1m
    #$BEAVER_PATH/benchmark/TPCDSonSparkSQL.py update_and_run $BEAVER_PATH/repo/TPCDS_99/
    pat_run $engine $query

}
run_LLAP() {
    sh /opt/Beaver/llap_run.sh
    wait
    sleep 1m
    #$BEAVER_PATH/benchmark/TPCDSonSparkSQL.py update_and_run $BEAVER_PATH/repo/TPCDS_99/
    pat_run $engine $query
    cp /opt/Beaver/llap_run.sh $backupdir/logs/$engine/${date}_tun_q${query}/
    cp $BEAVER_PATH/repo/TPCDS_99/99queries/testbench.settings  $backupdir/logs/$engine/${date}_tun_q${query}/

}
# engines=$(echo $1|sed 's/,/ /g')
query=$2
echo $query
if [ $1 ];then
    engines=$(echo $1|sed 's/,/ /g')
    for engine in $engines; do
        check_run
        #stop_server
        run_$engine $query
        wait
    done
else
    echo '$0 [Impala,Presto,Sparksql,LLAP] [22,33]'
fi

