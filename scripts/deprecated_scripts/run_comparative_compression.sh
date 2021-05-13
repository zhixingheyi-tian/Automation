#!/bin/bash
path=$(dirname $0)
path=${path/\./$(pwd)}
samba_dir="/opt/New_emerging_SQL_comparative_analysis"
backupdir="$samba_dir/`hostname`"
PATDIR="/opt/Beaver/pat"
BEAVER_PATH="${path}/Beaver"

export PYTHONPATH=$PYTHONPATH:$BEAVER_PATH

BB_HOME="/opt/Beaver/BB"

check_run(){
    # network_speed=$(pssh -i -h /opt/Beaver/hadoop/etc/hadoop/slaves "ip addr|grep 'state UP'|tail -n 1|awk -F[:] '{print\$2}'|xargs ethtool|grep 'Speed:*.10000Mb'"|grep 'Speed:*.10000Mb'|wc -l)
    # echo $network_speed
    # datanode_status=$(hdfs dfsadmin -report|grep 'Decommission Status'|wc -l)
    # echo $datanode_status
    # if [ X$network_speed != X$datanode_status ]; then
    #     echo 'some datanode Network Speed not 10000Mb! or datanode is down....'
    #     exit 0
    # fi
    mount |grep $samba_dir > /dev/null
    if [ $? -eq 1 ]; then
        echo "mount samba client data /opt/New_emerging_SQL_comparative_analysis" 1>&2
        mount -t cifs  //$PACKAGE_SERVER/New_emerging_SQL_comparative_analysis/ /opt/New_emerging_SQL_comparative_analysis/ -o username=smbuser,password=bdpe123
        mkdir -p $backupdir
    fi
    #ansible bdpe -m shell -a 'ntpdate 10.239.47.162'
    # ansible bdpe -m shell -a 'echo 3 > /proc/sys/vm/drop_caches && swapoff -a'
    # ansible bdpe -m shell -a "lsof |grep "PAT.*deleted"|awk '{print\$2}'|xargs -I{} kill -9 {}"
}


function pat_run() {
    echo $type,$2
    date=$(date +"%Y-%m-%d-%H-%M")
    cmd=$bbcmd
    #cmd="${BB_HOME}/bin/bigBench runBenchmark"
    backuplogdir="$backupdir/${type}"
    mkdir -p ${backuplogdir}
    #exit 0
    sed -i "/^CMD_PATH:/cCMD_PATH: $cmd" $PATDIR/PAT-collecting-data/config
    cd $PATDIR/PAT-collecting-data && ./pat run $type
    wait
    echo $L

    pdfdir="<source>$PATDIR/PAT-collecting-data/results/${type}/instruments/</source>"
    sed -i "/<source>/c$pdfdir" $PATDIR/PAT-post-processing/config.xml
    cd $PATDIR/PAT-post-processing && ./pat-post-process.py
    # wait
    cp $PATDIR/PAT-collecting-data/results/${type}/instruments/PAT-Result.pdf ${backuplogdir}/${type}_q${query}_${date}.pdf
}

bb_load(){
    sed -i '/^workload=/cworkload=LOAD_TEST' ${BB_HOME}/conf/bigBench.properties
    sed -i "/^set parquet.compression=/cset parquet.compression=${type};" ${BB_HOME}/engines/hive/conf/engineSettings.sql
    pat_run ${type} LOAD_TEST
    # hadoop jar /home/sundp/parquet-tools-2.9.6.jar meta  /user/hive/warehouse/bigbench_jk_3tb.db/customer/000005_0 > ${backupdir}/${type}/hadoop_info.txt
    # hadoop fs -du -h /user/hive/warehouse |grep bigbench_jk_3tb >> ${backupdir}/${type}/hadoop_info.txt
    # hadoop fs -du -h /user/hive/warehouse/bigbench_jk_3tb.db >> ${backupdir}/${type}/hadoop_info.txt
}

bb_power(){
    #SPARK_HOME="/home/sundp/spark-2.3.0-SNAPSHOT-bin-spark-2.7"
    sed -i '/^workload=/cworkload=BENCHMARK_START,POWER_TEST,BENCHMARK_STOP' ${BB_HOME}/conf/bigBench.properties
    #sed -i "/^spark.io.compression.codec/cspark.io.compression.codec  ${type}" $SPARK_HOME/conf/spark-defaults.conf
    pat_run ${type} POWER_TEST
    if [ ! $query ];then

        mv ${BB_HOME}/logs/times.csv ${backupdir}/${type}/
        # python $BEAVER_PATH/tools/make_xlsx.py $type
    fi
}
options="brotli GZIP UNCOMPRESSED SNAPPY"
#options="lz4 lzf snappy zstd brotli"
#options="brotli"

if [ $1 ];then
    query=$1
    bbcmd="${BB_HOME}/bin/bigBench runQuery -q $query -U"
else
    bbcmd="${BB_HOME}/bin/bigBench runBenchmark"
fi

for type in $options; do
    check_run
    bb_load $type
    unset SPARK_HOME
    bb_power $type
done
