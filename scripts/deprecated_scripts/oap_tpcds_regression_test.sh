#!/bin/bash

source ~/.bashrc
BEAVER_HOME=$(cd $(dirname ${BASH_SOURCE[0]})/..;pwd)
OAP_SOURCE_DIR=$BEAVER_HOME/package/source_code/oap
OAP_JAR_DIR=$OAP_HOME/oap_jar
date=$(date +%Y_%m_%d_%H_%M_%S)


deploy_sparkSQL() {
    sh $BEAVER_HOME/scripts/spark.sh compile $1 oap
    sh $BEAVER_HOME/scripts/spark.sh deploy $1 oap
}


gen_data(){
    sh $BEAVER_HOME/scripts/tpc_ds.sh gen_data $1
}


run_query(){
    sh $BEAVER_HOME/scripts/tpc_ds.sh run $1 $2
}


update_oap_jar(){
    branch=$1
    if ! [ -d $OAP_SOURCE_DIR ]; then git clone https://github.com/Intel-bigdata/OAP.git $OAP_SOURCE_DIR; fi
    cd $OAP_SOURCE_DIR
    git checkout $branch; git pull
    mvn clean -DskipTests package;
    oap_jar_file=`basename $OAP_SOURCE_DIR/target/oap-*-with-spark-*.jar`
	rm -rf $OAP_JAR_DIR/${oap_jar_file}
	cp $OAP_SOURCE_DIR/target/${oap_jar_file} $OAP_JAR_DIR
    sed -i "s/oap-.*-with-spark-.*.jar/${oap_jar_file}/"  $SPARK_HOME/conf/spark-defaults.conf
}


analyze_result(){
    iteration_num=$1
    latest_commit=$(cd $OAP_SOURCE_DIR; git rev-parse HEAD)
    mkdir -p $BEAVER_HOME/result/oap_tpcds_regression_test/$latest_commit/$date
    cp -r /opt/Beaver/spark-sql-perf/tpcds_script/*/tpcds/logs $BEAVER_HOME/result/oap_tpcds_regression_test/$latest_commit/$date

    #calculate average time
    total_running_time=0
    for t in $(seq $iteration_num);do
        iteration_time=` cat $BEAVER_HOME/result/oap_tpcds_regression_test/$latest_commit/$date/logs/$t/result.csv | awk -F ',' '{sum +=$2;} END{print sum}'`
        total_running_time=`expr $total_running_time '+' $iteration_time`
    done
    running_time=`expr $total_running_time '/' $iteration_num`
    echo "The average time of this test is $running_time s"

    #calculate performance fluctuation
    if [ ! -f $BEAVER_HOME/result/oap_tpcds_regression_test/performance_compare_result.csv ]; then
        echo "No history result !"
        echo "$latest_commit,$running_time,0,$date" > $BEAVER_HOME/result/oap_tpcds_regression_test/performance_compare_result.csv
    else
        last_running_time=`cat $BEAVER_HOME/result/oap_tpcds_regression_test/performance_compare_result.csv | awk -F ',' '{sum=$2;} END{print sum}'`
        echo "The running time of last test is $last_running_time s"
        time_difference=`expr $running_time '-' $last_running_time`
        echo "the average time of this commit is $time_difference s longer than last test!"
        performance_fluctuation_percentage=`echo $time_difference  $last_running_time | awk '{printf("%0.3f\n",$1/$2)}'`
        echo "performance fluctuation is $performance_fluctuation_percentage"
        echo "$latest_commit,$running_time,$performance_fluctuation_percentage,$date" >>  $BEAVER_HOME/result/oap_tpcds_regression_test/performance_compare_result.csv
    fi
}



echo original parameters=[$@]
# use -c option to check if called by cron or bash
args=`getopt -a -o crb:gi:d: -l cron,rerun,branch:,gen,iteration:,dir: -- "$@"`
echo ARGS=[$args]
eval set -- "${args}"

echo formatted parameters=[$@]

runType=""
branch="master"
iteration=1
while true
do
    case "$1" in
    -c|--cron)
	runType="cron"
	;;
    -r|--rerun)
	runType="rerun"
	;;
    -b|--branch)
	branch="$2"
	shift
	;;
	-g|--gen)
	runType="gen"
	;;
    -i|--iteration)
        iteration=$2
        shift
        ;;
    -d|--dir)
        repo=$2
        shift
        ;;
    --)
	shift
	break
	;;
esac
shift
done


if [ ! -d "$repo" ]; then
  echo "Usage: $0 -r|--rerun|-g|--gen  -d|--dir conf_dir  -i|--iteration iteration_num  -b|--branch branch_name " >&2
  exit 1
fi


if [ "${runType}" = "rerun" ]; then
    echo "RUNING TPCDS"
    update_oap_jar $branch
    cd $BEAVER_HOME
    run_query  $repo $iteration
    if [ "$?" -ne 0 ]; then
        echo " ERROR RUNING!"
        exit 1
    else
        analyze_result $iteration
    fi
elif [ "${runType}" = "gen" ]; then
    deploy_sparkSQL $repo
    gen_data $repo
else
    echo "Usage: $0 -r|--rerun|-g|--gen  -d|--dir conf_dir  -i|--iteration iteration_num  -b|--branch branch_name " >&2
fi



 
