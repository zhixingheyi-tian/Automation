#!/bin/bash -x

INCLUDED_LIST=(19 42 43 52 55 63 68 73 98)
#INCLUDED_LIST=(19 42)

SET_REDUCE_NUM=()
SET_REDUCE_NUM[19]=200
SET_REDUCE_NUM[42]=200
SET_REDUCE_NUM[43]=200
SET_REDUCE_NUM[52]=200
SET_REDUCE_NUM[55]=200
SET_REDUCE_NUM[63]=200
SET_REDUCE_NUM[68]=200
SET_REDUCE_NUM[73]=200
SET_REDUCE_NUM[98]=200

SPARK_SQL_CMD="{%spark.home%}/bin/spark-sql"
SPARK_SQL_GLOBAL_OPTS="--master yarn --deploy-mode client "
DATABASE_NAME="tpcds_{%data.format%}_2_{%spark.mid.version%}_db"
#DATABASE_NAME="tpcds_10_{%data.format%}_uncompress"
QUERY_BEGIN_NUM=19
QUERY_END_NUM=99
#TUNNING_NAME=256G_mapjoin_work1_cores36_exemem230_tpcds_10t_filesize
TUNNING_NAME=drivermem100g_execores6_exemem80_yarnmemoverhead30g_{%data.format%}

function getExecTime() {
    start=$1
    end=$2
    qname=$3
    start_s=$(echo $start | cut -d '.' -f 1)
    start_ns=$(echo $start | cut -d '.' -f 2)
    end_s=$(echo $end | cut -d '.' -f 1)
    end_ns=$(echo $end | cut -d '.' -f 2)
    delta_ms=$(( ( 10#$end_s - 10#$start_s ) * 1000 + ( 10#$end_ns / 1000000 - 10#$start_ns / 1000000 ) ))
    show_s=$(( $delta_ms / 1000 ))
    show_ms=$(( $delta_ms % 1000 ))
    echo "++ Duration: ${show_s}s ${show_ms}ms for ${qname} ++"
}


len=${#INCLUDED_LIST[@]}
for (( i=${QUERY_BEGIN_NUM}; i<${QUERY_END_NUM}; i++)); do
    j=0
    found=false
    while [ $j -lt $len ]
    do
        if [ "${INCLUDED_LIST[$j]}" == "${i}" ]; then
            found=true
            break
        fi
        let j++
    done
    if [ "${found}" == "false" ]; then
        continue
    fi

    export QUERY_NUMBER=${i}
    export QUERY_NAME=q${QUERY_NUMBER}
    export QUERY_FILE_NAME="{%tpcds.script.home%}/tpcds/tpcds-queries/${QUERY_NAME}.sql"
    export QUERY_FILE_2_NAME="{%tpcds.script.home%}/tpcds/tpcds-queries/${QUERY_NAME}_r.sql"

    echo "create database if not exists ${DATABASE_NAME};" > ${QUERY_FILE_2_NAME}
    echo "use ${DATABASE_NAME};" >> ${QUERY_FILE_2_NAME}
    cat ${QUERY_FILE_NAME} >> ${QUERY_FILE_2_NAME}
    echo ";" >> ${QUERY_FILE_2_NAME}

    mkdir -p "{%tpcds.script.home%}/tpcds/logs/${DATABASE_NAME}_debug_noindex_2.{%spark.mid.version%}"
    export LOG_FILE_NAME="{%tpcds.script.home%}/tpcds/logs/${DATABASE_NAME}_debug_noindex_2.{%spark.mid.version%}/$DATABASE_NAME_${QUERY_NAME}.log"
    
    export REDUCE_NUM=${SET_REDUCE_NUM[${QUERY_NUMBER}]}
    export SPARK_SQL_LOCAL_OPTS="--conf spark.sql.shuffle.partitions=${REDUCE_NUM}"

    start=$(date +%s.%N)
    echo "${SPARK_SQL_CMD} ${SPARK_SQL_GLOBAL_OPTS} ${SPARK_SQL_LOCAL_OPTS} --database ${DATABASE_NAME} -f ${QUERY_FILE_2_NAME} 2>&1 | tee ${LOG_FILE_NAME}"
    echo "${SPARK_SQL_CMD} ${SPARK_SQL_GLOBAL_OPTS} ${SPARK_SQL_LOCAL_OPTS} --database ${DATABASE_NAME} -f ${QUERY_FILE_2_NAME} 2>&1 | tee ${LOG_FILE_NAME}" > {%pat.home%}/PAT-collecting-data/run.sh
    chmod +x {%pat.home%}/PAT-collecting-data/run.sh
    cd {%pat.home%}/PAT-collecting-data
    ./pat run ${QUERY_NAME}_$DATABASE_NAME_${TUNNING_NAME}_$REDUCE_NUM
    cd -
    end=$(date +%s.%N)
    getExecTime $start $end ${QUERY_NAME} >> ${LOG_FILE_NAME}

done
