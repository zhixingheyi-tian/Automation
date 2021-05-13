#!/usr/bin/bash -x

action=$1
if [ ! -n "$action" ]; then
    echo "Please input action baseline or validation"
    exit 1
fi

mkdir -p {%tpcds.script.home%}/tpcds/logs

if [ "${action}" = "baseline" ]; then
    cat run_tpcds_sparksql_baseline.scala | {%spark.home%}/bin/spark-shell --master yarn --deploy-mode client
elif [ "${action}" = "validation" ]; then
    cat run_tpcds_sparksql_validation.scala | {%spark.home%}/bin/spark-shell --master yarn --deploy-mode client
else
    echo "Please input action baseline or validation"
    exit 1
fi
