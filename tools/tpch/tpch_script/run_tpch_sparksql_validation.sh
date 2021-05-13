#!/usr/bin/bash -x

action=$1
if [ ! -n "$action" ]; then
    echo "Please input action baseline or validation"
    exit 1
fi

mkdir -p {%tpch.script.home%}/tpch/logs

if [ "${action}" = "baseline" ]; then
    cat native_sql_engine_run_tpch_sparksql_correct.scala | {%spark.home%}/bin/spark-shell --master yarn --deploy-mode client
elif [ "${action}" = "validation" ]; then
    cat native_sql_engine_run_tpch_sparksql.scala | {%spark.home%}/bin/spark-shell --master yarn --deploy-mode client
else
    echo "Please input action baseline or validation"
    exit 1
fi
