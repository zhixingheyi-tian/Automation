#!/usr/bin/bash -x
cat tpch_datagen.scala | {%spark.home%}/bin/spark-shell --master yarn --deploy-mode client --jars {%sparksql.perf.home%}/target/scala-{%scala.version%}/spark-sql-perf_{%scala.version%}-0.5.1-SNAPSHOT.jar