val root_dir="hdfs://{%hostname%}:9000/tpch_{%data.format%}/{%scale%}"
val log_location="{%sparksql.perf.home%}/tpch_script/{%spark.version%}/tpch"

val lineitem = spark.read.parquet(s"$root_dir/lineitem")
val orders = spark.read.parquet(s"$root_dir/orders")
val customer = spark.read.parquet(s"$root_dir/customer")
val part = spark.read.parquet(s"$root_dir/part")
val supplier = spark.read.parquet(s"$root_dir/supplier")
val partsupp = spark.read.parquet(s"$root_dir/partsupp")
val nation = spark.read.parquet(s"$root_dir/nation")
val region = spark.read.parquet(s"$root_dir/region")


lineitem.createOrReplaceTempView("lineitem")
orders.createOrReplaceTempView("orders")
customer.createOrReplaceTempView("customer")
part.createOrReplaceTempView("part")
supplier.createOrReplaceTempView("supplier")
partsupp.createOrReplaceTempView("partsupp")
nation.createOrReplaceTempView("nation")
region.createOrReplaceTempView("region")

import scala.io.Source
val queries = (1 to 22).map { q =>
   val queryContent: String = Source.fromFile(s"${log_location}/tpch-queries/$q.sql").mkString
   println(queryContent)
   val df = spark.sql(s"$queryContent")
   df.write.format("parquet").option("header", "true").mode("overwrite").save(s"${root_dir}/result/q${q}_result")
}

