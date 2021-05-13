val root_dir="hdfs://{%hostname%}:9000/tpcds_{%data.format%}/{%scale%}"
val log_location="{%tpcds.script.home%}/tpcds"
val queries="{%queries%}"
val queries_list=queries.split(" ").toList

val call_center = spark.read.parquet(s"$root_dir/call_center")
val catalog_page = spark.read.parquet(s"$root_dir/catalog_page")
val catalog_returns = spark.read.parquet(s"$root_dir/catalog_returns")
val catalog_sales = spark.read.parquet(s"$root_dir/catalog_sales")
val customer = spark.read.parquet(s"$root_dir/customer")
val customer_address = spark.read.parquet(s"$root_dir/customer_address")
val customer_demographics = spark.read.parquet(s"$root_dir/customer_demographics")
val date_dim = spark.read.parquet(s"$root_dir/date_dim")
val household_demographics = spark.read.parquet(s"$root_dir/household_demographics")
val income_band = spark.read.parquet(s"$root_dir/income_band")
val inventory = spark.read.parquet(s"$root_dir/inventory")
val item = spark.read.parquet(s"$root_dir/item")
val promotion = spark.read.parquet(s"$root_dir/promotion")
val reason = spark.read.parquet(s"$root_dir/reason")
val ship_mode = spark.read.parquet(s"$root_dir/ship_mode")
val store = spark.read.parquet(s"$root_dir/store")
val store_returns = spark.read.parquet(s"$root_dir/store_returns")
val store_sales = spark.read.parquet(s"$root_dir/store_sales")
val time_dime = spark.read.parquet(s"$root_dir/time_dim")
val warehouse = spark.read.parquet(s"$root_dir/warehouse")
val web_page = spark.read.parquet(s"$root_dir/web_page")
val web_returns = spark.read.parquet(s"$root_dir/web_returns")
val web_sales = spark.read.parquet(s"$root_dir/web_sales")
val web_site = spark.read.parquet(s"$root_dir/web_site")

call_center.createOrReplaceTempView("call_center")
catalog_page.createOrReplaceTempView("catalog_page")
catalog_returns.createOrReplaceTempView("catalog_returns")
catalog_sales.createOrReplaceTempView("catalog_sales")
customer.createOrReplaceTempView("customer")
customer_address.createOrReplaceTempView("customer_address")
customer_demographics.createOrReplaceTempView("customer_demographics")
date_dim.createOrReplaceTempView("date_dim")
household_demographics.createOrReplaceTempView("household_demographics")
income_band.createOrReplaceTempView("income_band")
inventory.createOrReplaceTempView("inventory")
item.createOrReplaceTempView("item")
promotion.createOrReplaceTempView("promotion")
reason.createOrReplaceTempView("reason")
ship_mode.createOrReplaceTempView("ship_mode")
store.createOrReplaceTempView("store")
store_returns.createOrReplaceTempView("store_returns")
store_sales.createOrReplaceTempView("store_sales")
time_dime.createOrReplaceTempView("time_dime")
warehouse.createOrReplaceTempView("warehouse")
web_page.createOrReplaceTempView("web_page")
web_returns.createOrReplaceTempView("web_returns")
web_sales.createOrReplaceTempView("web_sales")
web_site.createOrReplaceTempView("web_site")

import scala.io.Source
val queries = queries_list.map { q =>
   val queryContent: String = Source.fromFile(s"${log_location}/tpcds-queries/q${q}.sql").mkString
   println(queryContent)
   val df = spark.sql(s"$queryContent")
   df.write.format("parquet").option("header", "true").mode("overwrite").save(s"${root_dir}/result/q${q}_result")
}

