val root_dir="hdfs://{%hostname%}:9000/tpch_{%data.format%}/{%scale%}"
val log_location="{%sparksql.perf.home%}/tpch_script/{%spark.version%}/tpch"

val lineitem = spark.read.format("arrow").load(s"$root_dir/lineitem")
val orders = spark.read.format("arrow").load(s"$root_dir/orders")
val customer = spark.read.format("arrow").load(s"$root_dir/customer")
val part = spark.read.format("arrow").load(s"$root_dir/part")
val supplier = spark.read.format("arrow").load(s"$root_dir/supplier")
val partsupp = spark.read.format("arrow").load(s"$root_dir/partsupp")
val nation = spark.read.format("arrow").load(s"$root_dir/nation")
val region = spark.read.format("arrow").load(s"$root_dir/region")

val result_1 = spark.read.format("arrow").load(s"$root_dir/result/q1_result")
val result_2 = spark.read.format("arrow").load(s"$root_dir/result/q2_result")
val result_3 = spark.read.format("arrow").load(s"$root_dir/result/q3_result")
val result_4 = spark.read.format("arrow").load(s"$root_dir/result/q4_result")
val result_5 = spark.read.format("arrow").load(s"$root_dir/result/q5_result")
val result_6 = spark.read.format("arrow").load(s"$root_dir/result/q6_result")
val result_7 = spark.read.format("arrow").load(s"$root_dir/result/q7_result")
val result_8 = spark.read.format("arrow").load(s"$root_dir/result/q8_result")
val result_9 = spark.read.format("arrow").load(s"$root_dir/result/q9_result")
val result_10 = spark.read.format("arrow").load(s"$root_dir/result/q10_result")
val result_11 = spark.read.format("arrow").load(s"$root_dir/result/q11_result")
val result_12 = spark.read.format("arrow").load(s"$root_dir/result/q12_result")
val result_13 = spark.read.format("arrow").load(s"$root_dir/result/q13_result")
val result_14 = spark.read.format("arrow").load(s"$root_dir/result/q14_result")
val result_15 = spark.read.format("arrow").load(s"$root_dir/result/q15_result")
val result_16 = spark.read.format("arrow").load(s"$root_dir/result/q16_result")
val result_17 = spark.read.format("arrow").load(s"$root_dir/result/q17_result")
val result_18 = spark.read.format("arrow").load(s"$root_dir/result/q18_result")
val result_19 = spark.read.format("arrow").load(s"$root_dir/result/q19_result")
val result_20 = spark.read.format("arrow").load(s"$root_dir/result/q20_result")
val result_21 = spark.read.format("arrow").load(s"$root_dir/result/q21_result")
val result_22 = spark.read.format("arrow").load(s"$root_dir/result/q22_result")

lineitem.createOrReplaceTempView("lineitem")
orders.createOrReplaceTempView("orders")
customer.createOrReplaceTempView("customer")
part.createOrReplaceTempView("part")
supplier.createOrReplaceTempView("supplier")
partsupp.createOrReplaceTempView("partsupp")
nation.createOrReplaceTempView("nation")
region.createOrReplaceTempView("region")
result_1.createOrReplaceTempView("result_1")
result_2.createOrReplaceTempView("result_2")
result_3.createOrReplaceTempView("result_3")
result_4.createOrReplaceTempView("result_4")
result_5.createOrReplaceTempView("result_5")
result_6.createOrReplaceTempView("result_6")
result_7.createOrReplaceTempView("result_7")
result_8.createOrReplaceTempView("result_8")
result_9.createOrReplaceTempView("result_9")
result_10.createOrReplaceTempView("result_10")
result_11.createOrReplaceTempView("result_11")
result_12.createOrReplaceTempView("result_12")
result_13.createOrReplaceTempView("result_13")
result_14.createOrReplaceTempView("result_14")
result_15.createOrReplaceTempView("result_15")
result_16.createOrReplaceTempView("result_16")
result_17.createOrReplaceTempView("result_17")
result_18.createOrReplaceTempView("result_18")
result_19.createOrReplaceTempView("result_19")
result_20.createOrReplaceTempView("result_20")
result_21.createOrReplaceTempView("result_21")
result_22.createOrReplaceTempView("result_22")

import scala.io.Source
import java.io.PrintWriter
val out = new PrintWriter(s"$log_location/logs/validation.log")

val queries = Range(1,2).++(Range(3,16)).++(Range(17,23)).map { q =>
   val queryContent: String = Source.fromFile(s"${log_location}/tpch-queries/${q}.sql").mkString
   println(queryContent)
   val df = spark.sql(s"$queryContent")
   df.createOrReplaceTempView(s"df_${q}")
   val diff = spark.sql(s"select * from df_${q} EXCEPT select * from result_${q}")
   if(diff.head(1).isEmpty) {
        out.println(s"Q${q} passes.")
    } else {
	    out.println(s"Q${q} fails.")
	    out.println("native-sql-engine result:")
	    val df_row = df.collect
	    for (i <- 0 to (df_row.length - 1)){
            out.println(df_row(i).mkString(",")) //get each column data and save into file
        }
        out.println("vanilla spark result:")
        val vanilla_df = spark.sql(s"select * from result_${q}").collect
	    for (i <- 0 to (vanilla_df.length - 1)){
            out.println(vanilla_df(i).mkString(",")) //get each column data and save into file
        }
    }
}

out.close

