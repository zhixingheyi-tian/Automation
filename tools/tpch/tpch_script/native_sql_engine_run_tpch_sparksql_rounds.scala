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


lineitem.createOrReplaceTempView("lineitem")
orders.createOrReplaceTempView("orders")
customer.createOrReplaceTempView("customer")
part.createOrReplaceTempView("part")
supplier.createOrReplaceTempView("supplier")
partsupp.createOrReplaceTempView("partsupp")
nation.createOrReplaceTempView("nation")
region.createOrReplaceTempView("region")

import scala.io.Source
import java.io.PrintWriter
import scala.reflect.io.Directory
import java.io.File
import java.util.Date

val iteration=6

for (round <- 1 to iteration){
    val directory = new Directory(new File(s"$log_location/logs/$round"))   //create log directory for each round
    directory.createDirectory(true, false)
    val out = new PrintWriter(s"$log_location/logs/$round/result.csv")      // each round result
    val queries = (1 to 22).map { q =>
        val start_time = new Date().getTime                                 // the starting time of query
        val tmp = new PrintWriter(s"$log_location/logs/$round/${q}.log") //query log
        val queryContent: String = Source.fromFile(s"${log_location}/tpch-queries/${q}.sql").mkString  //query string
        println(queryContent)
        val df = spark.sql(s"$queryContent")
        tmp.println(df.columns.mkString(","))   //get all columns name and save into file
        try {
            val df_row = df.collect                 //get all Row data
            val end_time = new Date().getTime       //the ending time of query
            for (i <- 0 to (df_row.length - 1)){
                tmp.println(df_row(i).mkString(",")) //get each column data and save into file
            }
            val elapse_time = (end_time - start_time) / 1000   //the elapsed time of query
            out.println(s"q${q},${elapse_time},Success")
        } catch {
            case ex: Exception => {
                tmp.println(ex.getStackTraceString)
                out.println(s"q${q},-1,Fail")
            }
        } finally {
            tmp.close
        }

    }
    out.close
}





