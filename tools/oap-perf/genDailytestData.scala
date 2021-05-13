import com.databricks.spark.sql.perf.tpcds.TPCDSTables
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.util.Utils
import org.apache.spark.sql.{SaveMode}
import org.apache.spark._

val scale = "{%scale%}"
val p = scale.toInt / 2048.0
val store_sales_p = (3164 * p * 0.5 * 0.5 + 1).toInt
val fileformats = "{%data.format%}".split(",", 0)
val codec = "SNAPPY"
val dataLocationRoot = "hdfs://{%hostname%}:9000/{%oap.benchmark.hdfs.file.root.dir%}"
val tpcdskitsDir = "/opt/Beaver/tpcds-kit/tools"

for ( fileformat <- fileformats ){
    val basename = s"$fileformat$scale"
    val dataLocation = s"$dataLocationRoot/$fileformat$scale/"
    import scala.collection.mutable
    class logoap extends Logging{
      override def logWarning(msg: => String) {
        if (log.isWarnEnabled) log.warn(msg)
      }
    }
    def time[T](code: => T, action: String): Unit = {
      val t0 = System.nanoTime
      code
      val t1 = System.nanoTime
      println(action + ((t1 - t0) / 1000000) + "ms")
    }
    val Logoap = new logoap
    spark.sqlContext.setConf(s"spark.sql.$fileformat.compression.codec", codec)
    val tables = new TPCDSTables(spark, spark.sqlContext, tpcdskitsDir, scale)
    tables.genData(dataLocation, fileformat, true, false, false, false, "store_sales", store_sales_p)
    spark.sql(s"drop database if exists $basename CASCADE")
    spark.sql(s"create database $basename").show()
    spark.sql(s"use $basename").show()
    spark.sql("drop table if exists store_sales")
    spark.sql("drop table if exists store_sales_dup")
    val df = spark.read.format(fileformat).load(dataLocation + "store_sales")
    val divRatio = df.select("ss_item_sk").orderBy(desc("ss_item_sk")).limit(1).
      collect()(0)(0).asInstanceOf[Int] / 1000
    val divideUdf = udf((s: Int) => s / divRatio)
    df.withColumn("ss_item_sk1", divideUdf(col("ss_item_sk"))).write.format(fileformat).mode(SaveMode.Overwrite).save(dataLocation + "store_sales1")
    val conf = new Configuration()
    val hadoopFs = FileSystem.get(conf)
    hadoopFs.delete(new Path(dataLocation + "store_sales"), true)
    // Notice here delete source flag should firstly be set to false
    FileUtil.copy(hadoopFs, new Path(dataLocation + "store_sales1"),
      hadoopFs, new Path(dataLocation + "store_sales"), false, conf)
    FileUtil.copy(hadoopFs, new Path(dataLocation + "store_sales1"),
      hadoopFs, new Path(dataLocation + "store_sales_dup"), true, conf)
    spark.catalog.createExternalTable("store_sales", dataLocation + "store_sales", fileformat)
    spark.catalog.createExternalTable("store_sales_dup", dataLocation + "store_sales_dup", fileformat)
    Logoap.logWarning(s"File size of original table store_sales in $fileformat format: " )
    Logoap.logWarning("Records of table store_sales: " + spark.read.format(fileformat).load(dataLocation + "store_sales").count())
    def buildAllIndex() {
      def buildBtreeIndex(tablePath: String, table: String, attr: String): Unit = {
        try {
          spark.sql(s"DROP OINDEX ${table}_${attr}_index ON $table")
        } catch {
          case _: Throwable => Logoap.logWarning("Index doesn't exist, so don't need to drop here!")
        } finally {
          time(
            spark.sql(
              s"CREATE OINDEX IF NOT EXISTS ${table}_${attr}_index ON $table ($attr) USING BTREE"
            ),
            s"Create B-Tree index on ${table}(${attr}) cost "
          )
          Logoap.logWarning(s"The size of B-Tree index on ${table}(${attr}) cost:")
        }
      }
      def buildBitmapIndex(tablePath: String, table: String, attr: String): Unit = {
        try {
          spark.sql(s"DROP OINDEX ${table}_${attr}_index ON $table")
        } catch {
          case _: Throwable => Logoap.logWarning("Index doesn't exist, so don't need to drop here!")
        } finally {
          time(
            spark.sql(
              s"CREATE OINDEX IF NOT EXISTS ${table}_${attr}_index ON $table ($attr) USING BITMAP"
            ),
            s"Create Bitmap index on ${table}(${attr}) cost"
          )
          Logoap.logWarning(s"The size of Bitmap index on ${table}(${attr}) cost:")
        }
      }
      spark.sql(s"use $basename")
      buildBtreeIndex(dataLocation, "store_sales", "ss_customer_sk")
      buildBitmapIndex(dataLocation, "store_sales", "ss_item_sk1")
     }
    buildAllIndex()
}
