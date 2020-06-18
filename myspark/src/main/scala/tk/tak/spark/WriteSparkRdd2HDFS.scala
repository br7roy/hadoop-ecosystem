package tk.tak.spark

import org.apache.spark.sql.SparkSession

/**
 * @author Tak
 */
object WriteSparkRdd2HDFS extends {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("")
      .master("local[*]")
      .config("spark.driver.port",2002)
      .config("spark.driver.host","localhost")
      .getOrCreate()
    val path: String = Thread.currentThread().getContextClassLoader.getResource("product.csv").getPath
    val df = spark.read.option("header", "false").csv(path)
    df.show





  }



}
