package tk.tak.spark_exercise

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
 * @author Tak
 */
class Init {

  protected var spark: SparkSession = _
  protected var sc: SparkContext = _

  def this(name: String = "spark shell training") {
    this
    val hc = SparkSession.builder().appName(name).enableHiveSupport().master("local").getOrCreate()
    this.spark = hc
    this.sc = spark.sparkContext
  }

}
