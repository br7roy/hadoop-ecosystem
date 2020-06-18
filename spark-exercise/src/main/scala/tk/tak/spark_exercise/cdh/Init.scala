package tk.tak.spark_exercise.cdh

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  * @author Tak
  */
trait Init {

  protected var spark: SparkSession = _
  protected var sc: SparkContext = _

  def init(): Unit = {
    val hc = SparkSession.builder().appName("spark shell training").enableHiveSupport().master("local[2]").getOrCreate()
    //    val conf = new SparkConf().setAppName("spark shell training").setMaster("local[2]")
    //    val sc = new SparkContext(conf)
    this.spark = hc
    this.sc = spark.sparkContext
  }

}
