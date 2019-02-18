package com.rust.scala.cdh

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  * @author Rust
  */
trait Init {

  protected var spark: SparkSession = _
  protected var sc: SparkContext = _

  def init(): Unit = {
    val hc = SparkSession.builder().appName("spark shell training").enableHiveSupport().master("spark://s100:7077").getOrCreate()
    val conf = new SparkConf().setAppName("spark shell training").setMaster("spark://s100:7077")
    val sc = new SparkContext(conf)
    this.spark = hc
    this.sc = sc
  }

}
