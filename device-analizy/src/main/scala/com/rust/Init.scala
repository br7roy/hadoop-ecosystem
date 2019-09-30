package com.rust

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Rust
  */
trait Init {

    val sparkConf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = SparkContext.getOrCreate(sparkConf)
    val hiveContext = new HiveContext(sc)
}
