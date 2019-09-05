package com.rust.scala.exercise

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 分组求平均值
 * @author Rust
 */
object AvgWithGroupCase {
  def main(args:Array[String] ): Unit ={
    val conf = new SparkConf().setAppName("test").setMaster("local")
    val sc = new SparkContext(conf)
    val iScores = Array(("Fred", 88.0), ("Fred", 95.0), ("Fred", 91.0), ("Wilma", 93.0), ("Wilma", 95.0), ("Wilma", 98.0))
    val rdd = sc.makeRDD(iScores)
    rdd.groupByKey().map(f=>{
      var num =0
      var sum = 0.0

      for (elem <- f._2) {
        sum=elem+sum
        num+=1
      }
      val avg = sum/num
      (f._1,f"$avg%1.2f")
    }).collect().foreach(println)

  }

}
