package com.rust.scala.cdh

/**
  * 读取csv数据，添加字段，存入mysql
  *
  * @author Rust
  */
object Etl extends Init {


  def main(args: Array[String]): Unit = {
    val path = ""
    val rdd1 = spark.read.option("header", value = true).csv(path)


  }

}
