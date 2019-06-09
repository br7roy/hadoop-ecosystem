package com.rust.scala.cdh

import org.apache.spark.sql.SaveMode

/**
  * 读取json文件解析
  * @author Rust
  */
object Text13 extends Init with App {

  val df = spark.sqlContext.read.json("/user/ubuntu/text13")

  case class Man(fName: String, lName: String)

  df.createOrReplaceTempView("man")

  val ddf = df.sqlContext.sql("from man select *")

  ddf.show()

  ddf.toJSON.write.mode(SaveMode.Overwrite).json("/user/ubuntu/cdhtest/text13")


}
