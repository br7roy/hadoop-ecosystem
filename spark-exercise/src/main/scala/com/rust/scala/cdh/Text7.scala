package com.rust.scala.cdh

import org.apache.spark.sql.{Encoder, Encoders}

/**
  * @author Rust
  */
object Text7 extends Init {
  def main(args: Array[String]): Unit = {
    //    /user/ubuntu/EmployeeName.csv
    //You have been given a file named spark7/EmployeeName.csv
    //(id,name).
    //EmployeeName.csv
    //E01,Lokesh
    //E02,Bhupesh
    //E03,Amit
    //E04,Ratan
    //E05,Dinesh
    //E06,Pavan
    //E07,Tejas
    //E08,Sheela
    //E09,Kumar
    //E10,Venkat
    //1. Load this file from hdfs and sort it by name and save it back as (id,name) in results directory.
    //However, make sure while saving it should be able to write In a single file.
    val df = spark.read.option("header", "true").option("inferSchema", "true").csv("file://D:\\project\\gitlab\\spark-exercise\\src\\main\\resources\\EmployeeName.csv")
    case class Emp(id: String, name: String)
    val sp = spark
    import sp.implicits._
    implicit val ee: Encoder[Emp] = Encoders.kryo(classOf[Emp])
    val ds = df.as[Emp]
    //    create view
    ds.createOrReplaceTempView("emp")
    //    peek result
    spark.sql("select * from emp order by name").show(false)

    //    save to hdfs
    spark.sql("select * from emp order by name").write.option("header", "true").csv("/user/ubuntu/cdhtest/text7")


  }

}
