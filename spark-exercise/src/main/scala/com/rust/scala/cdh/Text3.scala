package com.rust.scala.cdh

import org.apache.spark.sql.Encoders

/**
  * @author Rust
  */
object Text3 extends Init {

  // 接text2

  def main(args: Array[String]): Unit = {

    val df = spark.read.option("header", "false").csv("/user/ubuntu/product.csv")

    case class Prd(productID: Integer, productCode: String, name: String, quantity: Integer, price: Float)

    val prdRdd = df.rdd.map(r => Prd(Integer.valueOf(r.getString(0)), r.getString(1), r.getString(2), Integer.valueOf(r.getString(3)), r.getString(4).toFloat))

//    import spark.implicits._
    implicit val rr = Encoders.kryo(classOf[Prd])
    val prdDs = spark.createDataset(prdRdd)

    prdDs.createOrReplaceTempView("prd")

    //  隐式传参
    import org.apache.spark.sql.functions._

    //  用DataSet 和spark-sql 分别实现
    //1. Select all the products which has product code as null
    spark.sql("select * from prd where productCode is null").show
    prdDs.filter(_.productCode.isEmpty).show

    //2. Select all the products, whose name starts with Pen and results should be order by Price
    //descending order.
    spark.sql("select * from prd where name like 'Pen %' order by price desc").show
    prdDs.filter(_.name.startsWith("Pen ")).orderBy(col("price").desc).show

    //3. Select all the products, whose name starts with Pen and results should be order by
    //Price descending order and quantity ascending order.
    spark.sql("select * from prd where name like 'Pen %' order by price desc ,quantity asc ").show
    prdDs.filter(r => r.name.startsWith("Pen ")).orderBy(-col("price"), col("quantity")).show

    //4. Select top 2 products by price
    spark.sql("select * from prd order by price desc limit 2").show
    prdDs.orderBy(col("price").desc).show(2)
  }

}
