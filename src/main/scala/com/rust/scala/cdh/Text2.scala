package com.rust.scala.cdh

import org.apache.spark.sql.{Encoders, SaveMode}

/**
  * 读取csv(不带header)转换成orc,parquet存入hdfs的hive houseware,然后再导入hive表
  * @author Rust
  */
object Text2 extends Init {
  /*
    You have been given MySQL DB with following details. You have been given
      following product.csv file product.csv productID,productCode,name,quantity,price
    1001,PEN,Pen Red,5000,1.23
    1002,PEN,Pen Blue,8000,1.25
    1003,PEN,Pen Black,2000,1.25
    1004,PEC,Pencil 2B,10000,0.48
    1005,PEC,Pencil 2H,8000,0.49
    1006,PEC,Pencil HB,0,9999.99
    Now accomplish following activities.
    1 . Create a Hive ORC table using SparkSql
    2 . Load this data in Hive table.
    3 . Create a Hive parquet table using SparkSQL and load data in it.
    */
  def main(args: Array[String]): Unit = {
    //  在HDFS上创建目录并且上传product.csv(不带header)

    //  使用spark-shell 读取csv
    val df = spark.read.option("header", "false").csv("/user/ubuntu/product.csv")

    // 看一眼数据
    df.first()

    // 看一眼schema
    df.printSchema()

    //  创建样例类
    case class Prd(productID: Integer, productCode: String, name: String, quantity: Integer, price: Float)

    //  创建样例类的RDD
    val prdRdd = df.rdd.map(r => Prd(Integer.valueOf(r.getString(0)), r.getString(1), r.getString(2), Integer.valueOf(r.getString(3)), r.getString(4).toFloat))

    //  看一眼
    prdRdd.first

    prdRdd.count


    //  注意这里我如果用val prdDf = spark.createDataFrame(prdRdd,classOf[Prd]),创建的dataFrame中并没有数据
    //  所以，创建DataSet
    //  导入隐式参数
//    import spark.implicits._
    implicit val rr = Encoders.kryo(classOf[Prd])
    val prdDs = spark.createDataset(prdRdd)

    //  用orc格式写入hive warehouse
    prdDs.write.mode(saveMode = SaveMode.Overwrite).format("orc").saveAsTable("product_orc_table")

    //  进入hive创建 orc 表
    //-------------------------
    //CREATE EXTERNAL TABLE products (productid int,code string,name string ,quantity int, price float}
    //STORED AS orc
    //LOCATION '/user/hive/warehouse/product_orc_table';


    //  用parquet格式写入hive warehouse
    prdDs.write.mode(SaveMode.Overwrite).format("parquet").saveAsTable("product_parquet_table")


    //  创建parquet hive 表

    //---------------------------
    //CREATE EXTERNAL TABLE products_parquet (productid int,code string,name string,quantity int, price float)
    //STORED AS parquet
    //LOCATION /user/hive/warehouse/product_parquet_table';


  }


}
