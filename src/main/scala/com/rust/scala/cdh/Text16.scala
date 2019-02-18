package com.rust.scala.cdh
import org.apache.spark.sql.Column

/**
  * @author Rust
  */
object Text16 extends Init {
  /*
  1. It is possible that, same product can be supplied by multiple supplier. Now find each product, its
     price according to each supplier.

  2. Find all the supllier name, who are supplying 'Pencil 3B'
  3. Find all the products , which are supplied by ABC Traders.
   */

  def main(args: Array[String]): Unit = {

    val prd = spark.sqlContext.read.csv("/user/ubuntu/product2.csv")
    val prdSup = spark.sqlContext.read.csv("/user/ubuntu/products_suppliers.csv")
    val sup = spark.sqlContext.read.csv("/user/ubuntu/supplier.csv")

    prd.createOrReplaceTempView("prd")
    prdSup.createOrReplaceTempView("prdSup")
    sup.createOrReplaceTempView("sup")

    spark.sqlContext.sql("select * from prd join prdSup on prd.productID = prdSup.productID").show()


//    spark.sqlContext.sql("select prd.productID,prd.name,sup.name from prd join prdSup on prd.productID = prdSup.productID" +
//      "join sup on sup.supplierid = prdSup.supplierID").show()


  }


}
