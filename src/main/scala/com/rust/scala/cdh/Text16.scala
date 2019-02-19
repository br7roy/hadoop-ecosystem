package com.rust.scala.cdh

import org.apache.spark.sql.types._

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

    val prd2 = spark.sqlContext.read.csv("/user/ubuntu/product2.csv")
    val prdSup2 = spark.sqlContext.read.csv("/user/ubuntu/products_suppliers.csv")
    val sup2 = spark.sqlContext.read.csv("/user/ubuntu/supplier.csv")

    val pheader = prd2.first()

    val prd = prd2.filter(r => r != pheader)
    val psheader = prdSup2.first()
    val prdSup = prdSup2.filter(r => r != psheader)
    val sheader = sup2.first()
    val sup = sup2.filter(r => r != sheader)

    val t = pheader.toSeq
    val pfields = for (a <- t) yield StructField(a.asInstanceOf[String], StringType, nullable = true)
    val pType = StructType(pfields)
    val prdDF = spark.createDataFrame(prd.rdd, pType)


    val e = psheader.toSeq
    val psfields = for (a <- e) yield StructField(a.asInstanceOf[String], StringType, nullable = true)
    val psType = StructType(psfields)
    val prdSupDF = spark.createDataFrame(prdSup.rdd, psType)


    val r = sheader.toSeq
    val shfields = for (a <- r) yield StructField(a.asInstanceOf[String], StringType, nullable = true)
    val shType = StructType(shfields)
    val supDF = spark.createDataFrame(sup.rdd, shType)


    prdDF.createOrReplaceTempView("prd")
    prdSupDF.createOrReplaceTempView("prdSup")
    supDF.createOrReplaceTempView("sup")

    spark.sql("select * from prd join prdSup on prd.productID = prdSup.productID").show()


    spark.sql("select prd.name AS Product_Name, price, sup.name AS Supplier_Name FROM prdSup JOIN prd ON prdSup.productID = prd.productID JOIN sup ON prdSup.supplierID = sup.supplierid").show

    spark.sql("select distinct(sup.name) from sup join prd on sup.supplierid = prd.supplierid join prdSup on prdSup.productID= prd.productID where prd.name='Pencil 3B'").show

    spark.sql("select prd.name ,sup.name from prdSup join prd on prd.productID = prdSup.productID join sup on sup.supplierid = prd.supplierid where sup.name='ABC Traders'").show(false)

    spark.sql("select p.name AS Product_Name, s.name AS Supplier_Name FROM prd AS p, prdSup AS ps, sup AS s WHERE p.productID = ps.productID AND ps.supplierID = s.supplierID AND s.name = 'ABC Traders'").show(false)


  }


}
