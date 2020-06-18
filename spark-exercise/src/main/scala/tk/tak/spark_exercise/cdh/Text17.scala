package tk.tak.spark_exercise.cdh

import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * @author Tak
  */
object Text17 extends Init {


  //----------------------------------------------------------------------------------------------------
  //  1. Select all the columns from product table with output header as below. productID AS ID code AS
  //     Code name AS Description price AS 'Unit Price'
  //  2. Select code and name both separated by ' -' and header name should be "Product Description.
  //  3. Select all distinct prices.
  //  4. Select distinct price and name combination.
  //  5. Select all price data sorted by both code and productID combination.
  //  6. count number of products.
  //  7. Count number of products for each code.
  //----------------------------------------------------------------------------------------------------


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


    prdDF.createOrReplaceTempView("p")
    prdSupDF.createOrReplaceTempView("ps")
    supDF.createOrReplaceTempView("s")

    //  1
    spark.sql("select p.productID as ID,p.productCode as Code,p.name as Description,p.price as UnitPrice from p")

    //  2
    spark.sql("select concat(p.productCode,'-',p.name) from p").toDF("ProductDescription").show

    //  3
    spark.sql("select distinct(p.price) from p ").show

    //  4
    spark.sql("select distinct price,name from p").show

    //  5
    spark.sql("select price,productID,productCode from p order by productID,productCode").show

    //  6
    prdDF.count()

    //  7
    spark.sql("select productCode,count(*) as count from p group by productCode order by count").show


  }

}
