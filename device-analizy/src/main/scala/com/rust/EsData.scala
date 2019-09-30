package com.rust

import org.apache.spark.{SparkConf, SparkContext}
import com.databricks.spark.csv
import org.apache.spark.sql.SQLContext
/**
 * @author fth
 */
object EsData {
  case class MacTrance(mac: String, servicecode: String, stime: String, etime: String)
  def main(args: Array[String]): Unit = {
    val conf= new SparkConf().setAppName("es").setMaster("local[*]").set("spark.hadoop.mapred.output.compress","false")
    val sc = SparkContext.getOrCreate(conf)
    val sqlContext = new SQLContext(sc)
    import org.elasticsearch.spark.sparkContextFunctions
    import org.apache.spark.sql.{Encoder, Encoders}

    implicit val ee: Encoder[MacTrance] = Encoders.kryo(classOf[MacTrance])
    import sqlContext.implicits._

    val tmp = sc.esRDD("mactrace201902/mactrace").values.map(x => (x("mac").toString,x("servicecode").toString,x("stime").toString,x("etime").toString)).toDF

    if (tmp.count()>100000000){
      sc.stop()
      println("bad! count more than hundred Billion,stop")
    }
    println("ok ! count less than hundred Billion,continue")

    tmp.repartition(50).write.mode("overwrite").option("header","true").format("com.databricks.spark.csv").save("file:///root/mactranceFull")
  }

}
