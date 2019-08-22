package com.xwc

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Administrator
 */
object Relation {
  case class Mac(macId: String, stime: String, jingdu: String, weidu: String)
  case class Imsi(imsId: String, stime: String, jingdu: String, weidu: String)
  def computeDistence(condition: Double, imsi: Imsi, mac: Mac) = {
    val distence = Support.getDistance(imsi.weidu.toDouble,imsi.jingdu.toDouble,mac.weidu.toDouble,mac.jingdu.toDouble)
    distence<condition
  }

  def drainTo(imsi:Imsi,mac:Mac): Unit = ???

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("sparkPhx").setMaster("local[2]")
    val sc = SparkContext.getOrCreate(conf)
    val sqlctx = new SQLContext(sc)

    val macRange = 150
    val imsiRange = 100
    val condition = (macRange + imsiRange).toDouble

    val macPath = Thread.currentThread().getContextClassLoader.getResource("mac.csv")
    val imsiPath = Thread.currentThread().getContextClassLoader.getResource("imsi.csv")
    val macdf = sqlctx.read.format("csv").option("header", "true").option("schema", "true").load("file://" + macPath.getPath)
    val imsidf = sqlctx.read.format("csv").option("header", "true").option("schema", "true").load("file://" + imsiPath.getPath)
    //    macdf.foreach(println)
    //    imsidf.foreach(println)

    var mrdd = macdf.map(r => Mac(r.getString(0), r.getString(5), r.getString(16), r.getString(17)))
    var irdd = imsidf.map(r => Imsi(r.getString(0), r.getString(1), r.getString(2), r.getString(3)))

    irdd.map(imsi => {
      mrdd.map(mac => {
        val value = computeDistence(condition, imsi, mac);if ( value)  {(mac.macId,imsi.imsId,1)}
      })
    }).reduce((x,y) =>{
      if (x._1==y._1){
        (x._1,x._3 + y._3)
      }
    })


  }

}
