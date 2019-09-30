package com.rust

import org.apache.phoenix.spark.{SparkContextFunctions, SparkSqlContextFunctions}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Administrator
 */
object SparkPhx {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("sparkPhx").setMaster("local[2]")
    val sc = SparkContext.getOrCreate(conf)
    val rddfunc = new SparkContextFunctions(sc)
    val dffunc = new SparkSqlContextFunctions(new SQLContext(sc))

    //    val rdd = func.phoenixTableAsRDD("\"mac_num\"", Seq("ROW"), Option("\"macnum\".\"20180101\" = '10005926748065,201801010000,1-36892408777407,201801011330,1-23333853679906,201801012200,1-37469564100091,201801010430,1-8943761922609,201801012000,1-59060184352100,201801011300,1-42714469031008,201801010030,1-32526824011131,201801012230,1-4771806710323,201801011700,1-23289551185232,201801011400,1-3458469473373,201801011230,1-107827046652,201801010730,1-43157332868766,201801010730,1-62129912120525,201801011230,1-98406664812895,201801010230,1-90662361952876,201801011730,1-29895772972806,201801012130,1-34841658922833,201801010000,1-97294853281110,201801011900,1-51623141576780,201801010530,1-17559193596196,201801011930,1'"), Option("probd01:2181"))

    //    val rdd = rddfunc.phoenixTableAsRDD("\"mac_num\"",Seq(),predicate=Option("limit = 5"),zkUrl = Option("probd01:2181"))
    //    rdd.foreach(println)


    val df = dffunc.phoenixTableAsDataFrame("\"mac_num\"", Seq(), zkUrl = Option("probd01:2181"))
    val rdd = sc.makeRDD(df.take(5))
    case class MacNum(row:String,num:String,_time:String)
    val mrd = rdd.map( r => MacNum(r.getString(0),r.getString(1), r.getString(2)))
//    mrd.groupBy( r => r._time).reduceByKey((x,y)=>x.row)




    df.groupBy("ROW").mean().show()
//    df.take(5).foreach(print)
  }

}
