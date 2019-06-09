package com.rust.scala.cdh

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}

import scala.collection.mutable.ListBuffer
import scala.util.parsing.json.JSON

/**
  * @author Rust
  */
object Text20 {


  def main(args: Array[String]): Unit = {
    /*    val people = new StringBuilder
        people.append("{\"name\" :\"John\", \"age\":28,\"gender\":\"M\"}")
        people.append("{\"name\" :\"Lolita\", \"age\":33,\"gender\":\"F\"}")
        people.append("{\"name\" :\"Dont Know\", \"age\":18,\"gender\":\"T\"}")
        val peopleRDD = sc.textFile("people.json")*/

    /*    val buf = ListBuffer
        val people = buf.newBuilder
        people.+("{\"name\" :\"John\", \"age\":28,\"gender\":\"M\"}")
        people + "{\"name\" :\"Lolita\", \"age\":33,\"gender\":\"F\"}"
        people.+("{\"name\" :\"Dont Know\", \"age\":18,\"gender\":\"T\"}")
        people.+("{\"name\":\"Amit\", \"age\":45,\"gender\":\"M\"}")
        people.+("{\"name\":\"Ganga\",\"age\":43,\"gender\":\"F\"}")
        val peopleRDD = sc.parallelize(people.result())
        val st = (0, 0)
        peopleRDD.aggregate(st)(seqFunc, combFunc)*/


    case class People(name: String, age: BigInt, gender: String)


    def seqFunc: (Array[BigInt], People) => Array[BigInt] = {
      (array, people) => Array(array(0) + people.age, array(1) + 1)
    }

    def combFunc: (Array[BigInt], Array[BigInt]) => Array[BigInt] = {
      (array1, array2) => Array(array1(0) + array2(0), array1(1) + array2(1))
    }

    implicit val impParam: Encoder[People] = org.apache.spark.sql.Encoders.kryo[People]
    val spark = SparkSession.builder().appName("spark shell training").enableHiveSupport().master("local[2]").getOrCreate()
    val df = spark.read.json("file:///D:\\project\\gitlab\\spark-exercise\\src\\main\\resources\\people.json").as[People]
    val rdd = df.rdd
    rdd.aggregate(Array(BigInt(0), BigInt(0)))(seqFunc, combFunc)


  }

}
