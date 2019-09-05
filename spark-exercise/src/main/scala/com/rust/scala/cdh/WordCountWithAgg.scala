package com.rust.scala.cdh

import java.util

import org.apache.spark.{Partitioner, RangePartitioner}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.OrderedRDDFunctions

import scala.collection.mutable

/**
  * 使用aggregate进行word count
  *
  * @author Rust
  */
object WordCountWithAgg extends Init with Logging {


  def seqFuc(agg: mutable.HashMap[String, Int], word: String): mutable.HashMap[String, Int] = {
    if (!agg.contains(word)) {
      agg.put(word, 1)
    } else {
      agg.put(word, agg(word) + 1)
    }
    agg
  }


  def seqFuc: (mutable.HashMap[String, Int], String) => mutable.HashMap[String, Int] = {
    (agg, word) => {
      if (!agg.contains(word)) {
        agg.put(word, 1)
      } else {
        agg.put(word, agg(word) + 1)
      }
      agg
    }
  }

  def combFuc: (mutable.HashMap[String, Int], mutable.HashMap[String, Int]) => mutable.HashMap[String, Int] = {
    (agg1, agg2) => {
      for ((word, count) <- agg1) {
        if (!agg2.contains(word)) {
          agg2.put(word, 1)
        } else {
          agg2.put(word, agg2(word) + count)
        }
      }
      agg2
    }



  }

  def main(args: Array[String]): Unit = {
    super.init()
    val rdd = sc.textFile("file:///D:\\project\\gitlab\\spark-exercise\\src\\test\\resources\\wc.txt")
    val res = rdd.flatMap(_.split("\n")).flatMap(_.split(" ")).aggregate(mutable.HashMap[String, Int]())(seqFuc, combFuc)
    res.foreach(println)
    logInfo(res.toString())
    val  rd = sc.makeRDD(Array(1,2,3),3)
    rd.mapPartitions( iter =>{
      var list = List[Int]()
      if (iter.hasNext) {
        list :+= iter.next()
      }
      list.iterator
    })

  }

}
