package tk.tak.spark_exercise.cdh

import java.util

import scala.collection.mutable

/**
  * @author Tak
  */
object Text1 extends Init {


  /*
      You have been given below code snippet (do a sum of values by key}, with
      intermediate output.
      val keysWithValuesList = Array("foo=A", "foo=A", "foo=A", "foo=A", "foo=B", "bar=C",
      "bar=D", "bar=D")
      val data = sc.parallelize(keysWithValuesl_ist}
      //Create key value pairs
      val kv = data.map(_.split("=")).map(v => (v(0), v(l))).cache()
      val initialCount = 0;
      val countByKey = kv.aggregateByKey(initialCount)(addToCounts, sumPartitionCounts)
      Now define two functions (addToCounts, sumPartitionCounts) such, which will produce following
      results.
      Output 1
      countByKey.collect
      res3: Array[(String, Int)] = Array((foo,5), (bar,3))
      import scala.collection._
      val initialSet = scala.collection.mutable.HashSet.empty[String]
      val uniqueByKey = kv.aggregateByKey(initialSet)(addToSet, mergePartitionSets)
      Now define two functions (addToSet, mergePartitionSets) such, which will produce following results.
      Output 2:
      uniqueByKey.collect
      res4: Array[(String, scala.collection.mutable.HashSet[String])] = Array((foo,Set(B, A}},
      (bar,Set(C, D)}}
      */
  def main(args: Array[String]): Unit = {
    init()
    val keysWithValuesList = Array("foo=A", "foo=A", "foo=A", "foo=A", "foo=B", "bar=C",
      "bar=D", "bar=D")
    val data = sc.parallelize(keysWithValuesList)

    val kv = data.map(_.split("=")).map(v => (v(0), v(1))).cache()
    val initialCount = 0


    /*    def addToCounts: (Int, String) => Int = {
          (src, res) => {
            src + 1
          }

        }*/


    def addToCounts(cnt: Int, word: String) = cnt + 1


    def sumPartitionCounts(i1: Int, i2: Int) = i1 + i2


    //  1
    val countByKey = kv.aggregateByKey(initialCount)(addToCounts, sumPartitionCounts)
    println(countByKey.collect.mkString(","))



    //  2
    import scala.collection._
    import scala.collection.mutable.HashSet
    val initialSet = mutable.HashSet.empty[String]
    val uniqueByKey = kv.aggregateByKey(initialSet)(addToSet, mergePartitionSets)


    println(uniqueByKey.collect.mkString(","))

  }

  def addToSet(set: mutable.HashSet[String], word: String): mutable.HashSet[String] = {
    set += word
  }

  def mergePartitionSets(s1: mutable.HashSet[String], s2: mutable.HashSet[String]): mutable.HashSet[String] = {
/*    for (elem <- s1) s2.add(elem)
    s2*/
    s1 ++= s2
  }
}
