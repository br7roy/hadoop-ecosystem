package com.rust.scala.cdh

/**
  * @author Rust
  */
object Text27 extends Init {

  //  Load these two tiles as Spark RDD and join them to produce the below results
  //  (l,((9,5),(g,h)))
  //  (2, ((7,4), (i,j))) (3, ((8,3), (k,l)))
  //  And write code snippet which will sum the second columns of above joined results (5+4+3)


  def main(args: Array[String]): Unit = {
    init()
    //  1

    val f1 = Array("1,9,5", "2,7,4", "3,8,3")

    val f2 = Array("1,g,h", "2,i,j", "3,k,l")
    val rdd1 = sc.makeRDD(f1).map(_.replaceAll(" ", ""))
    val rdd2 = sc.makeRDD(f2).map(_.replaceAll(" ", ""))

    /*    val rdd1 = sc.textFile("/user/ubuntu/file1.txt")
        val rdd2 = sc.textFile("/user/ubuntu/file2.txt")*/


    /*    def trime: scala.Array[String] => (String, (String, String)) = {
          a => (a(0).replaceAll(" ", ""), (a(1).replaceAll(" ", ""), a(2).replaceAll(" ", "")))
        }

        def timed(a: scala.Array[String] => (String, (String, String))) {
          a(0)._1.replaceAll(" ", "")
          a(1)._1.replaceAll(" ", "")
          a(1)._2._2.replaceAll(" ", "")
        }*/

    val one = rdd1.map {
      _.split(",", -1) match {
        case Array(a, b, c) => (a, (b, c))
      }
    }
    val two = rdd2.map {
      _.split(",", -1) match {
        case Array(a, b, c) => (a, (b, c))
      }
    }

    /*    rdd2.flatMap {
          _.split(",") match {
            case s: String => s
          }
        }*/
    val joinRdd = one.join(two)


    val res = joinRdd.collect

    for (elem <- res) {
      println("=========" + elem)
    }

    //  2
    joinRdd.map {
      case (_, ((_, sum), (_, _))) => sum.toInt
    }.reduce(_ + _)

  }

}
