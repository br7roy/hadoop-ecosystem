package com.rust.scala.cdh

/**
  * @author Rust
  */
object Text19 extends Init {


  def main(args: Array[String]): Unit = {
    init()
    val x = sc.parallelize(1 to 20)
    val y = sc.parallelize(10 to 30)
    val z = operational

    def operational = {


      //  返回两个RDD相同的元素
      x.intersection(y)
    }

    z.collect
  }

}
