import java.net.InetAddress

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Tak
  *
  */
object Test2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14), 12)

    def showName: String = {
      val host = InetAddress.getLocalHost.getHostName
      val tname = Thread.currentThread().getName
      host + ":" + tname
    }

    rdd.map(x => while (true) {
      println(showName + "<<==>>" + x)
    }).collect
  }

}
