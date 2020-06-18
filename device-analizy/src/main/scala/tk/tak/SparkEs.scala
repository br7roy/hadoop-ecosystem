package tk.tak

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.sparkContextFunctions
/**
 * @author fth
 */
object SparkEs {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("estest").setMaster("local[*]")

    //  conf.set("es.resource", "nm_1606/news") //指定读取的索引名称
    conf.set("es.nodes", "192.168.20.120,192.168.20.121,192.168.20.122,192.168.20.123,192.168.20.124")
    conf.set("es.port","9200")
    conf.set("es.query", " {  \"query\": {    \"match_all\": {    }  }}")  //使用query字符串对结果进行过滤
    conf.set("spark.akka.logLifecycleEvents","true")
    conf.set("es.nodes.discovery", "false")
    conf.set("es.nodes.data.only", "false")
    val sc = new SparkContext(conf)
    val rdd = sc.esRDD("mactrace201902/mactrace")
//    rdd.foreach( line => println(line._1+"\n"+line._2) )
    println(rdd.count)




  }
}
