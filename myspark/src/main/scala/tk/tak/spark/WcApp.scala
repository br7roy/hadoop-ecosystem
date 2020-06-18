package tk.tak.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.net.URL

object WcApp {
  def main(args:Array[String]) {
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[4]").set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
    val sc = new SparkContext(conf)
    val input = sc.textFile("file:/home/ubuntu/devicestatus.txt")
    val words = input.flatMap(line => line.split(","))
    //  Transform into pairs and count
    val counts = words.map(word => (word, 1)).reduceByKey { case (x, y) => x + y }
    //  save the word count back out to a text file, causing evaluation
    counts.saveAsTextFile("file:/home/ubuntu/out.txt")
  }
}
