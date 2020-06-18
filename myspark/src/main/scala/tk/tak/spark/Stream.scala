package tk.tak.spark

import java.net.InetAddress
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author Tak
  *
  */
object Stream {
  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local[5]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(5))
    // Create a DStream that will connect to hostname:port, like localhost:9999
    val hName = InetAddress.getLocalHost.getHostName
    //  使用socket连接 端口9999,需要事先监听端口9999  nc -lk 9999
    //  val lines = ssc.socketTextStream(hName, 9999)

    //  def flg (path: Path) = false

    def flg = (_: Path) => true

    //  监控目录下的文件
    val lines = ssc.fileStream[LongWritable, Text, TextInputFormat]("file:///home/ubuntu/spark/file_stream", flg, newFilesOnly = false)
    //    val lines = ssc.fileStream[LongWritable, Text, TextInputFormat]("file:///home/ubuntu/spark/file_stream", _ => false, newFilesOnly = false)
    // Split each line into words
    val words = lines.flatMap(_._2.toString.split("\\s+"))
    // Count each word in each batch
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    // Print the first ten elements of each RDD generated in this DStream to the console
    wordCounts.print()
    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate
  }
}
