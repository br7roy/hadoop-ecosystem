package tk.tak.spark_exercise.cdh

import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.apache.spark.rdd.RDD

import scala.Option

/**
  * @author Tak
  */
object Text18 extends Init {
  def main(args: Array[String]): Unit = {


    //    val rdd: RDD[Array[Byte]]
    //    Now you have to save this RDD as a SequenceFile. And below is the code snippet.
    //    import org.apache.hadoop.io.compress.GzipCodec
    //    rdd.map(bytesArray => (A.get(), new
    //    B(bytesArray))).saveAsSequenceFile('7output/path",classOt[GzipCodec])
    //    What would be the correct replacement for A and B in above snippet.

    val rdd: RDD[Array[Byte]] = sc.parallelize[Array[Byte]](Seq(Array(1, 2, 3, 4, 5, 6, 7), Array(7, 6, 5, 4, 3, 2, 1)))

    import org.apache.hadoop.io.compress.GzipCodec

    rdd.map(bytesArray => (NullWritable.get(), new BytesWritable(bytesArray))).saveAsSequenceFile("/user/ubuntu/cdhtest/text18", Option(classOf[GzipCodec]))


  }


}

