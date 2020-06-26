package tk.tak.spark_exercise

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Encoder, SparkSession}

/**
  * @author Tak
  */
object Text20Test extends Init with Logging {
  def main(args: Array[String]): Unit = {
    case class People(name: String, age: BigInt, gender: String)


    def seqFunc: (Array[BigInt], People) => Array[BigInt] = {
      (array, people) => {
        array(0) = array(0) + people.age
        array(1) = array(1) + 1
        array
      }
    }

    def combFunc: (Array[BigInt], Array[BigInt]) => Array[BigInt] = {
      (array1, array2) => {
        array2(0) = array1(0) + array2(0)
        array2(1) = array1(1) + 1
        array2
      }
    }

    implicit val impParam: Encoder[People] = org.apache.spark.sql.Encoders.kryo[People]
    val df = spark.read.json("file:///D:\\project\\gitlab\\spark-exercise\\src\\test\\resources\\people.json").as[People]
    val rdd = df.rdd
    rdd.aggregate(Array(BigInt(0), BigInt(0)))(seqFunc, combFunc)


  }

}
