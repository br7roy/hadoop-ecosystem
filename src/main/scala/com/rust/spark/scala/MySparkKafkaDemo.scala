package com.rust.spark.scala

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

/**
  * @author Takho
  */
object MySparkKafkaDemo {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[4]")
    val ssc = new StreamingContext(conf, Seconds(1))
    ssc.checkpoint("checkpoint")

    val gId = "g1"
    val topicName = "words_topic"

    val topics = Array(topicName)
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "s101:9092,s102:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> gId,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    val lines = stream.map(record => record.value)

    val wordCounts = lines.flatMap(_.split(" ")).map(r => (r, 1)).reduceByKeyAndWindow(_ + _, _ - _, Seconds(5), Seconds(2), 2)

    wordCounts.print()
    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate
  }
}
