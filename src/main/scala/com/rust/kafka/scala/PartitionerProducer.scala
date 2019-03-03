package com.rust.kafka.scala

import java.util.Properties

import kafka.producer.DefaultPartitioner
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
  * 使用分区函数让producer来发送消息
  *
  * @author Takho
  *
  */
object PartitionerProducer extends DefaultPartitioner {
  def main(args: Array[String]): Unit = {
    val props: Properties = new Properties
    //Set acknowledgements for producer requests.
    props.put("acks", "all")

    //If the request fails, the producer can automatically retry,
    props.put("retries", "0")

    //Specify buffer size in config
    props.put("batch.size", "16384")

    //Reduce the no of requests less than 0
    props.put("linger.ms", "1")

    //The buffer.memory controls the total amount of memory available to the producer for buffering.
    props.put("buffer.memory", "33554432")

    props.put("partitioner.class", classOf[MyPartitionFunc].getCanonicalName)
    props.put("request.required.acks", "1")


    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("bootstrap.servers", "s101:9092,s102:9092,s103:9092")

    val producer = new KafkaProducer[String, String](props)

    for (i <- 1 to 100) {
      val record = new ProducerRecord[String, String]("test3", "2partition message:" + i)
      producer.send(record)
    }
    println("over")


  }


}
