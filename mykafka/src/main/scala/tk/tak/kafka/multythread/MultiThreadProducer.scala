package com.tak.kafka.scala.multythread

import java.util.Properties
import java.util.concurrent.{ArrayBlockingQueue, ThreadPoolExecutor, TimeUnit}

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}

/**
  * @author Tak
  */
object MultiThreadProducer {

//  val poolExecutor = new ThreadPoolExecutor(2, 4, 60, TimeUnit.SECONDS, new ArrayBlockingQueue[Runnable](300))
//  Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
//    override def run(): Unit = poolExecutor.shutdown()
//  }))


  class MultiThreadWorker extends Runnable {
    override def run(): Unit = ???
  }

  def main(args: Array[String]): Unit = {
    val props = new Properties
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

    props.put("producer.type", "async")


    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("bootstrap.servers", "10.0.30.195:9092,10.0.30.194:9092")

    val producer = new KafkaProducer[String, String](props)

    val record = new ProducerRecord[String, String]("test3", Integer.toString(1), " from win10 client")

    producer.send(record, new Callback {
      override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = {
        println("onCompletion call")
        println(recordMetadata)
      }
    })
    producer.close()
    System.out.println("over")

//    while (!poolExecutor.isTerminated)
//      Thread.`yield`()

  }


}
