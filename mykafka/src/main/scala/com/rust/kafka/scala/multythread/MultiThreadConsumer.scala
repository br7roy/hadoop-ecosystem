package com.rust.kafka.scala.multythread

import java.util
import java.util.{HashMap, List, Map, Properties}

import kafka.consumer.{Consumer, ConsumerConfig, ConsumerIterator, KafkaStream}
import kafka.javaapi.consumer.ConsumerConnector
import kafka.message.MessageAndMetadata
import kafka.utils.ZkUtils

/**
  * @author Tak
  */
object MultiThreadConsumer {
  def main(args: Array[String]): Unit = {
    val properties: Properties = new Properties
    properties.put("zookeeper.connect", "10.0.30.49:2181")
    properties.put("group.id", "g1")
    properties.put("zookeeper.session.timeout.ms", "5000")
    properties.put("zookeeper.sync.time.ms", "250")
    properties.put("auto.commit.interval.ms", "1000")

    //	删除节点偏移量消费信息，这样就从头开始读取数据了
    ZkUtils.maybeDeletePath("10.0.30.49:2181", "/consumers/" + "g1")
    //	这个值代表偏移量的重置位置，设为smallest代表从头开始读取
    properties.put("auto.offset.reset", "smallest")
    // 创建消费者配置对象
    val consumerConfig: ConsumerConfig = new ConsumerConfig(properties)

    // 创建连接器
    val connector: ConsumerConnector = Consumer.createJavaConsumerConnector(consumerConfig)

    val topic: util.Map[String, Integer] = new util.HashMap[String, Integer]
    topic.put("test3", 1)
    val streamMap: util.Map[String, util.List[KafkaStream[Array[Byte], Array[Byte]]]] = connector.createMessageStreams(topic)
    val test3: util.List[KafkaStream[Array[Byte], Array[Byte]]] = streamMap.get("test3")

    import scala.collection.JavaConversions._
    for (metadata <- test3) {
      val iterator: ConsumerIterator[Array[Byte], Array[Byte]] = metadata.iterator
      while ( {
        iterator.hasNext
      }) {
        val next: MessageAndMetadata[Array[Byte], Array[Byte]] = iterator.next
        val bs: Array[Byte] = next.message
        System.out.println("topic hash:" + topic.hashCode + "message from topic:" + new String(bs))
      }
    }
    connector.shutdown()
  }

}
