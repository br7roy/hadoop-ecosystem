package tk.tak.kafka

import java.util

import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.common.Cluster

/**
  * 自定义分区函数，返回0，便于观察信息进入哪个分区
  *
  * @author Tak
  *
  */
class MyPartitionFunc extends Partitioner {
  override def partition(topic: String, key: Any, keyBytes: Array[Byte], value: Any, valueBytes: Array[Byte], cluster: Cluster): Int = 0

  override def close(): Unit = ???

  override def configure(configs: util.Map[String, _]): Unit = {}
}
