 /*
  * Package tk.tak.kafka
  * FileName: SimpleConsumer
  * Author:   Tak
  * Date:     19/3/4 20:45
  */
 package tk.tak.kafka;

 import kafka.consumer.Consumer;
 import kafka.consumer.ConsumerConfig;
 import kafka.consumer.ConsumerIterator;
 import kafka.consumer.KafkaStream;
 import kafka.javaapi.consumer.ConsumerConnector;
 import kafka.message.MessageAndMetadata;
 import kafka.utils.ZkUtils;

 import java.util.HashMap;
 import java.util.List;
 import java.util.Map;
 import java.util.Properties;

 /**
  * 一个简单的消费者
  *
  * @author Tak
  */
 public class SimpleConsumer {
	 public static void main(String[] args) {
		 Properties properties = new Properties();
		 properties.put("zookeeper.connect", "s101:2181");
		 properties.put("group.id", "g1");
		 properties.put("zookeeper.session.timeout.ms", "5000");
		 properties.put("zookeeper.sync.time.ms", "250");
		 properties.put("auto.commit.interval.ms", "1000");

		 //	删除节点偏移量消费信息，这样就从头开始读取数据了
		 ZkUtils.maybeDeletePath("s101:2181", "/consumers/" + "g1");
		 //	这个值代表偏移量的重置位置，设为smallest代表从头开始读取
		 properties.put("auto.offset.reset", "smallest");
		 // 创建消费者配置对象
		 ConsumerConfig consumerConfig = new ConsumerConfig(properties);

		 // 创建连接器
		 ConsumerConnector connector = Consumer.createJavaConsumerConnector(consumerConfig);

		 Map<String, Integer> topic = new HashMap<>();
		 topic.put("test3", 1);
		 Map<String, List<KafkaStream<byte[], byte[]>>> streamMap = connector.createMessageStreams(topic);
		 List<KafkaStream<byte[], byte[]>> test3 = streamMap.get("test3");

		 for (KafkaStream<byte[], byte[]> metadata : test3) {
			 ConsumerIterator<byte[], byte[]> iterator = metadata.iterator();
			 while (iterator.hasNext()) {
				 MessageAndMetadata<byte[], byte[]> next = iterator.next();
				 byte[] bs = next.message();
				 System.out.println("topic hash:" + topic.hashCode() + "message from topic:" + new String(bs));
			 }
		 }
		 connector.shutdown();
	 }
 }
