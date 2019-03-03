 /*
  * Package com.rust.kafka
  * FileName: SimpleProducer
  * Author:   Takho
  * Date:     19/3/3 14:26
  */
 package com.rust.kafka;

 import org.apache.kafka.clients.producer.KafkaProducer;
 import org.apache.kafka.clients.producer.Producer;
 import org.apache.kafka.clients.producer.ProducerRecord;

 import java.util.Properties;

 /**
  * 简单的Producer
  *
  * @author Takho
  */
 public class SimpleProducer {


	 public SimpleProducer() {

	 }

	 public static void main(String[] args) {
		 Producer<String, String> producer;
		 Properties props = new Properties();
		 //Set acknowledgements for producer requests.
		 props.put("acks", "all");

		 //If the request fails, the producer can automatically retry,
		 props.put("retries", 0);

		 //Specify buffer size in config
		 props.put("batch.size", 16384);

		 //Reduce the no of requests less than 0
		 props.put("linger.ms", 1);

		 //The buffer.memory controls the total amount of memory available to the producer for buffering.
		 props.put("buffer.memory", 33554432);

		 props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		 props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		 props.put("bootstrap.servers", "s101:9092");
		 props.put("producer.type", "async");
		 producer = new KafkaProducer<>(props);

		 ProducerRecord<String, String> record = new ProducerRecord<>("test3", Integer.toString(1), "HelloWorld from win7 client");

		 producer.send(record);
		 producer.close();
		 System.out.println("over");

	 }
 }
