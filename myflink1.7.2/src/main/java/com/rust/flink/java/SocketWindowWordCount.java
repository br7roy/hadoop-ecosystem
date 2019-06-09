 /*
  * Package com.rust.flink.java
  * FileName: SocketWindowWordCount
  * Author:   Rust
  * Date:     2019/2/26 15:04
  * Description:
  * History:
  *===============================================================================================
  *   author：          time：                             version：           desc：
  *   Rust           2019/2/26  15:04                      1.0
  *===============================================================================================
  */
 package com.rust.flink.java;

 import org.apache.flink.api.common.functions.FlatMapFunction;
 import org.apache.flink.api.common.functions.MapFunction;
 import org.apache.flink.api.java.utils.ParameterTool;
 import org.apache.flink.streaming.api.datastream.DataStreamSource;
 import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
 import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
 import org.apache.flink.streaming.api.windowing.time.Time;
 import org.apache.flink.util.Collector;

 /**
  * 处理无界数据的WordCount
  * 使用WordCount作为数据聚集模型
  * Java版本
  * Source为nc
  * Sink为logger
  *
  * @author Rust
  */
 public class SocketWindowWordCount {

	 public static void main(String[] args) throws Exception {

		 int port = ParameterTool.fromArgs(args).getInt("port", 9000);


		 StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		 DataStreamSource<String> dss = env.socketTextStream("s100", port);
		 SingleOutputStreamOperator<WordCount> sum = dss.flatMap(new FlatMapFunction<String, String>() {
			 @Override
			 public void flatMap(String s, Collector<String> collector) throws Exception {
				 String[] split = s.split("\\s");
				 for (String s1 : split) {
					 collector.collect(s1);
				 }
			 }
		 }).map(new MapFunction<String, WordCount>() {
			 @Override
			 public WordCount map(String s) throws Exception {
				 return new WordCount(s, 1);
			 }
		 }).keyBy("word").timeWindow(Time.seconds(10), Time.seconds(2))
				 .sum("count");

		 sum.print();
		 env.execute(" java stream word count with static class");

	 }

	 public static class WordCount {
		 public String word;
		 public long count;

		 public WordCount() {
		 }

		 public WordCount(String word, long count) {
			 this.word = word;
			 this.count = count;
		 }

		 @Override
		 public String toString() {
			 return "WordCount{" +
					 "word='" + word + '\'' +
					 ", count=" + count +
					 '}';
		 }
	 }
 }
