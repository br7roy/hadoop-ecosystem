 /*
  * Package com.rust.flink
  * FileName: WikipediaAnalysis
  * Author:   Rust
  * Date:     2019/2/25 16:17
  * Description:
  * History:
  *===============================================================================================
  *   author：          time：                             version：           desc：
  *   Rust           2019/2/25  16:17                      1.0
  *===============================================================================================
  */
 package com.rust.flink.java;

 import org.apache.flink.api.common.functions.FoldFunction;
 import org.apache.flink.api.java.functions.KeySelector;
 import org.apache.flink.api.java.tuple.Tuple2;
 import org.apache.flink.streaming.api.datastream.DataStream;
 import org.apache.flink.streaming.api.datastream.KeyedStream;
 import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
 import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
 import org.apache.flink.streaming.api.windowing.time.Time;
 import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
 import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;

 /**
  * @author Rust
  */
 public class WikipediaAnalysis {


	 public static void main(String[] args) throws Exception {
		 StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		 DataStream<WikipediaEditEvent> edits = env.addSource(new WikipediaEditsSource());

		 KeyedStream<WikipediaEditEvent, String> keyedStream = edits.keyBy(new KeySelector<WikipediaEditEvent, String>() {
			 @Override
			 public String getKey(WikipediaEditEvent wikipediaEditEvent) throws Exception {
				 return wikipediaEditEvent.getUser();
			 }
		 });

		 SingleOutputStreamOperator<Tuple2<String, Long>> res = keyedStream.timeWindow(Time.seconds(5))
				 .fold(new Tuple2<String, Long>("", 0L), new FoldFunction<WikipediaEditEvent, Tuple2<String, Long>>() {
					 @Override
					 public Tuple2<String, Long> fold(Tuple2<String, Long> tuple2, WikipediaEditEvent o) throws Exception {
						 tuple2.f0 = o.getUser();
						 tuple2.f1 += o.getByteDiff();
						 return tuple2;
					 }
				 });
		 res.print();
		 env.execute("start");


	 }


 }
