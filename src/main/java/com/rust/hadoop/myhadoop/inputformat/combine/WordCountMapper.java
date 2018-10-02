 /*
  * Package com.rust.hadoop.myhadoop.wordcount
  * FileName: WordCountMapper
  * Author:   Rust
  * Date:     2018/9/22 16:36
  */
 package com.rust.hadoop.myhadoop.inputformat.combine;


 import com.google.common.collect.Maps;
import com.rust.hadoop.myhadoop.mr.Util;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;

 /**
  * FileName:    WordCountMapper
  * Author:      Rust
  * Date:        2018/9/22
  * Description:
  */
 public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	 private Map<String, Integer> words;

	 @Override
	 protected void setup(Context context) {
		 // context.getCounter("m", Util.getGroup2("WCMapper.setup", this.hashCode())).increment(1);
		 // 初始化集合
		 this.words = Maps.newHashMap();
	 }

	 @Override
	 protected void map(LongWritable longWritable, Text value, Context context) throws IOException,
			 InterruptedException {

		 if (words.size() > 1000) {
			 sendData(context);
			 return;
		 }

		 String line = value.toString();

		 String[] ss = line.split(" ");
		 for (String words : ss) {
			 add2Map(words);
		 }

	 }

	 @Override
	 protected void cleanup(Context context) throws IOException, InterruptedException {
		 sendData(context);
		 context.getCounter("m", Util.getGroup2("WCMapper.map", this.hashCode())).increment(1);
	 }





	 /**
	  * 发送数据(IPC)
	  * @param context job作业上下文
	  */
	 private void sendData(Context context) {
		 Text key = new Text();
		 IntWritable value = new IntWritable();
		 words.entrySet().iterator().forEachRemaining(k -> {
			 try {
				 key.set(k.getKey());
				 value.set(k.getValue());
				 context.write(key, value);
			 } catch (IOException | InterruptedException e) {
				 e.printStackTrace();
				 System.exit(-1);
			 }
		 });
		 words.clear();
	 }

	 /**
	  * 将单词添加到集合中
	  * @param s word
	  */
	 private void add2Map(String s) {
		 Integer cnt = words.computeIfAbsent(s, k -> 0);
		 words.put(s, cnt + 1);
/*		 words.compute(s, (s1, integer) -> {
			 if (integer == null) {
				 integer = 1;
			 } else {
				 integer = integer + 1;
			 }
			 return integer;
		 });*/
/*		 Integer count = words.get(s);
		 if (count == null) {
			 count = 1;
		 } else {
			 count++;
		 }
		 words.put(s, count);*/
	 }

	 public static void main(String[] args) {
		 Map<String, Integer> words = Maps.newHashMap();
		 for (int i = 0; i < 5; i++) {

		 words.compute("test", (s1, integer) -> {
			 if (integer == null) {
				 integer = 1;
			 } else {
				 integer = integer + 1;
			 }
			 return integer;
		 });
		 }
		 System.out.println(words);
	 }
 }
