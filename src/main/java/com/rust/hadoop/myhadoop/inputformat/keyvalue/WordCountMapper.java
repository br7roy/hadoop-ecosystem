 /*
  * Package com.rust.hadoop.myhadoop.wordcount
  * FileName: WordCountMapper
  * Author:   Rust
  * Date:     2018/9/22 16:36
  */
 package com.rust.hadoop.myhadoop.inputformat.keyvalue;


 import org.apache.hadoop.io.IntWritable;
 import org.apache.hadoop.io.Text;
 import org.apache.hadoop.mapreduce.Mapper;

 import java.io.IOException;


 /**
  * FileName:    WordCountMapper
  * Author:      Rust
  * Date:        2018/9/22
  * Description:
  */
 public class WordCountMapper extends Mapper<Text, Text, Text, IntWritable> {
	 @Override
	 protected void map(Text key, Text value, Context context) throws IOException,
			 InterruptedException {
		 String[] arr = value.toString().substring(1).split(" ");
		 for (String str : arr) {
			 context.write(new Text(str), new IntWritable(1));
		 }
		 context.getCounter("m", "mapper.map." + key.toString()).increment(1);

	 }

 }
