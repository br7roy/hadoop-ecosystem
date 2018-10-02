 /*
  * Package com.rust.hadoop.myhadoop.wordcount
  * FileName: WordCountMapper
  * Author:   Rust
  * Date:     2018/9/22 16:36
  */
 package com.rust.hadoop.myhadoop.inputformat.nline;


 import org.apache.hadoop.io.IntWritable;
 import org.apache.hadoop.io.LongWritable;
 import org.apache.hadoop.io.Text;
 import org.apache.hadoop.mapreduce.Mapper;

 import java.io.IOException;


 /**
  * FileName:    WordCountMapper
  * Author:      Rust
  * Date:        2018/9/22
  * Description:
  */
 public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	 @Override
	 protected void map(LongWritable key, Text value, Context context) throws IOException,
			 InterruptedException {

		 String str = value.toString();

		 String[] arr = str.split(" ");
		 for (String s : arr) {
			 context.write(new Text(s), new IntWritable(1));
		 }

		 context.getCounter("m", "mapper.map." + key.toString()).increment(1);

	 }

 }
