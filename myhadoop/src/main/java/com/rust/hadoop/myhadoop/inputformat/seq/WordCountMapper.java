 /*
  * Package com.rust.hadoop.myhadoop.wordcount
  * FileName: WordCountMapper
  * Author:   Rust
  * Date:     2018/9/22 16:36
  */
 package com.rust.hadoop.myhadoop.inputformat.seq;


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
 public class WordCountMapper extends Mapper<IntWritable, Text, Text, IntWritable> {
	 @Override
	 protected void map(IntWritable key, Text value, Context context) throws IOException,
			 InterruptedException {
		 context.write(value, new IntWritable(1));
		 context.getCounter("m", "mapper.map." + key.toString()).increment(1);

	 }

 }
