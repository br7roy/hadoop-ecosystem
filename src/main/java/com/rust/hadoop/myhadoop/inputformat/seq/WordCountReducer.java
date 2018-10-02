 /*
  * Package com.rust.hadoop.myhadoop.wordcount
  * FileName: WordCountReducer
  * Author:   Rust
  * Date:     2018/9/22 16:38
  */
 package com.rust.hadoop.myhadoop.inputformat.seq;

 import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

 /**
  * FileName:    WordCountReducer
  * Author:      Rust
  * Date:        2018/9/22
  * Description:
  */
 public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	 @Override
	 protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
			 InterruptedException {
		 AtomicInteger count = new AtomicInteger();
		 values.forEach(k -> count.addAndGet(k.get()));
		 context.write(key, new IntWritable(count.get()));
	 }

 }
