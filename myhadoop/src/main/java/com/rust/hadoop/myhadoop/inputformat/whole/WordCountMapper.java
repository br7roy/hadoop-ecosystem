 /*
  * Package com.rust.hadoop.myhadoop.wordcount
  * FileName: WordCountMapper
  * Author:   Rust
  * Date:     2018/9/22 16:36
  */
 package com.rust.hadoop.myhadoop.inputformat.whole;


 import org.apache.hadoop.io.BytesWritable;
 import org.apache.hadoop.io.IntWritable;
 import org.apache.hadoop.io.NullWritable;
 import org.apache.hadoop.io.Text;
 import org.apache.hadoop.mapreduce.Mapper;

 import java.io.IOException;
 import java.nio.charset.StandardCharsets;


 /**
  * FileName:    WordCountMapper
  * Author:      Rust
  * Date:        2018/9/22
  * Description:
  */
 public class WordCountMapper extends Mapper<NullWritable, BytesWritable, Text, IntWritable> {
	 @Override
	 protected void map(NullWritable key, BytesWritable value, Context context) throws IOException,
			 InterruptedException {
		 Text text = new Text();
		 IntWritable one = new IntWritable(1);
		 // 整个文本数据
		 byte[] data = value.copyBytes();
		 String arr = new String(data, StandardCharsets.UTF_8);
		 System.out.println(arr.length());
		 String[] ss = arr.split(" |\r\n");
		 for (String s : ss) {
			 text.set(s);
			 context.write(text, one);
		 }
	 }

 }
