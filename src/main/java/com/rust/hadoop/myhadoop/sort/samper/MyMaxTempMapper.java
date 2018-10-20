 /*
  * Package com.rust.hadoop.myhadoop.mr
  * FileName: MyMaxTempMapper
  * Author:   Rust
  * Date:     2018/9/2 9:27
  */
 package com.rust.hadoop.myhadoop.sort.samper;

 import org.apache.hadoop.io.IntWritable;
 import org.apache.hadoop.io.Text;
 import org.apache.hadoop.mapreduce.Mapper;

 import java.io.IOException;

 /**
  * 通过采样器，实现全排序
  *
  * @author Rust
  */
 public class MyMaxTempMapper extends Mapper<IntWritable, Text, IntWritable, Text> {

	 private static final int MISSING = 9999;


	 /**
	  * @param key
	  * @param value
	  * @param context
	  * @throws IOException
	  * @throws InterruptedException
	  */
	 @Override
	 protected void map(IntWritable key, Text value, Context context) throws IOException, InterruptedException {
		 context.write(key, value);
	 }
 }
