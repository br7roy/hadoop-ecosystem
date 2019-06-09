 /*
  * Package com.rust.hadoop.myhadoop.join
  * FileName: MyMapper
  * Author:   Rust
  * Date:     2018/10/21 13:51
  */
 package com.rust.hadoop.myhadoop.join;

 import org.apache.hadoop.io.IntWritable;
 import org.apache.hadoop.io.LongWritable;
 import org.apache.hadoop.io.Text;
 import org.apache.hadoop.mapreduce.Mapper;

 import java.io.IOException;

 /**
  * 订单信息
  *
  * @author Rust
  */
 public class MyMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
	 @Override
	 protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		 String string = value.toString();
		 String[] split = string.split("\t");
		 context.write(new IntWritable(Integer.parseInt(split[3])), value);
	 }
 }
