 /*
  * Package com.rust.hadoop.myhadoop.mr.chain
  * FileName: MapperB
  * Author:   Rust
  * Date:     2018/10/27 15:04
  */
 package com.rust.hadoop.myhadoop.mr.chain;

 import org.apache.hadoop.io.IntWritable;
 import org.apache.hadoop.io.Text;
 import org.apache.hadoop.mapreduce.Mapper;

 import java.io.IOException;
 import java.net.InetAddress;

 /**
  * @author Rust
  */
 public class MapperB extends Mapper<Text, IntWritable, Text, IntWritable> {
	 @Override
	 protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
		 context.write(key, value);
		 context.getCounter("m", "MB." + InetAddress.getLocalHost().getHostName()).increment(1L);		 System.out.println("MA." + this.hashCode() + ":" + value.toString());
		 System.out.println("MC." + this.hashCode() + ":" + value.toString());
	 }
 }
