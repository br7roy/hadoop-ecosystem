 /*
  * Package com.rust.hadoop.myhadoop.join
  * FileName: MyMapper
  * Author:   Rust
  * Date:     2018/10/21 13:51
  */
 package com.rust.hadoop.myhadoop.join2;

 import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

 /**
  * 订单信息
  *
  * @author Rust
  */
 public class MyMapper extends Mapper<LongWritable, Text, ComboKey, Text> {
	 @Override
	 protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		 FileSplit inputSplit = (FileSplit) context.getInputSplit();
		 String path = inputSplit.getPath().toString();
		 int flag = 0;

		 if (path.contains("order")) {
			 flag = 1;
		 } else if (path.contains("customer")) {
			 flag = 0;
		 } else {
			 context.getCounter("mapper", "do nothing").increment(1);
		 }

		 int cid = 0;
		 String string = value.toString();
		 String[] split = string.split("\t");
		 if (flag == 0) {

			 cid = new Integer(split[0]);
		 } else if (flag == 1) {
			 cid = new Integer(split[3]);
		 } else {
			 context.getCounter("mapper", "do nothing").increment(1);
			 System.out.println("do nothing");
		 }

		 context.write(new ComboKey(cid, flag), value);
	 }
 }
