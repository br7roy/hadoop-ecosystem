 /*
  * Package com.rust.hadoop.myhadoop.mr
  * FileName: MyMaxTempMapper
  * Author:   Rust
  * Date:     2018/9/2 9:27
  */
 package com.rust.hadoop.myhadoop.sort;

 import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

 /**
  * FileName:    MyMaxTempMapper
  * Author:      Rust
  * Date:        2018/9/2
  * Description: 筛选最高气温
  */
 public class MyMaxTempMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	 private static final int MISSING = 9999;


	 /**
	  * @param key
	  * @param value
	  * @param context
	  * @throws IOException
	  * @throws InterruptedException
	  */
	 @Override
	 protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		 //   value String
		 String line = value.toString();
		 //   提取年份
		 String year = line.substring(15, 19);
		 //   气温
		 int airTemperature;
		 //   提取气温值 + / -
		 if (line.charAt(87) == '+') {
			 airTemperature = Integer.parseInt(line.substring(88, 92));
		 } else {
			 airTemperature = Integer.parseInt(line.substring(87, 92));
		 }
		 //   质量
		 String quality = line.substring(92, 93);
		 //   判断气温的有效性
		 if (airTemperature != MISSING && quality.matches("[01459]")) {
			 context.write(new Text(year), new IntWritable(airTemperature));
		 }
		 // context.getCounter("m", Util.getGroup("MaxMapper.map", this.hashCode())).increment(1);
	 }
 }
