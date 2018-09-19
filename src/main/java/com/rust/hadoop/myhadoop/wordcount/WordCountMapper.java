 /*
  * Package com.rust.hadoop.myhadoop.wordcount
  * FileName: WordCountMapper
  * Author:   Rust
  * Date:     2018/9/22 16:36
  */
 package com.rust.hadoop.myhadoop.wordcount;


 import com.rust.hadoop.myhadoop.mr.Util;
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
	 protected void setup(Context context) {
		 context.getCounter("m", Util.getGroup2("WCMapper.setup", this.hashCode())).increment(1);
	 }

	 @Override
	 protected void map(LongWritable longWritable, Text value, Context context) throws IOException,
			 InterruptedException {

		 String line = value.toString();

		 String[] ss = line.split(" ");
		 for (String s : ss) {
			 context.write(new Text(s), new IntWritable(1));
		 }
		 context.getCounter("m", Util.getGroup2("WCMapper.map", this.hashCode())).increment(1);
	 }

	 @Override
	 protected void cleanup(Context context) throws IOException, InterruptedException {
		 context.getCounter("m", Util.getGroup2("WCMapper.cleanup", this.hashCode())).increment(1);
	 }
 }
