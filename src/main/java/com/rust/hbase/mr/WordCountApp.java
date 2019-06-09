/*
 * Package:  com.rust.hbase.mr
 * FileName: WordCountApp
 * Author:   Rust
 * Date:     19/3/31 18:06
 * email:    bryroy@gmail.com
 */
package com.rust.hbase.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 将HBase作为MR的输入
 * 从HBase中读取数据
 * @author Rust
 */
public class WordCountApp {
	public static void main(String[] args) throws Exception{
		if (args.length != 1) {
			System.err.println("Usage: WordCount <input path> <output path>");
		}
		Job job = Job.getInstance();
		Configuration conf = job.getConfiguration();


		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(args[0]), true);
		System.out.println(conf.get("fs.defaultFS"));

		// mapreduce.job.jar
		job.setJarByClass(WordCountApp.class);

		// mapreduce.job.name
		job.setJobName("Word count");// 设置作业名称


		FileOutputFormat.setOutputPath(job, new Path(args[0]));// 输出路径

		job.setInputFormatClass(TableInputFormat.class);



		Scan scan = new Scan();
		TableMapReduceUtil.initTableMapperJob("ns1:t2", scan, WordCountMapper.class, Text.class, IntWritable.class, job);
		conf.set("hbase.zookeeper.quorum", "s101:2181,s102:2181,s103:2181");



		job.getConfiguration().set(TableInputFormat.INPUT_TABLE, "ns1:t2");


		job.setMapperClass(WordCountMapper.class);        // 设置Mapper类型

		job.setReducerClass(WordCountReducer.class);    // 设置Reducer类型

		job.setOutputKeyClass(Text.class);        // 设置输出key类型

		job.setOutputValueClass(IntWritable.class);    // 设置输出value类型



		// 开始执行任务
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
