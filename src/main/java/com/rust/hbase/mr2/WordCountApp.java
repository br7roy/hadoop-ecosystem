/*
 * Package:  com.rust.hbase.mr
 * FileName: WordCountApp
 * Author:   Rust
 * Date:     19/3/31 18:06
 * email:    bryroy@gmail.com
 */
package com.rust.hbase.mr2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

/**
 * 将HBase作为MR的输出
 * 输出结果到HBase
 * @author Rust
 */
public class WordCountApp {
	public static void main(String[] args) throws Exception {
		Job job = Job.getInstance();
		Configuration conf = job.getConfiguration();


		// mapreduce.job.jar
		job.setJarByClass(WordCountApp.class);

		// mapreduce.job.name
		job.setJobName("Word count");// 设置作业名称


		job.setInputFormatClass(TableInputFormat.class);


		Scan scan = new Scan();
		TableMapReduceUtil.initTableMapperJob("ns1:t2", scan, WordCountMapper.class, Text.class, IntWritable.class, job);
		conf.set("hbase.zookeeper.quorum", "s101:2181,s102:2181,s103:2181");

		TableMapReduceUtil.initTableReducerJob("ns1:t5", WordCountReducer.class, job);


		job.setMapperClass(WordCountMapper.class);        // 设置Mapper类型

		job.setReducerClass(WordCountReducer.class);    // 设置Reducer类型


		// 开始执行任务
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
