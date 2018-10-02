 /*
  * Package com.rust.hadoop.myhadoop.wordcount
  * FileName: WordCountApp
  * Author:   Rust
  * Date:     2018/9/22 16:35
  */
 package com.rust.hadoop.myhadoop.inputformat.whole;

 import org.apache.hadoop.conf.Configuration;
 import org.apache.hadoop.fs.FileSystem;
 import org.apache.hadoop.fs.Path;
 import org.apache.hadoop.io.IntWritable;
 import org.apache.hadoop.io.Text;
 import org.apache.hadoop.mapreduce.Job;
 import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
 import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

 import java.io.IOException;

 /**
  * FileName:    WordCountApp
  * Author:      Rust
  * Date:        2018/9/22
  * Description: CombineFileInputFormat合成文件输入格式
  */
 public class WordCountApp {
	 public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		 if (args.length != 2) {
			 System.err.println("Usage: WordCount <input path> <output path>");
		 }
		 Job job = Job.getInstance();
		 Configuration conf = job.getConfiguration();
		 FileSystem fs = FileSystem.get(conf);
		 fs.delete(new Path(args[1]), true);
		 System.out.println(conf.get("fs.defaultFS"));

		 // mapreduce.job.jar
		 job.setJarByClass(WordCountApp.class);

		 // mapreduce.job.name
		 job.setJobName("Word count");// 设置作业名称

		 // mapreduce.input.fileinputformat.inputdir
		 FileInputFormat.addInputPath(job, new Path(args[0]));// 输入路径

		 // mapreduce.output.fileoutputformat.outputdir
		 FileOutputFormat.setOutputPath(job, new Path(args[1]));// 输出路径

		 job.setInputFormatClass(WholeInputFormat.class); // 设置输入格式类型

		 // 通过程序设置最小切片和最大切片
		 FileInputFormat.setMaxInputSplitSize(job, 30);
		 FileInputFormat.setMinInputSplitSize(job, 30);


		 job.setMapperClass(WordCountMapper.class);        // 设置Mapper类型

		 job.setReducerClass(WordCountReducer.class);    // 设置Reducer类型

		 job.setOutputKeyClass(Text.class);        // 设置输出key类型

		 job.setOutputValueClass(IntWritable.class);    // 设置输出value类型

		 job.setNumReduceTasks(1);    //设置reduce的任务数


		 // 开始执行任务
		 System.exit(job.waitForCompletion(true) ? 0 : 1);

	 }
 }
