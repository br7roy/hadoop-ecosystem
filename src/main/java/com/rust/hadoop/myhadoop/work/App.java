 /*
  * Package com.rust.hadoop.myhadoop.wordcount
  * FileName: WordCountApp
  * Author:   Rust
  * Date:     2018/9/22 16:35
  */
 package com.rust.hadoop.myhadoop.work;

 import com.rust.hadoop.myhadoop.inputformat.whole.WholeInputFormat;
 import org.apache.hadoop.conf.Configuration;
 import org.apache.hadoop.fs.FileSystem;
 import org.apache.hadoop.fs.Path;
 import org.apache.hadoop.io.Text;
 import org.apache.hadoop.mapreduce.Job;
 import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
 import org.apache.hadoop.mapreduce.lib.db.DBInputFormat.NullDBWritable;
 import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
 import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

 import java.io.IOException;

 /**
  * Author:      Rust
  * Date:        2018/10/9 23:38
  */
 public class App {
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
		 job.setJarByClass(App.class);

		 // mapreduce.job.name
		 job.setJobName("area table");// 设置作业名称

		 FileInputFormat.addInputPath(job, new Path(args[0]));// 输入路径
		 job.setInputFormatClass(WholeInputFormat.class); // 设置输入格式类型

		 //设置输出格式类
		 job.setOutputFormatClass(DBOutputFormat.class);
		 // 设置输出格式参数
		 DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver", "jdbc:mysql://192.168.231.1:3306/test", "root",
				 "root");
		 DBOutputFormat.setOutput(job, "t_msap_area_rel", "AREA_REL_NO", "REGION_2_CODE", "REGION_2_NAME", "CITY_CODE",
				 "CITY_NAME", "CREATE_TIME", "UPDATE_TIME", "CREATE_BY", "UPDATE_BY");


		 job.setMapperClass(WordCountMapper.class);        // 设置Mapper类型

		 job.setReducerClass(WordCountReducer.class);    // 设置Reducer类型
		 job.setMapOutputKeyClass(Text.class);
		 job.setMapOutputValueClass(Text.class);

		 // reduce output(job output)类型
		 job.setOutputKeyClass(AreaDBWritable.class);        // 设置输出key类型

		 job.setOutputValueClass(NullDBWritable.class);    // 设置输出value类型

		 job.setNumReduceTasks(1);    //设置reduce的任务数

		 // 开始执行任务
		 System.exit(job.waitForCompletion(true) ? 0 : 1);

	 }
 }
