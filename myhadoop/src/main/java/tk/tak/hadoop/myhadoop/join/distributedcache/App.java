 /*
  * Package tk.tak.hadoop.myhadoop.mr
  * FileName: App
  * Author:   Tak
  * Date:     2018/9/2 10:23
  */
 package tk.tak.hadoop.myhadoop.join.distributedcache;

 import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;

 /**
  * FileName:    App
  * Author:      Tak
  * Date:        2018/9/2
  * Description: MapperReducer App类
  */
 public class App {
	 public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		 if (args.length != 3) {
			 System.err.println("Usage: MaxTemperature <input path> <output path>");
		 }
		 Job job = Job.getInstance();
		 Configuration conf = job.getConfiguration();
		 FileSystem fs = FileSystem.get(conf);
		 fs.delete(new Path(args[1]), true);
		 System.out.println(conf.get("fs.defaultFS"));
		 job.setJarByClass(App.class);

		 // 创建缓存文件uri
		 // UriBuilder.fromPath();
		 URI uri = URI.create(args[2]);
		 job.addCacheFile(uri);


		 // 设置作业名称
		 job.setJobName("Join app");

		 // "hdfs://s100:8020/user/ubuntu/join/customers.txt"

		 conf.set("customersdir", args[2]);
		 // 输入路径
		 FileInputFormat.addInputPath(job, new Path(args[0]));
		 // 输出路径
		 FileOutputFormat.setOutputPath(job, new Path(args[1]));



		 // 设置Mapper类型
		 job.setMapperClass(MyMapper.class);
		 // 设置Reducer类型
		 job.setReducerClass(MyReduce.class);
		 // 设置输出key类型
		 job.setOutputKeyClass(Text.class);
		 // 设置输出value类型
		 job.setOutputValueClass(NullWritable.class);

		 job.setMapOutputKeyClass(IntWritable.class);
		 job.setMapOutputValueClass(Text.class);


		 job.setNumReduceTasks(2);

		 // 开始执行任务
		 System.exit(job.waitForCompletion(true) ? 0 : 1);
	 }
 }

