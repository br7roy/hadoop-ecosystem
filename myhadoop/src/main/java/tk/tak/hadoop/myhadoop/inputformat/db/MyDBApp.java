 /*
  * Package tk.tak.hadoop.myhadoop.wordcount
  * FileName: WordCountApp
  * Author:   Tak
  * Date:     2018/9/22 16:35
  */
 package tk.tak.hadoop.myhadoop.inputformat.db;

 import org.apache.hadoop.conf.Configuration;
 import org.apache.hadoop.fs.FileSystem;
 import org.apache.hadoop.fs.Path;
 import org.apache.hadoop.io.Text;
 import org.apache.hadoop.mapreduce.Job;
 import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
 import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
 import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
 import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

 import java.io.IOException;
 import java.net.InetAddress;

 /**
  * FileName:    WordCountApp
  * Author:      Tak
  * Date:        2018/9/22
  * Description: KeyValueFormat
  */
 public class MyDBApp {
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
		 job.setJarByClass(MyDBApp.class);

		 // mapreduce.job.name
		 job.setJobName("DB import App");// 设置作业名称

		 // mapreduce.output.fileoutputformat.outputdir
		 FileOutputFormat.setOutputPath(job, new Path(args[1]));// 输出路径

		 job.setInputFormatClass(DBInputFormat.class); // 设置数据库输入格式类型

		 //设置数据库配置
		 InetAddress address = InetAddress.getLocalHost();
		 String localIp = address.getHostAddress();

		 conf.set(DBConfiguration.INPUT_QUERY, "select * from users");

		 DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver", "jdbc:mysql://192.168.231.1:3306" + "/test",
				 "root", "root");

		 //	设置输入格式
		 DBInputFormat.setInput(job, UserDBWritable.class, "select id, name, age from users", "select count(*) from " +
				 "users");

		 // 通过程序设置最小切片和最大切片
		 FileInputFormat.setMaxInputSplitSize(job, 30);
		 FileInputFormat.setMinInputSplitSize(job, 30);

		 // 设置Mapper类型
		 job.setMapperClass(DBMapper.class);
		 // 设置输出key类型
		 job.setOutputKeyClass(Text.class);
		 // 设置输出value类型
		 job.setOutputValueClass(Text.class);
		 //设置reduce的任务数
		 job.setNumReduceTasks(0);


		 // 开始执行任务
		 System.exit(job.waitForCompletion(true) ? 0 : 1);

	 }
 }
