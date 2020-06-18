 /*
  * Package tk.tak.hadoop.myhadoop.wordcount
  * FileName: WordCountApp
  * Author:   Tak
  * Date:     2018/9/22 16:35
  */
 package tk.tak.hadoop.myhadoop.work;

 import org.apache.hadoop.conf.Configuration;
 import org.apache.hadoop.fs.FileSystem;
 import org.apache.hadoop.fs.Path;
 import org.apache.hadoop.io.Text;
 import org.apache.hadoop.mapreduce.Job;
 import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
 import org.apache.hadoop.mapreduce.lib.db.DBInputFormat.NullDBWritable;
 import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
 import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
 import org.slf4j.Logger;
 import org.slf4j.LoggerFactory;

 import java.io.IOException;
 import java.time.LocalDateTime;

 /**
  * Author:      Tak
  * Date:        2018/10/9 23:38
  */
 public class App {
	 private static final Logger LOG = LoggerFactory.getLogger(App.class.getName());

	 public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		 if (args.length != 2) {
			 System.err.println("Usage: WordCount <input path> <output path>");
		 }
		 Job job = Job.getInstance();
		 Configuration conf = job.getConfiguration();
		 FileSystem fs = FileSystem.get(conf);
		 fs.delete(new Path(args[1]), true);
		 System.out.println(conf.get("fs.defaultFS"));
		 System.out.println("ok, start at :" + LocalDateTime.now().toString());
		 // mapreduce.job.jar
		 job.setJarByClass(App.class);

		 // mapreduce.job.name
		 job.setJobName("area table");// 设置作业名称

		 FileInputFormat.addInputPath(job, new Path(args[0]));// 输入路径
		 // job.setInputFormatClass(WholeInputFormat.class); // 设置输入格式类型

		 //设置输出格式类
		 job.setOutputFormatClass(DBOutputFormat.class);
		 // 设置输出格式参数
		 DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver", "jdbc:mysql://192.168.1.19:3308/msap", "msapopr",
				 "Msap123456!");
		 DBOutputFormat.setOutput(job, "t_msap_chl_area_city_rel", "AREA_REL_NO", "REGION_2_CODE", "REGION_2_NAME",
				 "CITY_CODE",
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
