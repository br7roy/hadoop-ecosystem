 /*
  * Package tk.tak.hadoop.myhadoop.mr
  * FileName: App
  * Author:   Tak
  * Date:     2018/9/2 10:23
  */
 package tk.tak.hadoop.myhadoop.sort.secondary;

 import org.apache.hadoop.conf.Configuration;
 import org.apache.hadoop.fs.FileSystem;
 import org.apache.hadoop.fs.Path;
 import org.apache.hadoop.io.NullWritable;
 import org.apache.hadoop.mapreduce.Job;
 import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
 import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

 import java.io.IOException;

 /**
  * FileName:    App
  * Author:      Tak
  * Date:        2018/9/2
  * Description: MapperReducer App类
  */
 public class App {
	 public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		 if (args.length != 2) {
			 System.err.println("Usage: MaxTemperature <input path> <output path>");
		 }
		 Job job = Job.getInstance();
		 Configuration conf = job.getConfiguration();
		 FileSystem fs = FileSystem.get(conf);
		 fs.delete(new Path(args[1]), true);
		 System.out.println(conf.get("fs.defaultFS"));
		 job.setJarByClass(App.class);
		 // 设置作业名称
		 job.setJobName("Max Temperature");
		 // 输入路径
		 FileInputFormat.addInputPath(job, new Path(args[0]));
		 // 输出路径
		 FileOutputFormat.setOutputPath(job, new Path(args[1]));
		 // 设置Mapper类型
		 job.setMapperClass(MyMaxTempMapper.class);
		 // 设置Reducer类型
		 job.setReducerClass(MyMaxTempReducer.class);
		 // 设置输出key类型
		 job.setOutputKeyClass(CombineKey.class);
		 // 设置输出value类型
		 job.setOutputValueClass(NullWritable.class);


		 job.setPartitionerClass(YearPartitioner.class);

		 // 在到达reduce之前，设置key的排序方式
		 // job.setSortComparatorClass();


		 job.setGroupingComparatorClass(YearGroupComparator.class);

		 job.setNumReduceTasks(2);


		 // 开始执行任务
		 System.exit(job.waitForCompletion(true) ? 0 : 1);
	 }
 }

