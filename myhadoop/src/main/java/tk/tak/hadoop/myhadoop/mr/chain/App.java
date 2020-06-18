 /*
  * Package tk.tak.hadoop.myhadoop.mr.chain
  * FileName: App
  * Author:   Tak
  * Date:     2018/10/27 15:19
  */
 package tk.tak.hadoop.myhadoop.mr.chain;

 import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

 /**
  * @author Tak
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
   job.setJobName("Chain App");
   // 输入路径
   FileInputFormat.addInputPath(job, new Path(args[0]));
   // 输出路径
   FileOutputFormat.setOutputPath(job, new Path(args[1]));


   // 设置链条
   ///////////////////Mapper///////////////
   Configuration aConf = new Configuration(false);
   ChainMapper.addMapper(job, MapperA.class, LongWritable.class, Text.class, Text.class, IntWritable.class, aConf);

   Configuration bConf = new Configuration(false);
   ChainMapper.addMapper(job, MapperB.class, Text.class, IntWritable.class, Text.class, IntWritable.class, bConf);


   /////////////////Reducer/////////////////

   Configuration rConf = new Configuration(false);
   ChainReducer.setReducer(job, ReducerA.class, Text.class, IntWritable.class, Text.class, IntWritable.class, rConf);



   Configuration cConf = new Configuration(false);
   ChainMapper.addMapper(job, MapperC.class, Text.class, IntWritable.class, Text.class, IntWritable.class, cConf);




   // 开始执行任务
   System.exit(job.waitForCompletion(true) ? 0 : 1);




  }
 }
