 /*
  * Package com.rust.hadoop.myhadoop.mr
  * FileName: App
  * Author:   Rust
  * Date:     2018/9/2 10:23
  */
 package com.rust.hadoop.myhadoop.sort.samper;

 import org.apache.hadoop.conf.Configuration;
 import org.apache.hadoop.fs.FileSystem;
 import org.apache.hadoop.fs.Path;
 import org.apache.hadoop.io.IntWritable;
 import org.apache.hadoop.io.Text;
 import org.apache.hadoop.mapreduce.Job;
 import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
 import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
 import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
 import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
 import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

 import java.io.IOException;

 /**
  * @author Rust
  * Description: MapperReducer App类
  */
 public class App {
	 public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		 Job job = Job.getInstance();
		 Configuration conf = job.getConfiguration();
		 FileSystem fs = FileSystem.get(conf);
		 fs.delete(new Path(args[1]), true);
		 fs.delete(new Path(args[2]), true);
		 System.out.println(conf.get("fs.defaultFS") + job.getJobName() + "\r\n--------->starting!!!");


		 // RandomSampler
		 // 第一个参数表示key会被选中的概率
		 // 第二个参数是一个选取samples数
		 // 第三个参数是最大读取input,你打算把他采样出来之后的key分成多少个切片,第三个参数指定最大的切片数

		 InputSampler.RandomSampler<IntWritable, Text> sampler = new InputSampler.RandomSampler<>(1, 100,
				 3);

		 // 设置分区文件(SeqFile: key(Key) - Value(null))
		 TotalOrderPartitioner.setPartitionFile(conf, new Path(args[1]));

		 // 设置作业名称
		 job.setJobName("Total-Sort");
		 job.setJarByClass(App.class);


		 job.setMapperClass(MyMaxTempMapper.class);
		 job.setReducerClass(MyMaxTempReducer.class);
		 job.setInputFormatClass(SequenceFileInputFormat.class);
		 job.setOutputKeyClass(IntWritable.class);
		 job.setOutputValueClass(Text.class);


		 // 设置分区数
		 // reduce数量和sampler的设置是相同的
		 // reduce的数量不能超过sample的最大split数
		 job.setNumReduceTasks(3);

		 // partitioner class设置成TotalOrderPartitioner
		 job.setPartitionerClass(TotalOrderPartitioner.class); //设置全排序分区函数

		 FileInputFormat.setInputPaths(job, args[0]);
		 FileOutputFormat.setOutputPath(job, new Path(args[2]));


		 InputSampler.writePartitionFile(job, sampler);

/*		 SequenceFileOutputFormat.setCompressOutput(job, true);
		 SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		 SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);*/

		 // Add to DistributedCache
		 // 添加分区文件到分布式缓存
/*		 String partitionFile = TotalOrderPartitioner.getPartitionFile(conf);
		 URI partitionUri = new URI(partitionFile);
		 job.addCacheFile(partitionUri);*/

		 // 开始执行任务
		 System.exit(job.waitForCompletion(true) ? 0 : 1);
	 }
 }

