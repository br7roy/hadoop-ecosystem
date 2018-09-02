 /*
  * Package com.rust.hadoop.myhadoop.mr
  * FileName: MyMaxTempReducer
  * Author:   Rust
  * Date:     2018/9/2 9:57
  */
 package com.rust.hadoop.myhadoop.mr;

 import org.apache.hadoop.io.IntWritable;
 import org.apache.hadoop.io.Text;
 import org.apache.hadoop.mapreduce.Reducer;

 import java.io.IOException;
 import java.net.InetAddress;

 /**
  * FileName:    MyMaxTempReducer
  * Author:      Rust
  * Date:        2018/9/2
  * Description: reduce阶段
  */
 public class MyMaxTempReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	 @Override
	 protected void reduce(Text keyIn, Iterable<IntWritable> valueIn, Context context) throws IOException, InterruptedException {
		 // 打印map task 执行主机地址
		 InetAddress address = InetAddress.getLocalHost();
		 System.out.println("reduce:" + address.getHostAddress());
		 final Integer[] max = {0};
		 // 提取年度最高气温
		 valueIn.forEach(intWritable -> {
			 if (intWritable.get() > max[0]) {
				 max[0] = intWritable.get();
			 }
		 });
		 // 写入输出
		 context.write(keyIn, new IntWritable(max[0]));
	 }
 }
