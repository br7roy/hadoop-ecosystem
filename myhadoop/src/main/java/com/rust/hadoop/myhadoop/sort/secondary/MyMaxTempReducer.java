 /*
  * Package com.rust.hadoop.myhadoop.mr
  * FileName: MyMaxTempReducer
  * Author:   Rust
  * Date:     2018/9/2 9:57
  */
 package com.rust.hadoop.myhadoop.sort.secondary;

 import org.apache.hadoop.io.NullWritable;
 import org.apache.hadoop.mapreduce.Reducer;

 import java.io.IOException;

 /**
  * FileName:    MyMaxTempReducer
  * Author:      Rust
  * Date:        2018/9/2
  * Description: reduce阶段
  */
 public class MyMaxTempReducer extends Reducer<CombineKey, NullWritable, CombineKey, NullWritable> {


	 /**
	  * 二次排序以后，组合key中的温度就是当年的最高温度
	  *
	  * @param key
	  * @param values
	  * @param context
	  * @throws IOException
	  * @throws InterruptedException
	  */
	 @Override
	 protected void reduce(CombineKey key, Iterable<NullWritable> values, Context context) throws IOException,
			 InterruptedException {
		 context.write(key, NullWritable.get());
	 }


 	/*

	 @Override
	 protected void reduce(Text keyIn, Iterable<IntWritable> valueIn, Context context) throws IOException,
			 InterruptedException {
		 int[] cnt = {0};
		 int[] max = {0};
		 // 提取年度最高气温
		 valueIn.forEach(intWritable -> {
			 if (intWritable.get() > max[0]) {
				 max[0] = intWritable.get();
			 }
			 cnt[0]++;

		 });
		 context.getCounter("r", Util.getGroup("MaxReducer.reduce" + keyIn.toString(), this.hashCode())).increment
		 (cnt[0]);
		 // 写入输出
		 context.write(keyIn, new IntWritable(max[0]));
		 // context.getCounter("r", Util.getGroup("MaxReducer.reduce", this.hashCode())).increment(1);
	 }
*/

 }
