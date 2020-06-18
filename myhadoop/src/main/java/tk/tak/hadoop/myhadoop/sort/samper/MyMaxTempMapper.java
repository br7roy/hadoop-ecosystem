 /*
  * Package tk.tak.hadoop.myhadoop.mr
  * FileName: MyMaxTempMapper
  * Author:   Tak
  * Date:     2018/9/2 9:27
  */
 package tk.tak.hadoop.myhadoop.sort.samper;

 import org.apache.hadoop.io.IntWritable;
 import org.apache.hadoop.io.Text;
 import org.apache.hadoop.mapreduce.Mapper;

 import java.io.IOException;

 /**
  * 通过采样器，实现全排序
  *
  * @author Tak
  */
 public class MyMaxTempMapper extends Mapper<IntWritable, Text, IntWritable, Text> {

	 private static final int MISSING = 9999;


	 /**
	  * @param key
	  * @param value
	  * @param context
	  * @throws IOException
	  * @throws InterruptedException
	  */
	 @Override
	 protected void map(IntWritable key, Text value, Context context) throws IOException, InterruptedException {
		 context.write(key, value);
	 }
 }
