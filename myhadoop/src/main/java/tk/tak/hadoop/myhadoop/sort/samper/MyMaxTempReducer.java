 /*
  * Package tk.tak.hadoop.myhadoop.mr
  * FileName: MyMaxTempReducer
  * Author:   Tak
  * Date:     2018/9/2 9:57
  */
 package tk.tak.hadoop.myhadoop.sort.samper;

 import org.apache.hadoop.io.IntWritable;
 import org.apache.hadoop.io.Text;
 import org.apache.hadoop.mapreduce.Reducer;

 import java.io.IOException;

 /**
  * @author Tak
  * Description: reduce阶段
  */
 public class MyMaxTempReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

	 @Override
	 protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException,
			 InterruptedException {
		 super.reduce(key, values, context);
	 }
 }
