 /*
  * Package tk.tak.hadoop.myhadoop.join
  * FileName: MyReduce
  * Author:   Tak
  * Date:     2018/10/21 14:06
  */
 package tk.tak.hadoop.myhadoop.join2;

 import org.apache.hadoop.io.NullWritable;
 import org.apache.hadoop.io.Text;
 import org.apache.hadoop.mapreduce.Reducer;

 import java.io.IOException;

 /**
  * @author Tak
  */
 public class MyReduce extends Reducer<ComboKey, Text, Text, NullWritable> {


	 /**
	  * 每个分组调用一遍 key是变化的
	  * 当进入reduce 之前，如果设置过分组函数，重新定义compareTo方法
	  * 是可以让组合key以" 值不相等" 的形式进入同一个reduce 进行聚合
	  *
	  * @param key 变化的
	  * @throws IOException
	  * @throws InterruptedException
	  */
	 @Override
	 protected void reduce(ComboKey key, Iterable<Text> values, Context context) throws IOException,
			 InterruptedException {
		 StringBuilder sb = new StringBuilder();
		 values.forEach(text -> {
			 if (key.getFlag() == 0) {
				 sb.append(text.toString());
			 } else {
				 try {
					 context.write(new Text(sb.toString() + " " + text.toString()), NullWritable.get());
				 } catch (IOException | InterruptedException e) {
					 e.printStackTrace();
				 }
			 }
		 });
/*		 Iterator<Text> iterator = values.iterator();
		 String cust = iterator.next().toString();

		 iterator.forEachRemaining(text -> {
			 try {
				 context.write(new Text(cust + " " + text.toString()), NullWritable.get());
			 } catch (IOException | InterruptedException e) {
				 e.printStackTrace();
			 }
		 });*/

	 }
 }
