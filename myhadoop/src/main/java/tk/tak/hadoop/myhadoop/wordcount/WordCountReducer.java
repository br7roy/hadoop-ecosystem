 /*
  * Package tk.tak.hadoop.myhadoop.wordcount
  * FileName: WordCountReducer
  * Author:   Tak
  * Date:     2018/9/22 16:38
  */
 package tk.tak.hadoop.myhadoop.wordcount;

 import tk.tak.hadoop.myhadoop.mr.Util;
 import org.apache.hadoop.io.IntWritable;
 import org.apache.hadoop.io.Text;
 import org.apache.hadoop.mapreduce.Reducer;

 import java.io.IOException;
 import java.util.concurrent.atomic.AtomicInteger;

 /**
  * FileName:    WordCountReducer
  * Author:      Tak
  * Date:        2018/9/22
  * Description:
  */
 public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	 @Override
	 protected void setup(Context context) throws IOException, InterruptedException {
		 context.getCounter("r", Util.getGroup2("WCReducer.setup", this.hashCode())).increment(1);
	 }

	 @Override
	 protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
			 InterruptedException {
		 AtomicInteger count = new AtomicInteger();
		 values.forEach(k -> count.addAndGet(k.get()));
		 context.write(key, new IntWritable(count.get()));

		 context.getCounter("r", Util.getGroup2("WCReducer.reduce", this.hashCode())).increment(1);

	 }

	 @Override
	 protected void cleanup(Context context) throws IOException, InterruptedException {
		 context.getCounter("r", Util.getGroup2("WCReducer.cleanup", this.hashCode())).increment(1);
	 }
 }
