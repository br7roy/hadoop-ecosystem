/*
 * Package:  tk.tak.hbase.mr
 * FileName: WordCountReducer
 * Author:   Tak
 * Date:     19/3/31 18:01
 * email:    bryroy@gmail.com
 */
package tk.tak.hbase.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Tak
 */
public class WordCountReducer extends Reducer<Text, IntWritable,Text,IntWritable> {
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		AtomicInteger ai = new AtomicInteger();
		values.iterator().forEachRemaining(res->{
			ai.incrementAndGet();
		});
		context.write(key, new IntWritable(ai.intValue()));
	}
}
