 /*
  * Package com.rust.hadoop.myhadoop.wordcount
  * FileName: WordCountReducer
  * Author:   Rust
  * Date:     2018/9/22 16:38
  */
 package com.rust.hadoop.myhadoop.outputformat.db;

 import org.apache.hadoop.io.IntWritable;
 import org.apache.hadoop.io.Text;
 import org.apache.hadoop.mapreduce.Reducer;
 import org.apache.hadoop.mapreduce.lib.db.DBInputFormat.NullDBWritable;

 import java.io.IOException;
 import java.util.concurrent.atomic.AtomicInteger;

 /**
  * FileName:    WordCountReducer
  * Author:      Rust
  * Date:        2018/9/22
  * Description:
  */
 public class WordCountReducer extends Reducer<Text, IntWritable, WordDBWritable, NullDBWritable> {

	 @Override
	 protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
			 InterruptedException {
		 NullDBWritable ndb = new NullDBWritable();
		 AtomicInteger count = new AtomicInteger();
		 values.forEach(k -> count.addAndGet(k.get()));
		 WordDBWritable dbWritable = new WordDBWritable();
		 dbWritable.setWord(key.toString());
		 dbWritable.setNum(count.get());
		 context.write(dbWritable, ndb);
	 }
 }
