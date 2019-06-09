/*
 * Package:  com.rust.hbase.mr
 * FileName: WordCountReducer
 * Author:   Rust
 * Date:     19/3/31 18:01
 * email:    bryroy@gmail.com
 */
package com.rust.hbase.mr2;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Rust
 */
public class WordCountReducer extends TableReducer<Text, IntWritable, NullWritable> {
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		AtomicInteger ai = new AtomicInteger();
		values.iterator().forEachRemaining(res -> {
			ai.incrementAndGet();
		});
		Put put = new Put(Bytes.toBytes("row-" + key.toString()));
		put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("word"), Bytes.toBytes(key.toString()));
		put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("nums"), Bytes.toBytes(ai.get()));

		context.write(NullWritable.get(), put);
	}
}
