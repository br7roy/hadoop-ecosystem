/*
 * Package:  com.rust.hbase.mr
 * FileName: WordCountMapper
 * Author:   Rust
 * Date:     19/3/31 17:56
 * email:    bryroy@gmail.com
 */
package com.rust.hbase.mr;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * HBaseMapper
 *
 * @author Rust
 */
public class WordCountMapper extends TableMapper<Text, IntWritable> {
	@Override
	protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
		String name = Bytes.toString(value.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("name")));
		context.write(new Text(name), new IntWritable(1));
	}
}
