package com.rust.hadoop.myhadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DeflateCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

/**
 * @author Rust 测试SequenceFile
 */
public class TestSequenceFile {

	/**
	 * 写入seqFile
	 *
	 * @throws Throwable
	 */
	@SuppressWarnings("deprecation")
	@Test
	public void write() throws Throwable {

		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://s100:8020");
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path("hdfs://s100:8020/user/myseqfile.seq");
		SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, path,
				IntWritable.class, Text.class);
		IntWritable key = new IntWritable();
		Text value = new Text();
		for (int i = 0; i < 100; i++) {
			key.set(i);
			value.set("tom" + i);
			writer.append(key, value);
		}
		/*
		 * IntWritable key = new IntWritable(1); Text value = new Text("tom");
		 * writer.append(key, value); writer.append(key, new Text("tom1"));
		 * writer.append(key, new Text("tom2")); writer.append(key, new Text("tom3"));
		 * writer.append(key, new Text("tom4")); writer.append(key, new Text("tom5"));
		 */
		writer.close();
		System.out.println("done");
	}

	/**
	 * 读取seqfile
	 *
	 * @throws Throwable
	 */
	@Test
	public void readSeq() throws Throwable {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://s100:8020");
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path("hdfs://s100:8020/user/myseqfile.seq");
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
		// SequenceFile.Reader reader = new SequenceFile.Reader( conf,new
		// Option("rw","read"));

		IntWritable key = new IntWritable();
		Text value = new Text();

		while (reader.next(key, value)) {
			System.out.println(key + "=" + value);
		}
		reader.close();
		System.out.println("done");

	}

	/**
	 * 读取SeqFile文件位置
	 *
	 * @throws Throwable
	 */
	@Test
	public void getSeqFile() throws Throwable {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://s100:8020");
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path("hdfs://s100:8020/user/myseqfile.seq");
		Reader reader = new SequenceFile.Reader(fs, path, conf);
		// 接受key值
		IntWritable key = new IntWritable();
		// 接受value值
		Text value = new Text();

		// key 类型
		Class keyClz = reader.getKeyClass();
		// value 类型
		Class valueClz = reader.getValueClass();

		// 得到压缩类型
		CompressionType cType = reader.getCompressionType();
		CompressionCodec cCode = reader.getCompressionCodec();

		// 得到当前key对应的字节数（位置）
		long position = reader.getPosition();
		System.out.println(position);

		// reader.getCurrentValue(key);
		while (reader.next(key, value)) {
			System.out.println(position + "，" + key + "=" + value);
			position = reader.getPosition();
		}
		reader.close();
		System.out.println("done");

	}

	/**
	 * 读取SeqFile文件位置
	 *
	 * @throws Throwable
	 */
	@Test
	public void seekSeq() throws Throwable {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://s100:8020");
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path("hdfs://s100:8020/user/myseqfile.seq");
		Reader reader = new SequenceFile.Reader(fs, path, conf);
		// 接受key值
		IntWritable key = new IntWritable();
		// 接受value值
		Text value = new Text();

		// 定位指针到指定位置
		reader.seek(128);
		reader.next(key, value);
		System.out.println(key + ":" + value);
		reader.close();

	}

	/**
	 * 读取seqFile同步点
	 *
	 * @throws Exception
	 */
	@Test
	public void sync() throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://s100:8020");
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path("hdfs://s100:8020/user/myseqfile.seq");
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
		IntWritable key = new IntWritable();
		Text value = new Text();

		int syncPos = 595;

		// 定位到下一个同步点
		reader.sync(syncPos);
		// 得到当前指针位置
		long pos = reader.getPosition();

		reader.next(key, value);
		System.out.println(syncPos + " : " + pos + " : " + key + "=" + value);
		reader.close();
		System.out.println("done");

	}

	/**
	 * 写入seqFile,手动指定同步点
	 *
	 * @throws Throwable
	 */
	@SuppressWarnings("deprecation")
	@Test
	public void writeWithSync() throws Throwable {

		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://s100:8020");
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path("hdfs://s100:8020/user/myseqfile.seq");
		SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, path,
				IntWritable.class, Text.class);
		IntWritable key = new IntWritable();
		Text value = new Text();
		for (int i = 0; i < 100; i++) {
			key.set(i);
			value.set("tom" + i);
			writer.append(key, value);
			if (i % 5 == 0) {
				// 创建同步点
				writer.sync();
			}
		}
		/*
		 * IntWritable key = new IntWritable(1); Text value = new Text("tom");
		 * writer.append(key, value); writer.append(key, new Text("tom1"));
		 * writer.append(key, new Text("tom2")); writer.append(key, new Text("tom3"));
		 * writer.append(key, new Text("tom4")); writer.append(key, new Text("tom5"));
		 */
		writer.close();
		System.out.println("done");
	}

	/**
	 * 不使用压缩写入seqFile SequenceFile key-value 类似于map
	 *
	 * @throws Throwable
	 */
	@SuppressWarnings("deprecation")
	@Test
	public void writeNoCompress() throws Throwable {

		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://s100:8020");
		FileSystem fs = FileSystem.get(conf);

		Path path = new Path("hdfs://s100:8020/user/myseq2.seq");
		/*
		 * SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, path,
		 * IntWritable.class, Text.class, CompressionType.NONE,new DeflateCodec());
		 */
		SequenceFile.Writer writer = SequenceFile.createWriter(conf, fs.create(path),
				IntWritable.class, Text.class, CompressionType.NONE, null);

		IntWritable key = new IntWritable();
		Text value = new Text();
		for (int i = 0; i < 100; i++) {
			key.set(i);
			value.set("tom" + i);
			writer.append(key, value);
		}
		/*
		 * IntWritable key = new IntWritable(1); Text value = new Text("tom");
		 * writer.append(key, value); writer.append(key, new Text("tom1"));
		 * writer.append(key, new Text("tom2")); writer.append(key, new Text("tom3"));
		 * writer.append(key, new Text("tom4")); writer.append(key, new Text("tom5"));
		 */
		writer.close();
		System.out.println("done");
	}

	/**
	 * 使用块压缩写入seqFile SequenceFile key-value 类似于map
	 * 编解码格式使用Deflate
	 *
	 * @throws Throwable
	 */
	@SuppressWarnings("deprecation")
	@Test
	public void writeInBlockCompressDeflateCodec() throws Throwable {

		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://s100:8020");
		FileSystem fs = FileSystem.get(conf);

		Path path = new Path("hdfs://s100:8020/user/myseq3.seq");

		CompressionCodec codec = ReflectionUtils.newInstance(DeflateCodec.class, conf);

		SequenceFile.Writer writer = SequenceFile.createWriter(conf, fs.create(path),
				IntWritable.class, Text.class, CompressionType.BLOCK, codec);

		IntWritable key = new IntWritable();
		Text value = new Text();
		for (int i = 0; i < 100; i++) {
			key.set(i);
			value.set("tom" + i);
			writer.append(key, value);
		}
		/*
		 * IntWritable key = new IntWritable(1); Text value = new Text("tom");
		 * writer.append(key, value); writer.append(key, new Text("tom1"));
		 * writer.append(key, new Text("tom2")); writer.append(key, new Text("tom3"));
		 * writer.append(key, new Text("tom4")); writer.append(key, new Text("tom5"));
		 */
		writer.close();
		System.out.println("done");
	}

	/**
	 * 使用块压缩写入seqFile SequenceFile key-value 类似于map
	 * 编解码格式使用Deflate
	 *
	 * @throws Throwable
	 */
	@SuppressWarnings("deprecation")
	@Test
	public void writeInBlockCompressSnappyCodec() throws Throwable {

		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://s100:8020");
		FileSystem fs = FileSystem.get(conf);

		Path path = new Path("hdfs://s100:8020/user/myseq3.seq");

		CompressionCodec codec = ReflectionUtils.newInstance(SnappyCodec.class, conf);

		SequenceFile.Writer writer = SequenceFile.createWriter(conf, fs.create(path),
				IntWritable.class, Text.class, CompressionType.BLOCK, codec);

		IntWritable key = new IntWritable();
		Text value = new Text();
		for (int i = 0; i < 100; i++) {
			key.set(i);
			value.set("tom" + i);
			writer.append(key, value);
		}
		/*
		 * IntWritable key = new IntWritable(1); Text value = new Text("tom");
		 * writer.append(key, value); writer.append(key, new Text("tom1"));
		 * writer.append(key, new Text("tom2")); writer.append(key, new Text("tom3"));
		 * writer.append(key, new Text("tom4")); writer.append(key, new Text("tom5"));
		 */
		writer.close();
		System.out.println("done");
	}

	/**
	 * 读取seqfile
	 *
	 * @throws Throwable
	 */
	@Test
	public void readSnappyBlockSeq() throws Throwable {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://s100:8020");
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path("hdfs://s100:8020/user/myseq3.seq");
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
		// SequenceFile.Reader reader = new SequenceFile.Reader( conf,new
		// Option("rw","read"));

		IntWritable key = new IntWritable();
		Text value = new Text();

		while (reader.next(key, value)) {
			System.out.println(key + "=" + value);
		}
		reader.close();
		System.out.println("done");

	}

}
