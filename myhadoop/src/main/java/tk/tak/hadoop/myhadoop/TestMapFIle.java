package tk.tak.hadoop.myhadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.MapFile.Reader;
import org.apache.hadoop.io.MapFile.Writer;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

/**
 * 映射文件
 *
 * @author Tak
 */
public class TestMapFIle {

	/**
	 * 写入MapFile
	 *
	 * @throws Throwable
	 */
	@SuppressWarnings("deprecation")
	@Test
	public void write() throws Throwable {

		Configuration conf = new Configuration();
		// conf.set("fs.defaultFS", "hdfs://s100:8020");
		conf.set("fs.defaultFS", "file:///d:/");// 写入本地
		FileSystem fs = FileSystem.get(conf);
		// Path path = new Path("hdfs://s100:8020/user/mymap.map");
		Path path = new Path("file:///d:/mymap.map");
		// MapFile.Writer mf = new Writer(conf, path,opts);
		MapFile.Writer mf = new Writer(conf, fs, "file:///d:/mymap.map",
				IntWritable.class, Text.class);
		mf.append(new IntWritable(1), new Text("tom1"));
		mf.append(new IntWritable(2), new Text("tom2"));
		mf.append(new IntWritable(3), new Text("tom3"));
		mf.append(new IntWritable(4), new Text("tom4"));
		mf.append(new IntWritable(5), new Text("tom5"));
		mf.close();
	}

	/**
	 * 读取MapFile
	 *
	 * @throws Throwable
	 */
	@SuppressWarnings("deprecation")
	@Test
	public void read() throws Throwable {

		Configuration conf = new Configuration();
		// conf.set("fs.defaultFS", "hdfs://s100:8020");
		conf.set("fs.defaultFS", "file:///d:/");// 写入本地
		FileSystem fs = FileSystem.get(conf);
		// Path path = new Path("hdfs://s100:8020/user/mymap.map");
		Path path = new Path("file:///d:/mymap.map");
		// MapFile.Writer mf = new Writer(conf, path,opts);
		MapFile.Reader reader = new MapFile.Reader(fs, "file:///d:/mymap.map", conf);
		IntWritable key = new IntWritable();
		Text val = new Text();
		while (reader.next(key, val)) {
			System.out.println("key:" + key + " = value:" + val);
		}
		val = (Text) reader.get(new IntWritable(7), val);
		System.out.println(val);
		reader.close();
	}

	/**
	 * 写入MapFile
	 *
	 * @throws Throwable
	 */
	@SuppressWarnings("deprecation")
	@Test
	public void write2() throws Throwable {

		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://s100:8020");
		// conf.set("io.map.index.interval", "20");
		// conf.set("fs.defaultFS", "file:///d:/");// 写入本地
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path("hdfs://s100:8020/user/mymap.map");
		// Path path = new Path("file:///d:/mymap.map");
		DefaultCodec defaultCodec = ReflectionUtils.newInstance(DefaultCodec.class, conf);
		MapFile.Writer mWriter = new Writer(conf, path, Writer.compression(CompressionType.BLOCK, defaultCodec),
				Writer.keyClass(IntWritable.class), Writer.valueClass(Text.class));
		// 通过修改配置参数，修改索引间隔
/*		MapFile.Writer mWriter = new Writer(conf, fs,
				// "file:///d:/mymap.map",
				"hdfs://s100:8020/user/mymap.map", IntWritable.class, Text.class);*/
		int indexInterval = mWriter.getIndexInterval();
		System.out.println(indexInterval);
		// 修改索引间隔
		// mWriter.setIndexInterval(10);

		for (int i = 1; i < 500; i = i + 3) {
			mWriter.append(new IntWritable(i), new Text("tom" + i));
		}
		mWriter.close();
		System.out.println("done");
	}

	/**
	 * 读取MapFile
	 *
	 * @throws Throwable
	 */
	@SuppressWarnings("deprecation")
	@Test
	public void seek() throws Throwable {

		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://s100:8020");
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path("hdfs://s100:8020/user/mymap.map");
		MapFile.Reader reader = new Reader(fs, "hdfs://s100:8020/user/mymap.map", conf);
		IntWritable key = new IntWritable();
		Text val = new Text();
		reader.seek(new IntWritable(108));
		reader.next(key, val);
		System.out.println(key + ":" + val);
		reader.close();
		System.out.println("done");
	}

	/**
	 * 读取最近记录
	 *
	 * @throws Throwable
	 */
	@SuppressWarnings("deprecation")
	@Test
	public void getClosest() throws Throwable {

		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://s100:8020");
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path("hdfs://s100:8020/user/mymap.map");
		MapFile.Reader reader = new Reader(fs, "hdfs://s100:8020/user/mymap.map", conf);
		IntWritable key = (IntWritable) reader.getClosest(new IntWritable(8), new Text(), true);
		System.out.println(key);

		Text tmpVal = new Text();
		Text value = (Text) reader.get(key, tmpVal);
		System.out.println(value);
		reader.close();
		System.out.println("done");
	}

	/**
	 * 准备sequenceFile
	 *
	 * @throws Throwable
	 * @throws IllegalArgumentException
	 */
	@SuppressWarnings("deprecation")
	@Test
	public void prepareSeqFile() throws IllegalArgumentException, Throwable {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://s100:8020");
		FileSystem fs = FileSystem.get(conf);
		SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf,
				new Path("hdfs://s100:8020/user/seq/seq.seq"), IntWritable.class,
				Text.class);
		for (int i = 1; i <= 100; i++) {
			writer.append(new IntWritable(i), new Text("tom" + i));
		}
		writer.close();
		System.out.println("done");

	}

	/**
	 * 修复seqFile文件  重建index, 生成mapFile
	 *
	 * @throws Throwable
	 * @throws IllegalArgumentException
	 */
	@Test
	public void fixSeqFile() throws IllegalArgumentException, Throwable {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://s100:8020");
		FileSystem fs = FileSystem.get(conf);
		MapFile.fix(fs, new Path("hdfs://s100:8020/user/seq"), IntWritable.class,
				Text.class, false, conf);

		System.out.println("done");
	}


}
