package tk.tak.hadoop.myhadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SetFile;
import org.apache.hadoop.io.SetFile.Writer;
import org.apache.hadoop.io.Text;
import org.junit.Test;

/**
 * @author Tak
 * 测试SequenceFile
 */
public class TestSetFile {

	/**
	 * 写入MapFile变形 --> ArrayFile
	 * SequenceFile key-->value 类似于Map
	 *
	 * @throws Throwable
	 */
	@Test
	public void write() throws Throwable {

		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://s100:8020");
		FileSystem fs = FileSystem.get(conf);
		Writer writer = new SetFile.Writer(conf, fs,
				"hdfs://s100:8020/user/setfile.set", IntWritable.class, CompressionType.NONE);


		// 写入数据
		for (int i = 100; i < 200; i++) {
			writer.append(new IntWritable(i));
		}

		writer.close();
		System.out.println("done");
	}

	/**
	 * 写入MapFile变形 --> ArrayFile
	 * SequenceFile key-->value 类似于Map
	 *
	 * @throws Throwable
	 */
	@Test
	public void read() throws Throwable {

		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://s100:8020");
		FileSystem fs = FileSystem.get(conf);
		SetFile.Reader reader = new SetFile.Reader(fs,
				"hdfs://s100:8020/user/setfile.set", conf);

		Text value = new Text();

		// 读数据
		while (reader.next(value)) {
			System.out.println("val:" + value);
		}
		reader.close();

		System.out.println("done");
	}


}
