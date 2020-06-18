package tk.tak.hadoop.myhadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.ArrayFile;
import org.apache.hadoop.io.ArrayFile.Writer;
import org.apache.hadoop.io.Text;
import org.junit.Test;

/**
 * @author Tak
 * 测试SequenceFile
 */
public class TestArrayFile {

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
		Writer writer = new ArrayFile.Writer(conf, fs,
				"hdfs://s100:8020/user/arrayfile.array", Text.class);

		// 写入数据
		for (int i = 0; i < 100; i++) {
			writer.append(new Text("tom" + i));
		}

		writer.close();
		System.out.println("done");
	}

	/**
	 * @throws Throwable
	 */
	@Test
	public void read() throws Throwable {

		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://s100:8020");
		FileSystem fs = FileSystem.get(conf);
		ArrayFile.Reader reader = new ArrayFile.Reader(fs,
				"hdfs://s100:8020/user/arrayfile.array", conf);

		Text value = new Text();

		// 读数据
		while (reader.next(value) != null) {
			System.out.println("val:" + value);
		}
		reader.close();

		System.out.println("done");
	}


}
