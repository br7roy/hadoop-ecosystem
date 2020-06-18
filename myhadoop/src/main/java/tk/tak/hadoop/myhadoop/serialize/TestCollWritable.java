package tk.tak.hadoop.myhadoop.serialize;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.Test;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * @author Tak 测试集合串行
 */
public class TestCollWritable {

	@Test
	public void testArrayWritable() throws Throwable, IOException {
		// 创建writable数组对象
		Writable[] writables = new Writable[10];
		for (int i = 0, len = writables.length; i < len; i++) {
			writables[i] = new Text("tom" + i);
		}
		// 构造成ArrayWritable
		ArrayWritable arr = new ArrayWritable(Text.class);
		arr.set(writables);

		arr.write(new DataOutputStream(new FileOutputStream(new File("d:/array.aray"))));
		System.out.println("done");
	}

	@Test
	public void mapWritable() throws Throwable, IOException {
		MapWritable mapWritable = new MapWritable();
		for (int i = 0; i < 10; i++) {

			mapWritable.put(new IntWritable(i), new Text("tom" + i));
		}
		mapWritable.write(new DataOutputStream(new FileOutputStream(new File("d:/mapw.mapw"))));


	}

}
