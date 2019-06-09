package com.rust.hadoop.myhadoop.serialize;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;

public class TestText {

	/**
	 * @throws Exception
	 */
	@SuppressWarnings("static-access")
	@Test
	public void test1() throws Exception {
		Text txt = new Text("h中ello world!");
		int v = txt.charAt(1);
		System.out.println(v);
		int p = txt.find("e", 1);
		System.out.println(p);
		System.out.println(txt.getLength());
		System.out.println(txt.decode(txt.getBytes(), 0, txt.getLength()));
	}

	/**
	 * 串行和反串行
	 *
	 * @throws Throwable
	 */
	@Test
	public void serialAndDeserial() throws Throwable {
		FileOutputStream out = new FileOutputStream("d:/data.data");
		DataOutputStream dos = new DataOutputStream(out);
		// 开始串行化
		long start = System.nanoTime();
		IntWritable iw = new IntWritable(100); // 4
		iw.write(dos);
		LongWritable lw = new LongWritable(200); // 8
		lw.write(dos);

		Text text = new Text("hello"); // 5
		text.write(dos);

		text.set("world"); // 5
		text.write(dos);

		iw.set(-10); // 4
		iw.write(dos);

		dos.close();
		out.close();

		System.out.println("elapse:" + (System.nanoTime() - start));
		// 读取文件
		FileInputStream fis = new FileInputStream("d:/data.data");
		byte[] bs = new byte[fis.available()];
		fis.read(bs);
		fis.close();

		// 反串行
		DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bs));
		iw.readFields(dis);
		System.out.println(iw.get());

		lw.readFields(dis);
		System.out.println(lw.get());

		text.readFields(dis);
		System.out.println(text.toString());

		text.readFields(dis);
		System.out.println(text.toString());

		iw.readFields(dis);
		System.out.println(iw.get());

	}

	@Test
	public void javaSerialize() {

		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(baos);
			oos.writeObject(new Integer(100));
			oos.writeObject(new Long(200));
			oos.writeObject(new Long(200));
			oos.writeObject(new String("hello"));
			oos.writeObject(new String("world"));
			oos.writeObject(new Integer(-10));
			oos.close();
			System.out.println(baos.toByteArray().length);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(e.getMessage());
		}
		System.out.println("done");


	}

	@Test
	public void javaSerialize2() {
		long start = System.nanoTime();
		try {
			FileOutputStream fis = new FileOutputStream("d:/java.data");
			ObjectOutputStream oos = new ObjectOutputStream(fis);
			oos.writeObject(new Integer(100));
			oos.writeObject(new Long(200));
			oos.writeObject(new Long(200));
			oos.writeObject(new String("hello"));
			oos.writeObject(new String("world"));
			oos.writeObject(new Integer(-10));
			oos.close();
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(e.getMessage());
		}
		System.out.println("elapse:" + (System.nanoTime() - start));
		System.out.println("done");
	}

}
