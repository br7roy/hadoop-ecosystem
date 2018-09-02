package com.rust.hadoop.myhadoop.serialize;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.ObjectOutputStream;

import org.apache.hadoop.io.IntWritable;
import org.hamcrest.core.Is;
import org.junit.Assert;
import org.junit.Test;

public class TestIntWritable {

	/**
	 * 串行化
	 * 
	 * @throws Throwable
	 */
	@Test
	public void serialize() throws Throwable {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(bos);
		IntWritable i = new IntWritable(255);
		i.write(oos);
		oos.close();
		bos.close();
		byte[] bs = bos.toByteArray();
		System.out.println(bs.length);
		Assert.assertThat(bs.length,Is.is(10));
	}
	
	/**
	 * 串行化
	 * 
	 * @throws Throwable
	 */
	@Test
	public void serialize2() throws Throwable {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(bos);
		IntWritable i = new IntWritable(255);
		i.write(dos);
		bos.close();
		dos.close();
		byte[] bs = bos.toByteArray();
		System.out.println(bs.length);
		Assert.assertThat(bs.length,Is.is(4));
		
		// 反序列化
		IntWritable i2 = new IntWritable();
		DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bos.toByteArray()));
		
		i2.readFields(dis);
		
		System.out.println(i2.get());
		
	}
	
	/**
	 * 比较大小
	 */
	@Test
	public void compare() {
		IntWritable i1 = new IntWritable(1);
		IntWritable i2 = new IntWritable(2);
		
		System.out.println(i1.compareTo(i2));
	}
	
	@Test
	public void compare2() {
		IntWritable.Comparator comparator = new IntWritable.Comparator();
		IntWritable i1 = new IntWritable(1);
		IntWritable i2 = new IntWritable(2);
		
		System.out.println(comparator.compare(i1, i2));
		
	}
	
	@Test
	public void booleanWritable() throws Exception {
//		BooleanWritable bw = 
		String s = "你好";
		
		byte[] bs=s.getBytes("UTF-8");
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
}
