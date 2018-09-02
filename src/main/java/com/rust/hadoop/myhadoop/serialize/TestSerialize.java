package com.rust.hadoop.myhadoop.serialize;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.junit.Test;

public class TestSerialize {

	/**
	 * 串行化
	 * 
	 * @throws Throwable
	 */
	@Test
	public void serialize() throws Throwable {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(bos);
		oos.writeObject(new Dog("小白"));
		oos.close();
		bos.close();
		System.out.println(bos.toByteArray().length);
	}

	/**
	 * 自定义串行化
	 * 
	 * @throws Throwable
	 */
	// @Test
	public byte[] customSerialize() throws Throwable {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(bos);
		Person person = new Person();
		person.setId(19);
		person.setName("tom");
		person.setAge(20);

		Address address = new Address();
		address.setPrivince("上海");
		address.setCity("上海");
		address.setStreet("嘉定");
		person.setAddress(address);
		person.write(oos);
		oos.close();
		bos.close();
		System.out.println("done");
		return bos.toByteArray();
	}

	@Test
	public void customDeSerialize() throws Throwable {
		byte[] bs = customSerialize();
		ByteArrayInputStream bais = new ByteArrayInputStream(bs);
		ObjectInputStream ois = new ObjectInputStream(bais);
		Person person = new Person();
		person.readFields(ois);
		System.out.println(person);
		

	}
	

}
