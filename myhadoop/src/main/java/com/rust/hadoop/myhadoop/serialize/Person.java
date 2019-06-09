package com.rust.hadoop.myhadoop.serialize;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Person implements Writable {
	private int id;
	private String name;
	private int age;
	private Address address;

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getAge() {
		return age;
	}

	public void setAge(int age) {
		this.age = age;
	}

	public Address getAddress() {
		return address;
	}

	public void setAddress(Address address) {
		this.address = address;
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(this.getId());
		out.writeUTF(this.getName());
		out.writeInt(this.getAge());
		address.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		this.setId(in.readInt());
		this.setName(in.readUTF());
		this.setAge(in.readInt());
		address.readFields(in);
	}

	@Override
	public String toString() {
		return "Person [id=" + id + ", name=" + name + ", age=" + age + ", address="
				+ address + "]";
	}

}
