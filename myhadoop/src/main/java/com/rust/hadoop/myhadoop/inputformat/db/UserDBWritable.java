 /*
  * Package com.rust.hadoop.myhadoop.inputformat.db
  * FileName: UserDBWritable
  * Author:   Rust
  * Date:     2018/10/4 9:38
  */
 package com.rust.hadoop.myhadoop.inputformat.db;

 import org.apache.hadoop.io.Writable;
 import org.apache.hadoop.mapreduce.lib.db.DBWritable;

 import java.io.DataInput;
 import java.io.DataOutput;
 import java.io.IOException;
 import java.sql.PreparedStatement;
 import java.sql.ResultSet;
 import java.sql.SQLException;

 /**
  * FileName:    UserDBWritable
  * Author:      Rust
  * Date:        2018/10/4
  * Description:
  */
 public class UserDBWritable implements DBWritable, Writable {
	 private int id;
	 private String name;
	 private int age;

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

	 /**
	  * Hadoop串行化
	  *
	  * @param out
	  * @throws IOException
	  */
	 @Override
	 public void write(DataOutput out) throws IOException {
		 out.writeInt(id);
		 out.writeUTF(name);
		 out.writeInt(age);

	 }

	 /**
	  * Hadoop反串行化
	  *
	  * @param in
	  * @throws IOException
	  */
	 @Override
	 public void readFields(DataInput in) throws IOException {
		 this.id = in.readInt();
		 this.name = in.readUTF();
		 this.age = in.readInt();
	 }

	 @Override
	 public void write(PreparedStatement statement) throws SQLException {
		 // TODO: Rust 2018/10/4

	 }

	 @Override
	 public void readFields(ResultSet resultSet) throws SQLException {
		 this.id = resultSet.getInt("id");
		 this.name = resultSet.getString("name");
		 this.age = resultSet.getInt("age");
	 }
 }
