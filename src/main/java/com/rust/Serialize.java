package com.rust;

import com.rust.myavro.model.User;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;

/**
 * 串行
 * @author Rust
 */
public class Serialize {
	public static void main(String[] args) throws Exception{
		User user =
				User.newBuilder().setFavoriteColor("black").setFavoriteNumber(7).setName("zhangsan").build();
		User user2 = User.newBuilder()
				.setName("tom2")
				.setFavoriteNumber(77)
				.setFavoriteColor("black")
				.build();


		// 通过Writer写入文件到磁盘
		DatumWriter<User> userDatumWriter = new SpecificDatumWriter<User>(User.class);
		DataFileWriter<User> dataFileWriter = new DataFileWriter<User>(userDatumWriter);

		// avro数据文件
		dataFileWriter.create(user.getSchema(), new File("users.avro"));
		dataFileWriter.append(user);
		dataFileWriter.append(user2);
		dataFileWriter.close();
		System.out.println("over");


	}
}
