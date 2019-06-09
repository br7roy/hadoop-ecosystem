package com.rust;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

import java.io.File;

/**
 * 不生成源代码，直接通过schema进行串行
 */
public class Serialize2WithoutCode{
	public static void main(String[] args) throws Exception{
		// 加载schema文件
		Schema schema = new Schema.Parser().parse(new File("user.avsc"));

		// 常规record类
		// 通过解析avro schema文件创建对象
		GenericRecord user1 = new Record(schema);
		user1.put("name", "Alyssa");
		user1.put("favorite_number", 256);

		GenericRecord user2 = new Record(schema);
		user2.put("name", "Ben");
		user2.put("favorite_number", 7);
		user2.put("favorite_color", "red");

		// Serialize user1 and user2 to disk
		File file = new File("users2.avro");
		DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
		DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
		dataFileWriter.create(schema, file);
		dataFileWriter.append(user1);
		dataFileWriter.append(user2);
		dataFileWriter.close();


		System.out.println("over");
	}
}
