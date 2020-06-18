 /*
  * Package tk.tak
  * FileName: DeSerialize
  * Author:   Tak
  * Date:     2018/11/10 11:15
  */
 package tk.tak;

 import org.apache.avro.Schema;
 import org.apache.avro.file.DataFileReader;
 import org.apache.avro.generic.GenericDatumReader;
 import org.apache.avro.generic.GenericRecord;
 import org.apache.avro.io.DatumReader;

 import java.io.IOException;
 import java.nio.file.Path;
 import java.nio.file.Paths;

 /**
  * 反串行
  *
  * @author Tak
  */
 public class DeSerialize2WhithoutCode {
	 public static void main(String[] args) throws IOException {
		 // create schema file
		 Schema schema = new Schema.Parser().parse(Paths.get("user.avsc").toFile());
		 // 数据文件
		 Path path = Paths.get("users2.avro");
		 // Deserialize users from disk
		 DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
		 DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(path.toFile(), datumReader);
		 GenericRecord user = null;
		 while (dataFileReader.hasNext()) {
			 // Reuse user object by passing it to next(). This saves us from
			 // allocating and garbage collecting many objects for files with
			 // many items.
			 user = dataFileReader.next(user);
			 System.out.println(user.get("name") + "," + user.get("favorite_number"));

		 }
	 }
 }
