 /*
  * Package tk.tak
  * FileName: PerformanceComparator
  * Author:   Tak
  * Date:     2018/11/11 15:39
  */
 package tk.tak;

 import avro.shaded.com.google.common.base.Stopwatch;
 import tk.tak.myavro.model.MyUser;
 import tk.tak.myavro.model.User;
 import tk.tak.myavro.model.UserProtos;
 import org.apache.avro.file.DataFileWriter;
 import org.apache.avro.io.DatumWriter;
 import org.apache.avro.specific.SpecificDatumWriter;

 import java.io.DataOutputStream;
 import java.io.FileOutputStream;
 import java.io.IOException;
 import java.io.ObjectOutputStream;

 /**
  * 性能评测
  *
  * @author Tak
  */
 public class PerformanceComparator {
	 private int max = 100000;

	 public static void main(String[] args) throws Exception {
		 PerformanceComparator performanceComparator = new PerformanceComparator();
		 performanceComparator.javaSerial();
		 performanceComparator.writableSerial();
		 performanceComparator.avroSerial();
		 performanceComparator.protobuf();
	 }

	 /**
	  * Java串行化
	  *
	  * @throws IOException
	  */
	 private void javaSerial() throws IOException {
		 Stopwatch stopwatch = new Stopwatch();
		 stopwatch.start();
		 try (
				 FileOutputStream fos = new FileOutputStream("users.java");
				 // ByteArrayOutputStream bos = new ByteArrayOutputStream();
				 ObjectOutputStream oos = new ObjectOutputStream(fos)) {
			 for (int i = 0; i < max; i++) {
				 MyUser user = new MyUser();
				 user.setName("tom" + i);
				 user.setFavorite_number(i);
				 user.setFavorite_color("red" + i);
				 oos.writeObject(user);
			 }
			 stopwatch.stop();
			 System.out.println("javaSerial,elapse:" + stopwatch + ",size:"
					 // + bos.toByteArray().length
			 );
		 }
	 }

	 /**
	  * Writable串行化
	  *
	  * @throws Exception
	  */
	 private void writableSerial() throws Exception {
		 // IOUtils.writeFully();
		 Stopwatch stopwatch = new Stopwatch();
		 stopwatch.start();
		 try (FileOutputStream fos = new FileOutputStream("users.hadoop");
			  // ByteArrayOutputStream bos = new ByteArrayOutputStream();
			  DataOutputStream dos = new DataOutputStream(fos)) {
			 for (int i = 0; i < max; i++) {
				 MyUser user = new MyUser();
				 user.setFavorite_number(i);
				 user.setFavorite_color("red" + i);
				 user.setName("tom" + i);
				 user.write(dos);
			 }
			 stopwatch.stop();
			 System.out.println("writableSerial,elapse:" + stopwatch + ",size:"
					 // + bos.toByteArray().length
			 );
		 }

	 }

	 /**
	  * avro串行
	  *
	  * @throws Exception
	  */
	 private void avroSerial() throws Exception {
		 Stopwatch stopwatch = new Stopwatch();
		 stopwatch.start();
		 try (
				 FileOutputStream fos = new FileOutputStream("users.avro_serialize");
				 // ByteArrayOutputStream bos = new ByteArrayOutputStream()
		 ) {
			 DatumWriter<User> datumWriter = new SpecificDatumWriter<>(User.class);
			 DataFileWriter<User> dataFileWriter = new DataFileWriter<>(datumWriter);
			 User user = new User();
			 // avro 数据文件
			 dataFileWriter.create(user.getSchema(), fos);
			 for (int i = 0; i < max; i++) {
				 User user1 = new User();
				 user1.setFavoriteNumber(i);
				 user1.setFavoriteColor("red" + i);
				 user1.setName("tom" + i);
				 dataFileWriter.append(user1);
			 }
			 dataFileWriter.close();
			 stopwatch.stop();
			 System.out.println("avroSerial,elapse:" + stopwatch + ",size:"
					 // + bos.toByteArray().length
			 );
		 }
	 }

	 /**
	  * ProtoBuf串行化
	  *
	  * @throws Exception
	  */
	 public void protobuf() throws Exception {
		 Stopwatch stopwatch = new Stopwatch();
		 stopwatch.start();
		 FileOutputStream fos = new FileOutputStream("users.protobuf_serialize");
		 for (int i = 0; i < max; i++) {
			 UserProtos.User user = UserProtos.User.newBuilder()
					 .setName("tom" + i)
					 .setFavoriteNumber(i)
					 .setFavoriteColor("red" + i)
					 .build();
			 user.writeDelimitedTo(fos);
		 }
		 fos.close();
		 stopwatch.stop();
		 System.out.println("ProtoBufSerial,elapse:" + stopwatch
				 // + ",size:"+ bos.toByteArray().length
		 );
	 }

 }
