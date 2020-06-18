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
 import org.apache.avro.file.DataFileReader;
 import org.apache.avro.io.DatumReader;
 import org.apache.avro.specific.SpecificDatumReader;

 import java.io.DataInputStream;
 import java.io.File;
 import java.io.FileInputStream;
 import java.io.IOException;
 import java.io.ObjectInputStream;

 /**
  * 性能评测2 反串行
  *
  * @author Tak
  */
 public class PerformanceComparator2 {
	 private int max = 100000;

	 public static void main(String[] args) throws Exception {
		 PerformanceComparator2 performanceComparator = new PerformanceComparator2();
		 performanceComparator.javaSerial();
		 performanceComparator.writableSerial();
		 performanceComparator.avroSerial();
		 performanceComparator.protoBufDeSerial();
	 }

	 /**
	  * Java串行化
	  *
	  * @throws IOException
	  */
	 private void javaSerial() throws IOException, ClassNotFoundException {
		 Stopwatch stopwatch = new Stopwatch();
		 stopwatch.start();

		 try (
				 FileInputStream fos = new FileInputStream("users.java");
				 ObjectInputStream oos = new ObjectInputStream(fos)) {
			 MyUser user;
			 for (int i = 0; i < max; i++) {
				 user = (MyUser) oos.readObject();
			 }

			 stopwatch.stop();
			 System.out.println("javaSerial,elapse:" + stopwatch
			 );
		 }
	 }

	 /**
	  * Writable串行化
	  *
	  * @throws Exception
	  */
	 private void writableSerial() throws Exception {
		 Stopwatch stopwatch = new Stopwatch();
		 stopwatch.start();
		 try (FileInputStream fos = new FileInputStream("users.hadoop");
			  DataInputStream dos = new DataInputStream(fos)) {
			 MyUser user = new MyUser();
			 for (int i = 0; i < max; i++) {
				 user.readFields(dos);
			 }
			 stopwatch.stop();
			 System.out.println("writableSerial,elapse:" + stopwatch
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
		 DatumReader<User> datumReader = new SpecificDatumReader<>(User.class);
		 DataFileReader<User> dataFileReader = new DataFileReader<User>(new File("users.avro_serialize"), datumReader);

		 // avro 数据文件
		 User next;
		 while (dataFileReader.hasNext()) {
			 next = dataFileReader.next();
		 }
		 dataFileReader.close();
		 stopwatch.stop();
		 System.out.println("avroSerial,elapse:" + stopwatch
		 );
	 }

	 private void protoBufDeSerial() throws Exception {
		 Stopwatch stopwatch = new Stopwatch();
		 stopwatch.start();
		 FileInputStream fis = new FileInputStream("users.protobuf_serialize");

		 for (int i = 0; i < max; i++) {
			 UserProtos.User user = UserProtos.User.parseDelimitedFrom(fis);
		 }
		 stopwatch.stop();
		 System.out.println("protoBufDeserialize,elapse:" + stopwatch);
	 }

 }
