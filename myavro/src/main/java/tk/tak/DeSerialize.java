 /*
  * Package tk.tak
  * FileName: DeSerialize
  * Author:   Tak
  * Date:     2018/11/10 11:15
  */
 package tk.tak;

 import tk.tak.myavro.model.User;
 import org.apache.avro.file.DataFileReader;
 import org.apache.avro.io.DatumReader;
 import org.apache.avro.specific.SpecificDatumReader;

 import java.io.IOException;
 import java.nio.file.Paths;

 /**
  * 反串行
  * @author Tak
  */
 public class DeSerialize {
	 public static void main(String[] args) throws IOException {
		 // Deserialize Users from disk
		 DatumReader<User> userDatumReader = new SpecificDatumReader<User>(User.class);
		 DataFileReader<User> dataFileReader = new DataFileReader<User>(Paths.get("users.avro").toFile(), userDatumReader);

		 User user = null;
		 while (dataFileReader.hasNext()) {
			 // Reuse user object by passing it to next(). This saves us from
			 // allocating and garbage collecting many objects for files with
			 // many items.
			 user = dataFileReader.next(user);
			 System.out.println(user.getName() + "," + user.getFavoriteNumber());
		 }
	 }
 }
