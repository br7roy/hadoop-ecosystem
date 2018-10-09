 /*
  * Package com.rust.hadoop.myhadoop.wordcount
  * FileName: WordCountMapper
  * Author:   Rust
  * Date:     2018/9/22 16:36
  */
 package com.rust.hadoop.myhadoop.work;


 import com.google.gson.Gson;
 import com.google.gson.GsonBuilder;
 import com.rust.hadoop.myhadoop.mr.Util;
 import com.rust.hadoop.myhadoop.work.entity.Big;
 import com.rust.hadoop.myhadoop.work.entity.City;
 import org.apache.hadoop.io.BytesWritable;
 import org.apache.hadoop.io.NullWritable;
 import org.apache.hadoop.io.Text;
 import org.apache.hadoop.mapreduce.Mapper;

 import java.io.IOException;
 import java.nio.charset.StandardCharsets;
 import java.nio.file.Path;
 import java.nio.file.Paths;
 import java.util.Objects;

 /**
  * FileName:    WordCountMapper
  * Author:      Rust
  * Date:        2018/9/22
  * Description:
  */
 public class WordCountMapper extends Mapper<NullWritable, BytesWritable, Text, Text> {
	 @Override
	 protected void setup(Context context) throws IOException, InterruptedException {
		 context.getCounter("m", Util.getGroup2("WordCountMapper.setup", this.hashCode()));

	 }

	 @Override
	 protected void map(NullWritable longWritable, BytesWritable value, Context context) throws IOException,
			 InterruptedException {
		 /////////////////////////////
		 byte[] data = value.copyBytes();
		 String arr = new String(data, StandardCharsets.UTF_8);
		 Gson gson = new GsonBuilder().create();
		 Big[] o = gson.fromJson(arr, Big[].class);
		 for (Big big : o) {
			 for (City city : big.getCity()) {
				 context.write(new Text(big.getCode()), new Text(city.getCode() + "," + city.getName()));
			 }
		 }

	 }

	 public static void main(String[] args) throws IOException {

		 String pathName = Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResource("city" +
				 ".json")).getPath().substring(1);

		 Path path = Paths.get(pathName);


		 /*try (InputStream inputStream = Files.newInputStream(path, StandardOpenOption.READ);) {
			 byte[] bs = new byte[20];
			 StringBuilder stringBuilder = new StringBuilder();
			 int len = -1;
			 while ((len = inputStream.read(bs)) != -1) {
				 stringBuilder.append(new String(bs, 0, len));
			 }
			 System.out.println(stringBuilder.toString());
		 }*/

/*		 try(BufferedReader bufferedReader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {

			 char[] chars = new char[1024];
			 StringBuilder stringBuilder = new StringBuilder();

			 int len = -1;

			 while ((len = bufferedReader.read(chars)) != -1) {
				 stringBuilder.append(chars, 0, len);
			 }
			 System.out.println(stringBuilder.toString());
		 }*/


/*		 try (JsonReader reader = new JsonReader(Files.newBufferedReader(path, StandardCharsets.UTF_8))) {

			 Gson gson = new GsonBuilder().create();

			 reader.beginArray();
			 while (reader.hasNext()) {
				 Big person = gson.fromJson(reader, Big.class);
				 System.out.println(person);
			 }
		 }*/


	 }
 }

