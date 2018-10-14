 /*
  * Package com.rust.hadoop.myhadoop.wordcount
  * FileName: WordCountMapper
  * Author:   Rust
  * Date:     2018/9/22 16:36
  */
 package com.rust.hadoop.myhadoop.work;


 import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;
import com.rust.hadoop.myhadoop.mr.Util;
import com.rust.hadoop.myhadoop.work.entity.Big;
import com.rust.hadoop.myhadoop.work.entity.City;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.sun.org.apache.xml.internal.serialize.LineSeparator.Windows;

 /**
  * FileName:    WordCountMapper
  * Author:      Rust
  * Date:        2018/9/22
  * Description:
  */
 public class WordCountMapper extends Mapper<LongWritable, Text, Text, Text> {
	 private static List<Big> bigList = null;

	 static {

		 try {
			 String pathName = Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResource(
					 "city" +
							 ".json")).getPath().substring(1);

			 Path path = Paths.get(pathName);
			 bigList = new ArrayList<>();
			 try (JsonReader reader = new JsonReader(Files.newBufferedReader(path, StandardCharsets.UTF_8))) {
				 Gson gson = new GsonBuilder().create();
				 reader.beginArray();
				 while (reader.hasNext()) {
					 Big bi = gson.fromJson(reader, Big.class);
					 bigList.add(bi);
				 }
			 }
		 } catch (IOException e) {
			 e.printStackTrace();
			 System.exit(-1);
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

	 @Override
	 protected void setup(Context context) throws IOException, InterruptedException {

		 context.getCounter("m", Util.getGroup2("WordCountMapper.setup", this.hashCode()));
	 }

	 @Override
	 protected void map(LongWritable longWritable, Text value, Context context) throws IOException,
			 InterruptedException {
		 /////////////////////////////
		 byte[] data = value.copyBytes();
		 String arr = new String(data, StandardCharsets.UTF_8);
		 String[] split = arr.replaceAll("\t", "").split(",");
		 String cityName = split[0];
		 String erjiName = split[1];
		 String erjiCode = split[2];
		 if ("东沙群岛".equals(cityName)) {
			 System.out.println("go");
		 }
		 boolean find = false;
		 for (Big big : bigList) {
			 for (City city : big.getCity()) {
				 String cityNameFromJson = city.getName();// 有市
				 if (cityNameFromJson.contains(cityName)) {
					 // 找到了对应的市名
					 // value: 2级名称，城市码，城市名称
					 context.write(new Text(erjiCode), new Text(erjiName + "," + city.getCode() + "," + cityName));
					 find = true;
				 }
			 }
		 }
		 // 没有找到,剔除erjiName=海南新疆
		 if (!find &&
				 !"海南".equals(erjiName) &&
				 !"新疆".equals(erjiName)) {
			 String cityCode = translateCityCodeExtenal(cityName);
			 context.write(new Text(erjiCode), new Text(erjiName + "," + cityCode + "," + cityName));
		 }


	 }

	 private String translateCityCodeExtenal(String cityName) {
		 String retCode = "";
		 if ("重庆郊县".equals(cityName)) {
			 retCode = "500200";
		 } else if ("济源市".equals(cityName)) {
			 retCode = "419001";
		 } else if ("东沙群岛".equals(cityName)) {
			 retCode = "442100";
		 }else {
			 retCode = "error";
		 }
		 return retCode;
	 }

	 @Test
	 public void test() throws IOException {

		 Path path = Paths.get("D:\\Users\\futanghang004\\Desktop", "true.sql");
		 final List<String> strings = Files.readAllLines(path, StandardCharsets.UTF_8);
		 Path writePath = Paths.get("D:\\Users\\futanghang004\\Desktop", "false.txt");
		 if (!Files.exists(writePath)) {
			 Files.createFile(writePath);
		 }
		 try (BufferedWriter writer = Files.newBufferedWriter(writePath, StandardOpenOption.WRITE)) {
			 strings.forEach(key -> {
				 if (key.contains("error")) {
					 int start = key.indexOf("(") + 1;
					 String ts = key.substring(start, key.lastIndexOf(")"));
					 //	 wrong in this line
					 try {
						 writer.write(ts);
						 writer.write(Windows);
						 writer.flush();
					 } catch (IOException e) {
						 e.printStackTrace();
					 }
				 }
			 });
		 }
		 System.out.println("done");

	 }
 }

