 /*
  * Package com.rust.hadoop.myhadoop.inputformat.db
  * FileName: DBMapper
  * Author:   Rust
  * Date:     2018/10/4 20:14
  */
 package com.rust.hadoop.myhadoop.inputformat.db;

 import org.apache.hadoop.io.LongWritable;
 import org.apache.hadoop.io.Text;
 import org.apache.hadoop.mapreduce.Mapper;

 import java.io.IOException;

 /**
  * FileName:    DBMapper
  * Author:      Rust
  * Date:        2018/10/4
  * Description:
  */
 public class DBMapper extends Mapper<LongWritable, UserDBWritable, Text, Text> {


	 @Override
	 protected void map(LongWritable key, UserDBWritable value, Context context) throws IOException,
			 InterruptedException {
		 int id = value.getId();
		 String name = value.getName();
		 int age = value.getAge();
		 context.write(new Text("" + id), new Text(name + "," + age));

	 }
 }
