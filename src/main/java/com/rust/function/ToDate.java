 /*
  * Package com.rust.function
  * FileName: ToDate
  * Author:   Takho
  * Date:     18/12/9 21:53
  */
 package com.rust.function;

 import org.apache.hadoop.hive.ql.exec.Description;
 import org.apache.hadoop.hive.ql.exec.UDF;

 import java.text.ParseException;
 import java.text.SimpleDateFormat;
 import java.util.Date;

 /**
  * 将字符串转换成日期
  *
  * @author Takho
  */
 @Description(name = "to_date", value = "this is my first udf!", extended = "e.g select to_date('2018-12-10 12:12:12')")
 public class ToDate extends UDF {

	 public Date evaluate(String date) {
		 SimpleDateFormat sdf = new SimpleDateFormat();
		 sdf.applyPattern("yyyy/MM/dd HH:mm:ss");
		 try {
			 return sdf.parse(date);
		 } catch (ParseException e) {
			 e.printStackTrace();
		 }
		 return new Date();
	 }
 }
