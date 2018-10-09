 /*
  * Package com.rust.hadoop.myhadoop.wordcount
  * FileName: WordCountReducer
  * Author:   Rust
  * Date:     2018/9/22 16:38
  */
 package com.rust.hadoop.myhadoop.work;

 import com.google.common.collect.Maps;
 import org.apache.hadoop.io.Text;
 import org.apache.hadoop.mapreduce.Reducer;
 import org.apache.hadoop.mapreduce.lib.db.DBInputFormat.NullDBWritable;

 import java.io.IOException;
 import java.sql.Timestamp;
 import java.util.Map;
 import java.util.UUID;

 /**
  * Author:      Rust
  * Date:        2018/10/10 1:42
  */
 public class WordCountReducer extends Reducer<Text, Text, AreaDBWritable, NullDBWritable> {

	 @Override
	 protected void reduce(Text key, Iterable<Text> values, Context context) {
		 NullDBWritable ndb = new NullDBWritable();
		 AreaDBWritable dbWritable = new AreaDBWritable();

		 values.forEach(text -> {
			 String[] split = text.toString().split(",");
			 String cityCode = split[0];
			 String cityName = split[1];
			 dbWritable.setUpdateBy("admin");
			 dbWritable.setCreateBy("admin");
			 dbWritable.setUpdateTime(new Timestamp(System.currentTimeMillis()));
			 dbWritable.setAreaRelNo(String.valueOf(UUID.randomUUID()));
			 dbWritable.setCityCode(cityCode);
			 dbWritable.setCityName(cityName);
			 dbWritable.setRegion2code(translatecode2RegionCode(key.toString()));
			 // dbWritable.setRegion2name(translateCode2RegionName(key.toString()));
			 dbWritable.setRegion2name("testName");
			 try {
				 context.write(dbWritable, ndb);
			 } catch (IOException | InterruptedException e) {
				 e.printStackTrace();
				 System.exit(-1);
			 }
		 });

	 }

	 private String translatecode2RegionCode(String toString) {

		 return oldNewRegionCodeMap.get(toString);
	 }

	 private  static Map<String, String> oldNewRegionCodeMap = Maps.newHashMap();
	 static {
		 oldNewRegionCodeMap.put("140000","33" );
		 oldNewRegionCodeMap.put("110000","1" );
		 oldNewRegionCodeMap.put("120000","3"  );
		 oldNewRegionCodeMap.put("130000","24" );
		 oldNewRegionCodeMap.put( "150000","31");
		 oldNewRegionCodeMap.put("210000","6"  );
		 oldNewRegionCodeMap.put("220000","8"  );
		 oldNewRegionCodeMap.put( "230000","19");
		 oldNewRegionCodeMap.put("310000","2"  );
		 oldNewRegionCodeMap.put( "320000","10");
		 oldNewRegionCodeMap.put("330000","12" );
		 oldNewRegionCodeMap.put("340000","25" );
		 oldNewRegionCodeMap.put("350000","13" );
		 oldNewRegionCodeMap.put("360000","22" );
		 /**/
		 oldNewRegionCodeMap.put( "370000","777");
		 /**/
		 oldNewRegionCodeMap.put( "410000","888");
		 oldNewRegionCodeMap.put( "420000","9");
		 oldNewRegionCodeMap.put( "430000","20");
		 oldNewRegionCodeMap.put( "440000","4");
		 oldNewRegionCodeMap.put( "450000","14");
		 oldNewRegionCodeMap.put( "460000","15");
		 oldNewRegionCodeMap.put("500000","18");
		 oldNewRegionCodeMap.put( "510000","26");
		 oldNewRegionCodeMap.put( "520000","35");
		 oldNewRegionCodeMap.put( "530000","16");
		 /**/
		 oldNewRegionCodeMap.put(  "540000","999");
		 oldNewRegionCodeMap.put( "610000","17");
		 oldNewRegionCodeMap.put( "620000","34");
		 oldNewRegionCodeMap.put( "630000","27");
		 oldNewRegionCodeMap.put( "640000","28");
		 oldNewRegionCodeMap.put( "650000","23");
/*		 oldNewRegionCodeMap.put("33", "140000");
		 oldNewRegionCodeMap.put("1",  "110000");
		 oldNewRegionCodeMap.put("3",  "120000");
		 oldNewRegionCodeMap.put("24", "130000");
		 oldNewRegionCodeMap.put("31",  "150000");
		 oldNewRegionCodeMap.put("6",  "210000");
		 oldNewRegionCodeMap.put("8",  "220000");
		 oldNewRegionCodeMap.put("19",  "230000");
		 oldNewRegionCodeMap.put("2",  "310000");
		 oldNewRegionCodeMap.put("10",  "320000");
		 oldNewRegionCodeMap.put("12", "330000");
		 oldNewRegionCodeMap.put("25", "340000");
		 oldNewRegionCodeMap.put("13", "350000");
		 oldNewRegionCodeMap.put("22", "360000");
		 *//**//*
		 oldNewRegionCodeMap.put("777", "370000");
		 *//**//*
		 oldNewRegionCodeMap.put("888", "410000");
		 oldNewRegionCodeMap.put("9",   "420000");
		 oldNewRegionCodeMap.put("20",  "430000");
		 oldNewRegionCodeMap.put("4",   "440000");
		 oldNewRegionCodeMap.put("14",  "450000");
		 oldNewRegionCodeMap.put("15",  "460000");
		 oldNewRegionCodeMap.put("18", "500000");
		 oldNewRegionCodeMap.put("26",  "510000");
		 oldNewRegionCodeMap.put("35",  "520000");
		 oldNewRegionCodeMap.put("16",  "530000");
		 *//**//*
		 oldNewRegionCodeMap.put("999",  "540000");
		 oldNewRegionCodeMap.put("17",  "610000");
		 oldNewRegionCodeMap.put("34",  "620000");
		 oldNewRegionCodeMap.put("27",  "630000");
		 oldNewRegionCodeMap.put("28",  "640000");
		 oldNewRegionCodeMap.put("23",  "650000");*/



	 }






 }
