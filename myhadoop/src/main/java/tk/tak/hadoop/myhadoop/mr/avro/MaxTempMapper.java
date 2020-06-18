 /*
  * Package tk.tak.hadoop.myhadoop.mr.avro
  * FileName: MaxTempMapper
  * Author:   Tak
  * Date:     2018/11/13 13:08
  * Description:
  * History:
  *===============================================================================================
  *   author：          time：                             version：           desc：
  *   Tak                 2018/11/13  13:08             1.0
  *===============================================================================================
  */
 package tk.tak.hadoop.myhadoop.mr.avro;

 import org.apache.avro.Schema;
 import org.apache.avro.generic.GenericData.Record;
 import org.apache.avro.generic.GenericRecord;
 import org.apache.avro.mapred.AvroKey;
 import org.apache.avro.mapred.AvroValue;
 import org.apache.hadoop.io.LongWritable;
 import org.apache.hadoop.io.Text;
 import org.apache.hadoop.mapreduce.Mapper;

 import java.io.IOException;

 /**
  * 使用AVRO串行化格式进行MR作业
  *
  * @author Tak
  */
 public class MaxTempMapper extends Mapper<LongWritable, Text, AvroKey<Integer>, AvroValue<GenericRecord>> {
	 private static final int MISSING = 9999;
	 public static final Schema SCHEMA = new Schema.Parser().parse(
			 "{" +
					 "\"type\":\"record\"," +
					 "\"name\":\"WeatherRecord\"," +
					 "\"doc\":\"A weather reading.\"," +
					 "\"fields\":[" +
					 "{\"name\":\"year\",\"type\":\"int\"}," +
					 "{\"name\":\"temperature\",\"type\":\"int\"}," +
					 "{\"name\":\"stationId\",\"type\":\"string\"}" +
					 "]" +
					 "}"
	 );
	 private NcdcRecordParser parser = new NcdcRecordParser();


	 @Override
	 protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		 //   value String
		 String line = value.toString();
		 //   提取年份
		 String year = line.substring(15, 19);
		 //   气温
		 int airTemperature;
		 //   提取气温值 + / -
		 if (line.charAt(87) == '+') {
			 airTemperature = Integer.parseInt(line.substring(88, 92));
		 } else {
			 airTemperature = Integer.parseInt(line.substring(87, 92));
		 }
		 //   质量
		 String quality = line.substring(92, 93);
		 //   判断气温的有效性
		 if (airTemperature != MISSING && quality.matches("[01459]")) {
			 // 构造avroKey
			 AvroKey<Integer> key0 = new AvroKey<Integer>(new Integer(year));
			 // 构造AvroValue<GenericRecord>
			 GenericRecord record = new Record(SCHEMA);
			 record.put("year", new Integer(year));
			 record.put("temperature", airTemperature);
			 record.put("stationId", "");
			 AvroValue<GenericRecord> value0 = new AvroValue<>(record);
			 context.write(key0, value0);
		 }

	 }
 }
