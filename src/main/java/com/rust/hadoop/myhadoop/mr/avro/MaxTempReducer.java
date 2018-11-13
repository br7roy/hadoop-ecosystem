 /*
  * Package com.rust.hadoop.myhadoop.mr.avro
  * FileName: MaxTempReducer
  * Author:   Takho
  * Date:     2018/11/13 21:53
  */
 package com.rust.hadoop.myhadoop.mr.avro;

 import org.apache.avro.generic.GenericData;
 import org.apache.avro.generic.GenericRecord;
 import org.apache.avro.mapred.AvroKey;
 import org.apache.avro.mapred.AvroValue;
 import org.apache.hadoop.io.NullWritable;
 import org.apache.hadoop.mapreduce.Reducer;

 import java.io.IOException;
 import java.util.Comparator;
 import java.util.Iterator;
 import java.util.stream.Stream;

 /**
  * @author Takho
  */
 public class MaxTempReducer extends Reducer<AvroKey<Integer>, AvroValue<GenericRecord>, AvroValue<GenericRecord>,
		 NullWritable> {

	 @Override
	 protected void reduce(AvroKey<Integer> key, Iterable<AvroValue<GenericRecord>> values, Context context) throws IOException, InterruptedException {

/*		 Optional<AvroValue<GenericRecord>> temperature1 =
				 StreamSupport.stream(values.spliterator(), true).max(new Comparator<AvroValue<GenericRecord>>() {
					 @Override
					 public int compare(AvroValue<GenericRecord> o1, AvroValue<GenericRecord> o2) {
						 return Integer.compareUnsigned((int) o1.datum().get("temperature"), (int) o2.datum().get(
						 		"temperature"));
					 }
				 });
		 int maxtemp = (int) temperature1.get().datum().get("temperature");*/


		 Iterator<AvroValue<GenericRecord>> iterator = values.iterator();
		 int max = 0;
		 while (iterator.hasNext()) {
			 AvroValue<GenericRecord> next = iterator.next();
			 int temperature = (int) next.datum().get("temperature");
			 max = temperature > max ? temperature : max;
		 }
		 GenericRecord genericRecord = new GenericData.Record(MaxTempMapper.SCHEMA);
		 genericRecord.put("temperature", max);
		 genericRecord.put("year", key.datum());
		 genericRecord.put("stationId", "");
		 context.write(new AvroValue<>(genericRecord), NullWritable.get());

	 }

	 public static void main(String[] args) {
		 Integer[] ints = new Integer[]{1, 2, 3, 4, 5};
		 System.out.println(Stream.of(ints).max(Comparator.comparingInt(o -> o)).get());
	 }
 }
