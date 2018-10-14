package com.rust.hadoop.myhadoop.work;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat.NullDBWritable;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.UUID;

/**
 * @author Rust
 * Date:    2018/10/10 1:42
 */
public class WordCountReducer extends Reducer<Text, Text, AreaDBWritable, NullDBWritable> {

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) {
		NullDBWritable ndb = new NullDBWritable();
		AreaDBWritable dbWritable = new AreaDBWritable();

		values.forEach(text -> {
			String[] split = text.toString().split(",");
			// value: 2级名称，城市码，城市名称
			String erjiName = split[0];
			String cityCode = split[1];
			String cityName = split[2];
			dbWritable.setUpdateBy("admin");
			dbWritable.setCreateBy("admin");
			dbWritable.setUpdateTime(new Timestamp(System.currentTimeMillis()));
			dbWritable.setAreaRelNo(String.valueOf(UUID.randomUUID()));
			dbWritable.setCityCode(cityCode);
			dbWritable.setCityName(cityName);
			dbWritable.setRegion2code(key.toString());
			dbWritable.setRegion2name(erjiName);
			try {
				context.write(dbWritable, ndb);
			} catch (IOException | InterruptedException e) {
				e.printStackTrace();
				System.exit(-1);
			}
		});
	}
}
