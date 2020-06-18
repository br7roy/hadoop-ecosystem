 /*
  * Package tk.tak.hadoop.myhadoop.join
  * FileName: MyReduce
  * Author:   Tak
  * Date:     2018/10/21 14:06
  */
 package tk.tak.hadoop.myhadoop.join.distributedcache;

 import com.google.common.collect.Maps;
 import org.apache.hadoop.fs.FSDataInputStream;
 import org.apache.hadoop.fs.FileSystem;
 import org.apache.hadoop.fs.Path;
 import org.apache.hadoop.io.IntWritable;
 import org.apache.hadoop.io.NullWritable;
 import org.apache.hadoop.io.Text;
 import org.apache.hadoop.mapreduce.Reducer;

 import java.io.BufferedReader;
 import java.io.IOException;
 import java.io.InputStreamReader;
 import java.net.URI;
 import java.util.Map;

 /**
  * @author Tak
  */
 public class MyReduce extends Reducer<IntWritable, Text, Text, NullWritable> {
	 private Map<Integer, String> customerMap = null;

	 @Override
	 protected void setup(Context context) throws IOException {

		 // 得到缓存uri
		 URI[] cacheFiles = context.getCacheFiles();

		 URI cacheFile = cacheFiles[0];

		 Path path = new Path(cacheFile);

		 try (FSDataInputStream inputStream = FileSystem.get(context.getConfiguration()).open(path);
			  BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream))) {
			 String line = null;
			 customerMap = Maps.newHashMap();
			 while ((line = bufferedReader.readLine()) != null) {
				 String[] split = line.split("\t");
				 customerMap.put(Integer.parseInt(split[0]), line);
			 }
		 }
	 }


	 @Override
	 protected void reduce(IntWritable key, Iterable<Text> values, Context context) {
		 // 取得customerid
		 int cid = key.get();
		 values.iterator().forEachRemaining(text -> {
			 String custInfo = customerMap.get(cid);
			 try {
				 context.write(new Text(custInfo + "\t" + text), NullWritable.get());
			 } catch (IOException | InterruptedException e) {
				 e.printStackTrace();
			 }
		 });

	 }
 }
