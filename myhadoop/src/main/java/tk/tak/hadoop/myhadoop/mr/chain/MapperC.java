 /*
  * Package tk.tak.hadoop.myhadoop.mr.chain
  * FileName: MapperC
  * Author:   Tak
  * Date:     2018/10/27 15:18
  */
 package tk.tak.hadoop.myhadoop.mr.chain;

 import org.apache.hadoop.io.IntWritable;
 import org.apache.hadoop.io.Text;
 import org.apache.hadoop.mapreduce.Mapper;

 import java.io.IOException;
 import java.net.InetAddress;

 /**
  * @author Tak
  */
 public class MapperC extends Mapper<Text, IntWritable, Text, IntWritable> {
	 @Override
	 protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
		 context.write(key, value);
		 context.getCounter("r", "MC." + InetAddress.getLocalHost().getHostName()).increment(1L);
	 }
 }