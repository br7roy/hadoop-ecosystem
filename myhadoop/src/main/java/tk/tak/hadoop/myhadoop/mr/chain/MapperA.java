 /*
  * Package tk.tak.hadoop.myhadoop.mr.chain
  * FileName: MapperA
  * Author:   Tak
  * Date:     2018/10/27 14:59
  */
 package tk.tak.hadoop.myhadoop.mr.chain;

 import org.apache.hadoop.io.IntWritable;
 import org.apache.hadoop.io.LongWritable;
 import org.apache.hadoop.io.Text;
 import org.apache.hadoop.mapreduce.Mapper;

 import java.io.IOException;
 import java.net.InetAddress;

 /**
  * 第一个Mapper
  *
  * @author Tak
  */
 public class MapperA extends Mapper<LongWritable, Text, Text, IntWritable> {
	 @Override
	 protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		 String line = value.toString();
		 String[] split = line.split(" ");
		 for (String s : split) {
			 context.write(new Text(s), new IntWritable(1));
		 }
		 context.getCounter("m", "MA." + InetAddress.getLocalHost().getHostName()).increment(1L);
		 System.out.println("MA." + this.hashCode() + ":" + value.toString());
	 }
 }
