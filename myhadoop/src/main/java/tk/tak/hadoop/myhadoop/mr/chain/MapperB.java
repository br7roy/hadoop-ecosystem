 /*
  * Package tk.tak.hadoop.myhadoop.mr.chain
  * FileName: MapperB
  * Author:   Tak
  * Date:     2018/10/27 15:04
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
 public class MapperB extends Mapper<Text, IntWritable, Text, IntWritable> {
	 @Override
	 protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
		 context.write(key, value);
		 context.getCounter("m", "MB." + InetAddress.getLocalHost().getHostName()).increment(1L);		 System.out.println("MA." + this.hashCode() + ":" + value.toString());
		 System.out.println("MC." + this.hashCode() + ":" + value.toString());
	 }
 }
