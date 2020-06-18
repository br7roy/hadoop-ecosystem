 /*
  * Package tk.tak.hadoop.myhadoop.wordcount
  * FileName: WordCountMapper
  * Author:   Tak
  * Date:     2018/9/22 16:36
  */
 package tk.tak.hadoop.myhadoop.outputformat.seq;


 import org.apache.hadoop.io.IntWritable;
 import org.apache.hadoop.io.LongWritable;
 import org.apache.hadoop.io.Text;
 import org.apache.hadoop.mapreduce.Mapper;

 import java.io.IOException;

 /**
  * FileName:    WordCountMapper
  * Author:      Tak
  * Date:        2018/9/22
  * Description:
  */
 public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	 @Override
	 protected void map(LongWritable longWritable, Text value, Context context) throws IOException,
			 InterruptedException {
		 String line = value.toString();
		 String[] ss = line.split(" ");
		 for (String s : ss) {
			 context.write(new Text(s), new IntWritable(1));
		 }
	 }

 }
