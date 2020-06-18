 /*
  * Package tk.tak.hadoop.myhadoop.seqfile
  * FileName: SeqFile
  * Author:   Tak
  * Date:     2018/10/2 15:50
  */
 package tk.tak.hadoop.myhadoop.seqfile;

 import org.apache.hadoop.conf.Configuration;
 import org.apache.hadoop.fs.FileSystem;
 import org.apache.hadoop.fs.Path;
 import org.apache.hadoop.io.IntWritable;
 import org.apache.hadoop.io.SequenceFile;
 import org.apache.hadoop.io.Text;

 import java.io.IOException;

 /**
  * FileName:    SeqFile
  * Author:      Tak
  * Date:        2018/10/2
  * Description:
  */
 public class SeqFile {
	 public static void main(String[] args) throws IOException {
		 Configuration conf = new Configuration();
		 conf.set("fs.defaultFS", "hdfs://s100:8020");
		 FileSystem fs = FileSystem.get(conf);
		 Path path = new Path("hdfs://s100:8020/user/ubuntu/data/seq/seq.data");
		 SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, path,
				 IntWritable.class, Text.class);
		 IntWritable key = new IntWritable();
		 Text value = new Text();
		 for (int i = 0; i < 100; i++) {
			 key.set(i);
			 value.set("tom" + i);
			 writer.append(key, value);
		 }
		 writer.close();
		 System.out.println("done");
	 }
 }
