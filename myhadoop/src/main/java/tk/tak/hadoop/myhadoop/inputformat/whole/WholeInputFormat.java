 /*
  * Package tk.tak.hadoop.myhadoop.inputformat.whole
  * FileName: WholeInputFormat
  * Author:   Tak
  * Date:     2018/10/1 10:09
  */
 package tk.tak.hadoop.myhadoop.inputformat.whole;

 import org.apache.hadoop.fs.Path;
 import org.apache.hadoop.io.BytesWritable;
 import org.apache.hadoop.io.NullWritable;
 import org.apache.hadoop.mapreduce.InputSplit;
 import org.apache.hadoop.mapreduce.JobContext;
 import org.apache.hadoop.mapreduce.RecordReader;
 import org.apache.hadoop.mapreduce.TaskAttemptContext;
 import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

 import java.io.IOException;

 /**
  * FileName:    WholeInputFormat
  * Author:      Tak
  * Date:        2018/10/1
  * Description: 自定义输入格式，将整个文件作为一条记录
  */
 public class WholeInputFormat extends FileInputFormat<NullWritable, BytesWritable> {
	 @Override
	 public RecordReader<NullWritable, BytesWritable> createRecordReader(InputSplit split,
																		 TaskAttemptContext context) throws IOException, InterruptedException {
		 WholeRecordReader recordReader = new WholeRecordReader();
		 recordReader.initialize(split, context);
		 context.getCounter("in", "wholeinputformat.wholereader.new").increment(1);
		 return recordReader;
	 }

	 @Override
	 protected boolean isSplitable(JobContext context, Path filename) {
		 return false;
	 }
 }
