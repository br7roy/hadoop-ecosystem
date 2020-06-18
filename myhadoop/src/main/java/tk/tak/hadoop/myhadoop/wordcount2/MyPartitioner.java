 /*
  * Package tk.tak.hadoop.myhadoop.mr
  * FileName: MyPartitioner
  * Author:   Tak
  * Date:     2018/9/25 22:29
  */
 package tk.tak.hadoop.myhadoop.wordcount2;

 import org.apache.hadoop.io.IntWritable;
 import org.apache.hadoop.io.Text;
 import org.apache.hadoop.mapreduce.Partitioner;

 /**
  * FileName:    MyPartitioner
  * Author:      Tak
  * Date:        2018/9/25
  * Description: 自定义分区函数
  */
 public class MyPartitioner extends Partitioner<Text, IntWritable> {
	 @Override
	 public int getPartition(Text text, IntWritable intWritable, int numPartitions) {
		 return 0;

	 }
 }
