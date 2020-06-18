 /*
  * Package tk.tak.hadoop.myhadoop.sort.secondary.mr
  * FileName: YearPartitioner
  * Author:   Tak
  * Date:     2018/10/20 22:09
  */
 package tk.tak.hadoop.myhadoop.sort.secondary;

 import org.apache.hadoop.io.NullWritable;
 import org.apache.hadoop.mapreduce.Partitioner;

 /**
  * 按照组合key中的year进行分区
  *
  * @author Tak
  */
 public class YearPartitioner extends Partitioner<CombineKey, NullWritable> {
	 /**
	  * 按照year分区
	  *
	  * @param combineKey
	  * @param nullWritable
	  * @param numPartitions
	  * @return
	  */
	 @Override
	 public int getPartition(CombineKey combineKey, NullWritable nullWritable, int numPartitions) {
		 return combineKey.getYear() % numPartitions;
	 }
 }
