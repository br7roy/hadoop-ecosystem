 /*
  * Package com.rust.hadoop.myhadoop.sort.all
  * FileName: AllSortPartitioner
  * Author:   Rust
  * Date:     2018/10/18 7:26
  */
 package com.rust.hadoop.myhadoop.sort.all;

 import org.apache.hadoop.io.IntWritable;
 import org.apache.hadoop.mapreduce.Partitioner;

 /**
  * 全排序分区函数
  *
  * @author Rust
  */
 public class AllSortPartitioner extends Partitioner<IntWritable, IntWritable> {

	 /**
	  * 返回分区的索引
	  *
	  * @param key
	  * @param value
	  * @param num
	  * @return
	  */
	 @Override
	 public int getPartition(IntWritable key, IntWritable value, int num) {

		 // 2002-2009

		 // 2008 2009

		 int phase = 10 / num;

//		 2002-2007
//		 2008
//		 2009











/*		 int phase = 8 / numPartitions;
		 int year = intWritable.get() - 2001;


		 if (numPartitions > 1) {

		 }*/

		 return 0;
	 }
 }
