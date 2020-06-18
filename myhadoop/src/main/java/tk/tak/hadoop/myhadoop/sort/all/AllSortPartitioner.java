 /*
  * Package tk.tak.hadoop.myhadoop.sort.all
  * FileName: AllSortPartitioner
  * Author:   Tak
  * Date:     2018/10/18 7:26
  */
 package tk.tak.hadoop.myhadoop.sort.all;

 import org.apache.hadoop.io.IntWritable;
 import org.apache.hadoop.io.Text;
 import org.apache.hadoop.mapreduce.Partitioner;

 /**
  * 全排序分区函数
  *
  * @author Tak
  */
 public class AllSortPartitioner extends Partitioner<Text, IntWritable> {

	 /**
	  * 返回分区的索引
	  *
	  * @param key
	  * @param value
	  * @param num
	  * @return
	  */
	 @Override
	 public int getPartition(Text key, IntWritable value, int num) {

		 // 2002-2009

		 //		 2002-2007
		 //		 2008
		 //		 2009
		 int step = 10 / num;


		 int[][] array = new int[num][2];

		 for (int i = 0; i < num; i++) {
			 if (i == 0) {
				 array[i] = new int[]{Integer.MIN_VALUE, 2000 + step};
			 } else if (i == 1) {
				 array[i] = new int[]{2000 + i + step * i, 2000 + i + 2 * step};
			 } else {
				 array[i] = new int[]{2000 + 2 + step * i, Integer.MAX_VALUE};
			 }
		 }

		 int year = Integer.valueOf(key.toString());
		 for (int j = 0; j < array.length; j++) {

			 int[] row = array[j];
			 if (row[0] <= year && year <= row[1]) {
				 return j;
			 }
		 }
		 return 0;
	 }


	 public static void main(String[] args) {
		 //   2002-2009
		 //   MIN-2003
		 //   2004-2007
		 //   2008-MAX
		 int num = 3;
		 int step = 10 / num;
		 int[][] array = new int[num][2];
		 int cnt = step;
		 for (int i = 0; i < num; i++) {
			 if (i == 0) {
				 array[i] = new int[]{Integer.MIN_VALUE, 2000 + cnt};
			 } else if (i == 1) {
				 array[i] = new int[]{2000 + i + cnt * i, 2000 + i + 2 * cnt};
			 } else {
				 array[i] = new int[]{2000 + 2 + cnt * i, Integer.MAX_VALUE};
			 }
		 }
		 System.out.println();
		 int year = 2008;
		 for (int j = 0; j < array.length; j++) {

			 int[] row = array[j];

			 if (row[0] <= year && year <= row[1]) {
				 System.out.println(j);

			 }
		 }
		 System.out.println();
	 }


 }
